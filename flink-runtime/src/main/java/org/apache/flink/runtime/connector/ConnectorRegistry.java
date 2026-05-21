/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.connector;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * JobManager-side audit registry. Stores per-job connector metadata, keyed by {@link JobID}, and
 * emits one log entry per connector lifecycle transition:
 *
 * <ul>
 *   <li>C1 — a source started reading
 *   <li>C2 — a source failed reading
 *   <li>C3 — a sink started writing
 *   <li>C4 — a sink failed writing
 * </ul>
 *
 * <p>Each connector is logged on its own line — never aggregated into a single message. The
 * registry does not dedup: the calling coordinator already gates the "started" events behind its
 * own {@code startedLogged} flag (one coordinator per connector), and re-arms it on restart
 * (recreation for sources, {@code resetToCheckpoint} for sinks). The registry only stores the
 * connector metadata and emits.
 *
 * <p>In a fork that ships a real audit pipeline, replace the body of {@link #emit} with the
 * equivalent {@code AuditLogger.log(AuditEvent...)} call — every call site stays the same.
 */
@Internal
public final class ConnectorRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorRegistry.class);

    private static final String UNDEFINED = "UNDEFINED";

    private final ConcurrentMap<JobID, JobInfo> jobInfoMap = new ConcurrentHashMap<>();

    private ConnectorRegistry() {}

    public static ConnectorRegistry getInstance() {
        return ConnectorRegistryHolder.INSTANCE;
    }

    // ------------------------------------------------------------------------
    //  Registration — each connector registered once under a stable key.
    // ------------------------------------------------------------------------

    public void registerSource(final JobID jobID, final ConnectorInfo source) {
        if (jobID != null && source != null) {
            putWithAlias(info(jobID).sources, source);
        }
    }

    public void registerSink(final JobID jobID, final ConnectorInfo sink) {
        if (jobID != null && sink != null) {
            putWithAlias(info(jobID).sinks, sink);
        }
    }

    /**
     * Stores the entry under its full identifier and under the bare object name (after the last
     * dot). The bare alias lets a coordinator find it by substring of an operator name like
     * {@code "Source: kafka_src[1]"}, while the full identifier still wins when present.
     */
    private static void putWithAlias(
            final Map<String, ConnectorInfo> map, final ConnectorInfo info) {
        final String name = info.objectName();
        map.put(name, info);
        final int dot = name.lastIndexOf('.');
        if (dot > 0 && dot < name.length() - 1) {
            map.put(name.substring(dot + 1), info);
        }
    }

    /** Drop all state for a finished job. Wire to the JM job-terminated hook to avoid leaks. */
    public void clearJobConnectors(final JobID jobID) {
        if (jobID != null) {
            jobInfoMap.remove(jobID);
        }
    }

    private JobInfo info(final JobID jobID) {
        return jobInfoMap.computeIfAbsent(jobID, k -> new JobInfo());
    }

    // ------------------------------------------------------------------------
    //  Emission — one entry per connector. The coordinator's own "startedLogged"
    //  flag is the single dedup gate, so the registry only stores + emits.
    // ------------------------------------------------------------------------

    /** C1 — the given source started reading. */
    public void writeC1Log(final JobID jobID, final String sourceKey) {
        final ConnectorInfo connector = lookup(jobID, sourceKey, false);
        if (connector != null) {
            emit("C1", jobID, connector, "SUCCESS", null);
        }
    }

    /** C2 — the given source failed reading. */
    public void writeC2Log(final JobID jobID, final String sourceKey, final String reason) {
        final ConnectorInfo connector = lookup(jobID, sourceKey, false);
        if (connector != null) {
            emit("C2", jobID, connector, "FAIL", reason);
        }
    }

    /** C3 — the given sink started writing. */
    public void writeC3Log(final JobID jobID, final String sinkKey) {
        final ConnectorInfo connector = lookup(jobID, sinkKey, true);
        if (connector != null) {
            emit("C3", jobID, connector, "SUCCESS", null);
        }
    }

    /** C4 — the given sink failed writing. */
    public void writeC4Log(final JobID jobID, final String sinkKey, final String reason) {
        final ConnectorInfo connector = lookup(jobID, sinkKey, true);
        if (connector != null) {
            emit("C4", jobID, connector, "FAIL", reason);
        }
    }

    // ------------------------------------------------------------------------
    //  Direct emit for plan-translation failures. Nothing is registered yet at
    //  planning time (registration happens in the coordinator's start() at
    //  runtime), and the job has no JobID until submission — so the connector
    //  metadata is passed inline and jobID is typically null.
    // ------------------------------------------------------------------------

    /** C2 — a source could not be created during plan translation. */
    public void writeC2Log(
            final JobID jobID,
            final String objectName,
            final String objectId,
            final List<String> properties,
            final String reason) {
        emit("C2", jobID, new ConnectorInfo(objectId, objectName, properties), "FAIL", reason);
    }

    /** C4 — a sink could not be created during plan translation. */
    public void writeC4Log(
            final JobID jobID,
            final String objectName,
            final String objectId,
            final List<String> properties,
            final String reason) {
        emit("C4", jobID, new ConnectorInfo(objectId, objectName, properties), "FAIL", reason);
    }

    /**
     * Finds the connector a coordinator is asking about. The planner registers by table identifier
     * (and bare alias), while the coordinator only knows its {@code operatorName} (e.g. {@code
     * "Source: kafka_src[1]"} / {@code "IcebergSink iceberg.db.events: Writer"}). Matches the
     * registered key that is a substring of {@code operatorName}, longest wins. Pure read — a
     * concurrent {@link #clearJobConnectors} costs at most one missed log for a terminating job.
     */
    private ConnectorInfo lookup(
            final JobID jobID, final String operatorName, final boolean isSink) {
        final JobInfo info = jobInfoMap.get(jobID);
        if (info == null || operatorName == null) {
            return null;
        }
        final Map<String, ConnectorInfo> map = isSink ? info.sinks : info.sources;
        ConnectorInfo best = null;
        int bestLen = -1;
        for (final Map.Entry<String, ConnectorInfo> e : map.entrySet()) {
            if (operatorName.contains(e.getKey()) && e.getKey().length() > bestLen) {
                bestLen = e.getKey().length();
                best = e.getValue();
            }
        }
        return best;
    }

    private void emit(
            final String subtype,
            final JobID jobID,
            final ConnectorInfo connector,
            final String status,
            final String reason) {
        // Fork hook: swap this for AuditLogger.log(AuditEvent.builder()...build()).
        if ("FAIL".equals(status)) {
            LOG.error(
                    "[{}] job={} status={} object={} id={} properties={} reason={}",
                    subtype,
                    jobID,
                    status,
                    connector.objectName(),
                    connector.objectId(),
                    connector.properties(),
                    reason != null ? reason : "unknown reason");
        } else {
            LOG.info(
                    "[{}] job={} status={} object={} id={} properties={}",
                    subtype,
                    jobID,
                    status,
                    connector.objectName(),
                    connector.objectId(),
                    connector.properties());
        }
    }

    // ------------------------------------------------------------------------
    //  Types
    // ------------------------------------------------------------------------

    /** Immutable per-connector descriptor. {@code properties} are the physical WITH-options. */
    public static final class ConnectorInfo {
        private final String objectId;
        private final String objectName;
        private final List<String> properties;

        public ConnectorInfo(String objectId, String objectName, List<String> properties) {
            this.objectId = objectId != null ? objectId : UNDEFINED;
            this.objectName = objectName != null ? objectName : UNDEFINED;
            this.properties = properties != null ? List.copyOf(properties) : List.of();
        }

        public String objectId() {
            return objectId;
        }

        public String objectName() {
            return objectName;
        }

        public List<String> properties() {
            return properties;
        }
    }

    private static final class JobInfo {
        private final Map<String, ConnectorInfo> sources = new ConcurrentHashMap<>();
        private final Map<String, ConnectorInfo> sinks = new ConcurrentHashMap<>();
    }

    private static final class ConnectorRegistryHolder {
        static final ConnectorRegistry INSTANCE = new ConnectorRegistry();
    }
}
