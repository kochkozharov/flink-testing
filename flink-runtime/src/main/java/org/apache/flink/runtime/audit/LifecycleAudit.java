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

package org.apache.flink.runtime.audit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Single entry point that turns a connector lifecycle transition into an {@link AuditEvent} and
 * ships it via {@link AuditLogger}. Used identically from every path — eager (plan translation) and
 * runtime, source and sink, V1 and V2 — so all four events (C1–C4) come out in one shape.
 *
 * <p>Inputs are just the JobID and the connector's DDL {@code WITH (...)} options: {@code
 * objectName} is built from {@code connector} + the real table/topic name, {@code objectProperties}
 * is the full option list. Identity comes from {@link Sessions} (constant in this branch).
 *
 * <p>Every method is fully guarded: any failure (no session, null inputs, ...) is swallowed, so
 * audit never breaks the job. In session mode where nothing is wired, this simply produces no log.
 */
@Internal
public final class LifecycleAudit {

    private static final String SUCCESS = "SUCCESS";
    private static final String FAIL = "FAIL";
    private static final String UNDEFINED = "UNDEFINED";
    private static final String NO_COLUMNS_SENTINEL = "NO_DATA";

    private static final DateTimeFormatter DATE =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final ZoneId TZ = ZoneId.of("Europe/Moscow");

    private LifecycleAudit() {}

    // ----- Table API translation flag --------------------------------------
    // EagerAudit.begin()/clear() set/clear this. The DataStream env.fromSource patch checks it
    // and skips its own wrap+probe injection when we're inside Table API plan translation:
    // the planner already injects its own probe via CommonExecTableSourceScan.probeSource for
    // every source it materialises, so without this guard we'd double-wrap and get duplicate
    // audit events for every SQL job.
    private static final ThreadLocal<Boolean> IN_TABLE_TRANSLATION = new ThreadLocal<>();

    public static void enterTableTranslation() {
        IN_TABLE_TRANSLATION.set(Boolean.TRUE);
    }

    public static void exitTableTranslation() {
        IN_TABLE_TRANSLATION.remove();
    }

    public static boolean inTableTranslation() {
        return Boolean.TRUE.equals(IN_TABLE_TRANSLATION.get());
    }

    /**
     * Walks the cause chain to its deepest root and returns its message. Flink and most connectors
     * wrap the actual error in layers of generic descriptions ({@code Task failed} → {@code Failed
     * to construct kafka producer} → ...); the deepest cause carries the specific reason like
     * {@code No resolvable bootstrap urls given in bootstrap.servers}. Returns {@code null} for
     * {@code null} input.
     */
    public static String rootCauseMessage(Throwable t) {
        if (t == null) {
            return null;
        }
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur.getMessage();
    }

    /**
     * A2 — connector authenticated and ready to read/write. Fired once per probe-operator open()
     * (i.e. after the connector successfully initialised, even if no records ever flow). Carries
     * the connector's WITH-options so the audit event can identify which system was reached.
     */
    public static void authSucceeded(JobID jobId, Map<String, String> withOptions) {
        emit(AuditSubtypeId.A2, SUCCESS, jobId, withOptions, null, null);
    }

    /** A3 — connector authentication failed (creation or first connector-method call). */
    public static void authFailed(JobID jobId, Map<String, String> withOptions, String reason) {
        emit(AuditSubtypeId.A3, FAIL, jobId, withOptions, reason, null);
    }

    /** C1 — source started reading. */
    public static void readStarted(JobID jobId, Map<String, String> withOptions) {
        emit(AuditSubtypeId.C1, SUCCESS, jobId, withOptions, null, null);
    }

    /** C2 — source failed reading. */
    public static void readFailed(JobID jobId, Map<String, String> withOptions, String reason) {
        emit(AuditSubtypeId.C2, FAIL, jobId, withOptions, reason, null);
    }

    /** C3 — sink started writing. */
    public static void writeStarted(JobID jobId, Map<String, String> withOptions) {
        writeStarted(jobId, withOptions, null);
    }

    /**
     * C3 with the SQL-intended modified columns of the sink (target columns of INSERT/UPDATE; all
     * columns for {@code INSERT INTO sink SELECT ...}). Pass {@code null}/empty if unknown.
     */
    public static void writeStarted(
            JobID jobId, Map<String, String> withOptions, List<String> modifiedColumns) {
        emit(AuditSubtypeId.C3, SUCCESS, jobId, withOptions, null, modifiedColumns);
    }

    /** C4 — sink failed writing. */
    public static void writeFailed(JobID jobId, Map<String, String> withOptions, String reason) {
        writeFailed(jobId, withOptions, reason, null);
    }

    /** C4 with the SQL-intended modified columns of the sink. */
    public static void writeFailed(
            JobID jobId,
            Map<String, String> withOptions,
            String reason,
            List<String> modifiedColumns) {
        emit(AuditSubtypeId.C4, FAIL, jobId, withOptions, reason, modifiedColumns);
    }

    private static void emit(
            AuditSubtypeId subtype,
            String status,
            JobID jobId,
            Map<String, String> opts,
            String reason,
            List<String> modifiedColumns) {
        try {
            final SessionInfo s = Sessions.forJob(jobId);
            final String objectName = ObjectNameBuilder.build(opts);

            final List<String> props = new ArrayList<>();
            if (opts != null) {
                for (Map.Entry<String, String> e : opts.entrySet()) {
                    props.add(e.getKey() + "=" + e.getValue());
                }
            }
            // SQL-intended modified columns of the sink (target columns of the INSERT/UPDATE).
            // For C3/C4 it carries what the sink WAS ASKED to write; not what the connector
            // actually persisted. Downstream-pipeline constraint: this field must ALWAYS be
            // present and non-empty in audit events, so source events (where the concept
            // doesn't apply) and sinks whose schema couldn't be extracted fall back to a
            // sentinel value. Enforced centrally here so no caller path can bypass it.
            final String columnsValue =
                    (modifiedColumns == null || modifiedColumns.isEmpty())
                            ? NO_COLUMNS_SENTINEL
                            : String.join(",", modifiedColumns);
            props.add("modified_columns=[" + columnsValue + "]");

            final AuditEvent event =
                    AuditEvent.builder()
                            .subtypeId(subtype.name())
                            .bEvent(subtype.getOpCode())
                            .status(status)
                            .processName(jobId == null ? UNDEFINED : jobId.toString())
                            .objectName(objectName)
                            .objectId(UNDEFINED)
                            .objectProperties(props)
                            .sessionId(s.getSessionId())
                            .userLogin(s.getUserLogin())
                            .ipAddress(s.getIpAddress())
                            .fqdnAddress(s.getFqdnAddress())
                            .reason(reason)
                            .operationDate(
                                    ZonedDateTime.ofInstant(Instant.now(), TZ).format(DATE))
                            .build();

            AuditLogger.log(event);
        } catch (Throwable ignored) {
            // Audit is best-effort: never let it break the job (and stay quiet in session mode).
        }
    }

}
