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

import javax.annotation.Nullable;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Static side channel used to carry DDL {@code WITH (...)} connector options from the planner
 * (where they live inside {@code ContextResolvedTable}) to the runtime coordinators on the
 * JobManager. Lives in {@code flink-runtime}, in the parent classloader of both the planner
 * loader and the streaming/runtime coordinator classes, so all sides see the same class.
 *
 * <p>Storage is partitioned by {@link JobID} so that, in session mode, concurrent jobs do not
 * see each other's entries and a finished job can be reclaimed with {@link #clearJob(JobID)}.
 *
 * <p>The planner thread sets its target JobID via {@link #setCurrentJob(JobID)} around a
 * {@code translate()} invocation, and {@link #put(String, Entry)} writes into that scope.
 * Coordinators run on the JM and look up via {@link #findForOperator(JobID, String)}; the
 * legacy single-argument {@link #findForOperator(String)} falls back to scanning every scope
 * for operators whose context does not yet expose a JobID.
 */
@Internal
public final class ConnectorOptionsRegistry {

    /** All registrations keyed first by JobID, then by table identifier (or short alias). */
    private static final ConcurrentHashMap<JobID, ConcurrentHashMap<String, Entry>> BY_JOB =
            new ConcurrentHashMap<>();

    /** JobID for the in-progress planner translation. Threads call {@link #setCurrentJob} on entry
     * and {@link #clearCurrentJob} on exit. */
    private static final ThreadLocal<JobID> CURRENT_JOB_ID = new ThreadLocal<>();

    private ConnectorOptionsRegistry() {}

    public static void setCurrentJob(JobID jobId) {
        if (jobId != null) {
            CURRENT_JOB_ID.set(jobId);
        }
    }

    public static void clearCurrentJob() {
        CURRENT_JOB_ID.remove();
    }

    @Nullable
    public static JobID getCurrentJob() {
        return CURRENT_JOB_ID.get();
    }

    /**
     * Registers an entry under the {@link #setCurrentJob(JobID) current thread's JobID}. Writes
     * outside an active scope are ignored — callers that don't go through the TableEnvironment
     * hook simply produce empty lookups, which the coordinators format as {@code connector=null,
     * options={}} instead of NPE.
     */
    public static void put(String tableIdentifier, Entry entry) {
        JobID jobId = CURRENT_JOB_ID.get();
        if (jobId == null || tableIdentifier == null || entry == null) {
            return;
        }
        ConcurrentHashMap<String, Entry> jobMap =
                BY_JOB.computeIfAbsent(jobId, k -> new ConcurrentHashMap<>());
        jobMap.put(tableIdentifier, entry);
        // Alias by the bare object name so short operator labels like "Source: kafka_src[1]" still
        // match a fully-qualified identifier such as "default_catalog.default_database.kafka_src".
        int lastDot = tableIdentifier.lastIndexOf('.');
        if (lastDot > 0 && lastDot < tableIdentifier.length() - 1) {
            jobMap.put(tableIdentifier.substring(lastDot + 1), entry);
        }
    }

    /**
     * Coordinator-side lookup scoped to a known JobID — returns the entry whose registered key
     * is the longest substring of {@code operatorName}.
     */
    public static Entry findForOperator(@Nullable JobID jobId, @Nullable String operatorName) {
        if (operatorName == null) {
            return Entry.EMPTY;
        }
        if (jobId != null) {
            ConcurrentHashMap<String, Entry> jobMap = BY_JOB.get(jobId);
            return jobMap == null ? Entry.EMPTY : longestMatch(jobMap, operatorName);
        }
        return findForOperator(operatorName);
    }

    /**
     * Coordinator-side lookup for callers that cannot obtain a JobID from their context. Scans
     * every job's submap; longest-match-wins. In application mode there is only one JobID, so
     * this is equivalent to the scoped variant. In session mode with concurrent jobs sharing
     * table identifiers, the result is only as accurate as operator-name uniqueness allows —
     * acceptable for the lifecycle log line, not for routing decisions.
     */
    public static Entry findForOperator(@Nullable String operatorName) {
        if (operatorName == null || BY_JOB.isEmpty()) {
            return Entry.EMPTY;
        }
        Entry best = Entry.EMPTY;
        int bestLen = -1;
        for (ConcurrentHashMap<String, Entry> jobMap : BY_JOB.values()) {
            for (Map.Entry<String, Entry> e : jobMap.entrySet()) {
                if (operatorName.contains(e.getKey()) && e.getKey().length() > bestLen) {
                    bestLen = e.getKey().length();
                    best = e.getValue();
                }
            }
        }
        return best;
    }

    private static Entry longestMatch(Map<String, Entry> jobMap, String operatorName) {
        return jobMap.entrySet().stream()
                .filter(e -> operatorName.contains(e.getKey()))
                .max(Comparator.comparingInt(e -> e.getKey().length()))
                .map(Map.Entry::getValue)
                .orElse(Entry.EMPTY);
    }

    /**
     * Deduplicated snapshot of the current thread's job — used by the plan-translation error
     * logger to list every table registered for the failing job (e.g. so a sink-side failure log
     * can also display the upstream Kafka topic). Returns empty when called outside a scope.
     */
    public static List<Map.Entry<String, Entry>> snapshot() {
        JobID jobId = CURRENT_JOB_ID.get();
        if (jobId == null) {
            return Collections.emptyList();
        }
        ConcurrentHashMap<String, Entry> jobMap = BY_JOB.get(jobId);
        if (jobMap == null || jobMap.isEmpty()) {
            return Collections.emptyList();
        }
        IdentityHashMap<Entry, String> bestKey = new IdentityHashMap<>();
        for (Map.Entry<String, Entry> e : jobMap.entrySet()) {
            String prev = bestKey.get(e.getValue());
            if (prev == null || e.getKey().length() > prev.length()) {
                bestKey.put(e.getValue(), e.getKey());
            }
        }
        List<Map.Entry<String, Entry>> out = new ArrayList<>(bestKey.size());
        for (Map.Entry<Entry, String> e : bestKey.entrySet()) {
            out.add(new AbstractMap.SimpleImmutableEntry<>(e.getValue(), e.getKey()));
        }
        return out;
    }

    /** Removes all entries registered for a finished job. */
    public static void clearJob(JobID jobId) {
        if (jobId != null) {
            BY_JOB.remove(jobId);
        }
    }

    /** Test-only helper. */
    public static void clear() {
        BY_JOB.clear();
        CURRENT_JOB_ID.remove();
    }

    /** Connector metadata for a single table. */
    public static final class Entry {
        public static final Entry EMPTY = new Entry(null, Collections.emptyMap());

        @Nullable private final String connectorIdentifier;
        private final Map<String, String> options;

        public Entry(@Nullable String connectorIdentifier, Map<String, String> options) {
            this.connectorIdentifier = connectorIdentifier;
            this.options =
                    options == null
                            ? Collections.emptyMap()
                            : Collections.unmodifiableMap(options);
        }

        @Nullable
        public String getConnectorIdentifier() {
            return connectorIdentifier;
        }

        public Map<String, String> getOptions() {
            return options;
        }
    }
}
