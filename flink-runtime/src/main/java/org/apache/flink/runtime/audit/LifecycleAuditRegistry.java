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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Process-wide registry of audit handlers per {@link JobID}. Bridges the JobMaster's
 * job-status transitions to all per-operator audit coordinators in that job.
 *
 * <p>Why this exists: {@code OperatorCoordinator.executionAttemptFailed} only fires when the
 * subtask containing the coordinator's own operator fails. With V2 sinks that split into
 * multiple Flink tasks (Iceberg writer + pre-commit aggregator + committer; Paimon; Hudi V2)
 * the failure often lands in a task that has no audit probe — the probe-operator's task is
 * just CANCELLED as a side-effect of the job-wide failure, the audit coordinator sees no
 * {@code executionAttemptFailed}, and C2/C4 is silently dropped.
 *
 * <p>Flow: {@code DefaultExecutionGraph.transitionState(...)} → {@link #notifyJobFailed} on
 * FAILING/FAILED → each registered handler decides whether to emit (no-op if it already did
 * via its own per-attempt path). Idempotency is per-handler.
 */
@Internal
public final class LifecycleAuditRegistry {

    /** Implemented by audit coordinators (eg LifecycleCoordinator). Best-effort; never throws. */
    public interface JobFailureHandler {
        void onJobFailed(Throwable cause);
    }

    private static final ConcurrentMap<JobID, Set<JobFailureHandler>> ACTIVE =
            new ConcurrentHashMap<>();

    private LifecycleAuditRegistry() {}

    /** Called from {@code LifecycleCoordinator.start()}. No-op for null jobId (session mode). */
    public static void register(JobID jobId, JobFailureHandler handler) {
        if (jobId == null || handler == null) {
            return;
        }
        ACTIVE.computeIfAbsent(jobId, k -> ConcurrentHashMap.newKeySet()).add(handler);
    }

    /** Called from {@code LifecycleCoordinator.close()}. Cleans up the per-job set when empty. */
    public static void unregister(JobID jobId, JobFailureHandler handler) {
        if (jobId == null || handler == null) {
            return;
        }
        final Set<JobFailureHandler> set = ACTIVE.get(jobId);
        if (set == null) {
            return;
        }
        set.remove(handler);
        if (set.isEmpty()) {
            ACTIVE.remove(jobId);
        }
    }

    /**
     * Called from {@code DefaultExecutionGraph.transitionState(...)} when the job enters
     * FAILING or FAILED. Fans the cause out to every registered handler; handlers swallow
     * any errors so audit never breaks the JobMaster.
     */
    public static void notifyJobFailed(JobID jobId, Throwable cause) {
        if (jobId == null) {
            return;
        }
        final Set<JobFailureHandler> set = ACTIVE.get(jobId);
        if (set == null || set.isEmpty()) {
            return;
        }
        for (JobFailureHandler h : set) {
            try {
                h.onJobFailed(cause);
            } catch (Throwable ignored) {
                // best-effort: audit must never break the job-status callback
            }
        }
    }
}
