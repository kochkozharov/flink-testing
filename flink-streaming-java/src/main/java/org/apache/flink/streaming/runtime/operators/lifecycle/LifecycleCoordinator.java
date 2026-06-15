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

package org.apache.flink.streaming.runtime.operators.lifecycle;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.audit.LifecycleAudit;
import org.apache.flink.runtime.audit.LifecycleAuditRegistry;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * JM-side coordinator for one {@link LifecycleProbeOperator}. Role-parameterized: for a source it
 * emits C1/C2, for a sink C3/C4 — same class for V1 and V2. It carries the connector's DDL {@code
 * WITH (...)} options (passed in at plan time, serialized into the JobGraph via the {@link
 * Provider}) so every audit event includes them, without any registry or JobID keying.
 *
 * <p>Dedup: one started (C1/C3) per attempt; one failure (C2/C4) per failure episode. Both re-armed
 * on {@link #resetToCheckpoint}. All emission goes through {@link LifecycleAudit}, which is
 * session-safe (never throws) — so this stays inert in session mode.
 */
@Internal
final class LifecycleCoordinator
        implements OperatorCoordinator, LifecycleAuditRegistry.JobFailureHandler {

    private final boolean sink;
    private final Map<String, String> options;
    @Nullable private final JobID jobId;
    /** SQL-intended modified columns of the sink; null/empty for source and when unknown. */
    @Nullable private final List<String> modifiedColumns;

    private boolean connectedLogged;
    private boolean startedLogged;
    private boolean failedLogged;

    LifecycleCoordinator(
            boolean sink,
            Map<String, String> options,
            @Nullable JobID jobId,
            @Nullable List<String> modifiedColumns) {
        this.sink = sink;
        this.options = options;
        this.jobId = jobId;
        this.modifiedColumns = modifiedColumns;
    }

    @Override
    public void start() {
        // Register so DefaultExecutionGraph job-level FAILING/FAILED transitions reach us — needed
        // to emit C2/C4 when the failure lands in a sink-chain task we don't probe directly
        // (eg Iceberg's pre-commit aggregator: our probe-task gets CANCELLED, not FAILED, so
        // executionAttemptFailed never fires here).
        LifecycleAuditRegistry.register(jobId, this);
    }

    @Override
    public void close() {
        LifecycleAuditRegistry.unregister(jobId, this);
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        if (event instanceof LifecycleConnectedEvent) {
            if (connectedLogged) {
                return;
            }
            connectedLogged = true;
            // A2 — connector initialised, ready to read/write (even if no records yet).
            LifecycleAudit.authSucceeded(jobId, options);
            return;
        }
        if (event instanceof LifecycleStartedEvent) {
            if (startedLogged) {
                return;
            }
            startedLogged = true;
            failedLogged = false; // a fresh successful start re-arms failure logging
            if (sink) {
                LifecycleAudit.writeStarted(jobId, options, modifiedColumns);
            } else {
                LifecycleAudit.readStarted(jobId, options);
            }
        }
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        // Skip "empty" failures — cascade-cancel often surfaces here as null or
        // CancellationException without the real root cause (the actual failure happened in a
        // different task). Defer to LifecycleAuditRegistry.notifyJobFailed from the job-level
        // FAILING transition, which carries the proper cause.
        if (reason == null
                || reason instanceof java.util.concurrent.CancellationException
                || LifecycleAudit.rootCauseMessage(reason) == null) {
            return;
        }
        emitFailed(reason);
    }

    /**
     * Called by {@link LifecycleAuditRegistry} on job-wide FAILING/FAILED transitions. Catches the
     * failures whose owning task isn't our probe-task (eg Iceberg V2 pre-commit aggregator failure
     * → our probe-task gets CANCELLED, executionAttemptFailed never fires). Idempotent against the
     * per-attempt path via {@link #failedLogged}.
     */
    @Override
    public void onJobFailed(@Nullable Throwable cause) {
        emitFailed(cause);
    }

    private void emitFailed(@Nullable Throwable cause) {
        if (failedLogged) {
            return;
        }
        failedLogged = true;
        final String msg = LifecycleAudit.rootCauseMessage(cause);
        if (!connectedLogged) {
            // open()/init never reported success — couldn't establish connection → A3.
            LifecycleAudit.authFailed(jobId, options, msg);
        } else if (sink) {
            LifecycleAudit.writeFailed(jobId, options, msg, modifiedColumns);
        } else {
            LifecycleAudit.readFailed(jobId, options, msg);
        }
    }


    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {}

    @Override
    public void subtaskReset(int subtask, long checkpointId) {}

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
        resultFuture.complete(new byte[0]);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) {
        connectedLogged = false;
        startedLogged = false;
        failedLogged = false;
    }

    /** Serializable factory carried inside the JobGraph; recreated on the JM. */
    static final class Provider implements OperatorCoordinator.Provider {

        private static final long serialVersionUID = 1L;

        private final OperatorID operatorID;
        private final boolean sink;
        private final Map<String, String> options;
        @Nullable private final List<String> modifiedColumns;

        Provider(
                OperatorID operatorID,
                boolean sink,
                Map<String, String> options,
                @Nullable List<String> modifiedColumns) {
            this.operatorID = operatorID;
            this.sink = sink;
            this.options = options;
            this.modifiedColumns = modifiedColumns;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new LifecycleCoordinator(sink, options, jobIdFrom(context), modifiedColumns);
        }

        /**
         * The runtime {@link JobID} is not exposed on {@link Context} directly, but the coordinator's
         * metric group carries it as the {@code <job_id>} scope variable. Best-effort: returns null
         * if unavailable (e.g. session mode), so audit just falls back to UNDEFINED.
         */
        @Nullable
        private static JobID jobIdFrom(Context context) {
            try {
                final String hex =
                        context.metricGroup().getAllVariables().get(ScopeFormat.SCOPE_JOB_ID);
                return hex == null ? null : JobID.fromHexString(hex);
            } catch (Throwable ignored) {
                return null;
            }
        }
    }
}
