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
import org.apache.flink.runtime.audit.LifecycleAudit;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;

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
final class LifecycleCoordinator implements OperatorCoordinator {

    private final boolean sink;
    private final Map<String, String> options;

    private boolean startedLogged;
    private boolean failedLogged;

    LifecycleCoordinator(boolean sink, Map<String, String> options) {
        this.sink = sink;
        this.options = options;
    }

    @Override
    public void start() {}

    @Override
    public void close() {}

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        if (!(event instanceof LifecycleStartedEvent) || startedLogged) {
            return;
        }
        startedLogged = true;
        failedLogged = false; // a fresh successful start re-arms failure logging
        if (sink) {
            LifecycleAudit.writeStarted(null, options);
        } else {
            LifecycleAudit.readStarted(null, options);
        }
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        if (failedLogged) {
            return;
        }
        failedLogged = true;
        final String msg = reason != null ? reason.getMessage() : null;
        if (sink) {
            LifecycleAudit.writeFailed(null, options, msg);
        } else {
            LifecycleAudit.readFailed(null, options, msg);
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
        startedLogged = false;
        failedLogged = false;
    }

    /** Serializable factory carried inside the JobGraph; recreated on the JM. */
    static final class Provider implements OperatorCoordinator.Provider {

        private static final long serialVersionUID = 1L;

        private final OperatorID operatorID;
        private final boolean sink;
        private final Map<String, String> options;

        Provider(OperatorID operatorID, boolean sink, Map<String, String> options) {
            this.operatorID = operatorID;
            this.sink = sink;
            this.options = options;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new LifecycleCoordinator(sink, options);
        }
    }
}
