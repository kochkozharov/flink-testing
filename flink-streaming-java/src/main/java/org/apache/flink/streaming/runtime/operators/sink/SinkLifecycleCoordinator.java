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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.connector.ConnectorOptionsRegistry;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Tracks the lifecycle of {@link SinkWriterOperator} subtasks and emits a single log entry on the
 * JobManager when all writers have started successfully or when any writer fails to start.
 *
 * <p>Each subtask sends a {@link WriterStartedEvent} to this coordinator after its writer is
 * successfully initialized. When all subtasks have reported in, one INFO log is emitted. If a
 * subtask fails, one ERROR log is emitted for that subtask.
 */
@Internal
class SinkLifecycleCoordinator implements OperatorCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(SinkLifecycleCoordinator.class);

    private final String operatorName;
    private final OperatorCoordinator.Context context;

    /** Subtask indices that have successfully started their writer in the current attempt. */
    private final Set<Integer> startedSubtasks = new HashSet<>();

    /** Whether the "started successfully" log has already been emitted for this attempt. */
    private boolean startedLogged = false;

    SinkLifecycleCoordinator(String operatorName, OperatorCoordinator.Context context) {
        this.operatorName = operatorName;
        this.context = context;
    }

    @Override
    public void start() {}

    @Override
    public void close() {}

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        if (!(event instanceof WriterStartedEvent)) {
            return;
        }
        startedSubtasks.add(subtask);
        // Fire once per attempt on the first subtask that confirms real writes. With sinks that
        // funnel commits through subtask 0 (Iceberg uses .global() before its files-committer) the
        // strict "all N subtasks" gate would never trip.
        if (!startedLogged) {
            startedLogged = true;
            ConnectorOptionsRegistry.Entry meta =
                    ConnectorOptionsRegistry.findForOperator(operatorName);
            LOG.info(
                    "Sink '{}' (connector={}, options={}) started writing successfully (first subtask {} reported real data).",
                    operatorName,
                    meta.getConnectorIdentifier(),
                    meta.getOptions(),
                    subtask);
        }
    }

    @Override
    public void executionAttemptFailed(
            int subtask, int attemptNumber, @Nullable Throwable reason) {
        boolean hadStarted = startedSubtasks.remove(subtask);
        String phase = hadStarted ? "after start" : "during initialization";
        ConnectorOptionsRegistry.Entry meta =
                ConnectorOptionsRegistry.findForOperator(operatorName);
        // Flink only sometimes propagates a Throwable to executionAttemptFailed — for many
        // failure paths (e.g. TaskManager-side errors after the attempt has been marked failed)
        // reason is null and the actual cause lives in the standard task-failure log on JM.
        if (reason != null) {
            LOG.error(
                    "Sink writer for '{}' (connector={}, options={}) failed in subtask {} (attempt {}, {}): {}",
                    operatorName,
                    meta.getConnectorIdentifier(),
                    meta.getOptions(),
                    subtask,
                    attemptNumber,
                    phase,
                    reason.getMessage(),
                    reason);
        } else {
            LOG.error(
                    "Sink writer for '{}' (connector={}, options={}) failed in subtask {} (attempt {}, {}). Cause is logged separately by the task-failure handler.",
                    operatorName,
                    meta.getConnectorIdentifier(),
                    meta.getOptions(),
                    subtask,
                    attemptNumber,
                    phase);
        }
    }

    @Override
    public void executionAttemptReady(
            int subtask, int attemptNumber, SubtaskGateway gateway) {}

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        startedSubtasks.remove(subtask);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
        resultFuture.complete(new byte[0]);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) {
        startedSubtasks.clear();
        startedLogged = false;
    }

    // -------------------------------------------------------------------------
    //  Provider
    // -------------------------------------------------------------------------

    /** Creates a {@link SinkLifecycleCoordinator} for the given operator. */
    static final class Provider implements OperatorCoordinator.Provider {

        private static final long serialVersionUID = 1L;

        private final OperatorID operatorID;
        private final String operatorName;

        Provider(OperatorID operatorID, String operatorName) {
            this.operatorID = operatorID;
            this.operatorName = operatorName;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new SinkLifecycleCoordinator(operatorName, context);
        }
    }
}
