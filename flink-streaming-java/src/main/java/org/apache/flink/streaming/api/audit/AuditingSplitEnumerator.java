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

package org.apache.flink.streaming.api.audit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.runtime.audit.LifecycleAudit;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Wraps the {@link SplitEnumerator} returned by {@link AuditingSource#createEnumerator} so that
 * failures from deferred connection setup (e.g. Kafka admin-client construction inside
 * {@code KafkaSourceEnumerator.start()}) also surface as A3.
 */
@Internal
final class AuditingSplitEnumerator<SplitT extends SourceSplit, CheckpointT>
        implements SplitEnumerator<SplitT, CheckpointT> {

    private final SplitEnumerator<SplitT, CheckpointT> delegate;
    private final Map<String, String> options;

    AuditingSplitEnumerator(
            SplitEnumerator<SplitT, CheckpointT> delegate, Map<String, String> options) {
        this.delegate = delegate;
        this.options = options;
    }

    @Override
    public void start() {
        // A3 / A2 are emitted by the probe-operator's coordinator (via executionAttemptFailed /
        // LifecycleConnectedEvent) so the events carry the real jobID. We just propagate the
        // throw — coordinator catches the cascading task failure and logs A3 with jobID.
        delegate.start();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        delegate.handleSplitRequest(subtaskId, requesterHostname);
    }

    @Override
    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        delegate.addSplitsBack(splits, subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {
        delegate.addReader(subtaskId);
    }

    @Override
    public CheckpointT snapshotState(long checkpointId) throws Exception {
        return delegate.snapshotState(checkpointId);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        delegate.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        delegate.handleSourceEvent(subtaskId, sourceEvent);
    }
}
