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
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A transparent pass-through operator inserted right after a source (or right before a sink). It
 * forwards every record unchanged and, on the very first record, fires a single {@link
 * LifecycleStartedEvent} to its {@link LifecycleCoordinator} on the JM (→ C1 for sources, C3 for
 * sinks). Being a one-input operator it has a natural per-record hook ({@link #processElement}),
 * which works identically for V1 and V2 connectors — that is what makes the approach universal.
 */
@Internal
public final class LifecycleProbeOperator<T> extends AbstractStreamOperator<T>
        implements OneInputStreamOperator<T, T>, OperatorEventHandler {

    private transient OperatorEventGateway operatorEventGateway;
    private transient boolean startedSent;

    public void setOperatorEventGateway(OperatorEventGateway gateway) {
        this.operatorEventGateway = gateway;
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        output.collect(element);
        if (!startedSent && operatorEventGateway != null) {
            startedSent = true;
            operatorEventGateway.sendEventToCoordinator(new LifecycleStartedEvent());
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        // The coordinator never sends events to this operator; nothing to handle.
    }
}
