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
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pass-through decorator over a user-supplied legacy {@link SinkFunction}. Lifecycle audit events
 * (A2 / C3 / C4 / A3) are emitted by the probe-operator's coordinator on the sink JobVertex; this
 * wrapper exists so we have a place to capture connector identity for the audit-event options.
 */
@Internal
public final class AuditingSinkFunction<T> extends RichSinkFunction<T> {

    private static final long serialVersionUID = 1L;

    private final SinkFunction<T> delegate;
    private final String sinkName;

    public AuditingSinkFunction(SinkFunction<T> delegate, String sinkName) {
        this.delegate = delegate;
        this.sinkName = sinkName;
    }

    public SinkFunction<T> unwrap() {
        return delegate;
    }

    public Map<String, String> auditOptions() {
        final Map<String, String> opts = new LinkedHashMap<>();
        opts.putIfAbsent("connector", delegate.getClass().getSimpleName());
        if (sinkName != null) {
            opts.putIfAbsent("sink.name", sinkName);
        }
        return opts;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (delegate instanceof RichFunction) {
            ((RichFunction) delegate).setRuntimeContext(getRuntimeContext());
            ((RichFunction) delegate).open(parameters);
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        delegate.invoke(value, context);
    }

    @Override
    public void writeWatermark(org.apache.flink.api.common.eventtime.Watermark watermark)
            throws Exception {
        delegate.writeWatermark(watermark);
    }

    @Override
    public void finish() throws Exception {
        delegate.finish();
    }

    @Override
    public void close() throws Exception {
        if (delegate instanceof RichFunction) {
            ((RichFunction) delegate).close();
        }
    }
}
