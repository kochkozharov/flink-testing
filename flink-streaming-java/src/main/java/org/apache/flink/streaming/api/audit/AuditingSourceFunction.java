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
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pass-through decorator over a user-supplied legacy {@link SourceFunction}. Lifecycle audit
 * events (A2 / C1 / C2 / A3) are emitted by the probe-operator's coordinator on the source
 * JobVertex; this wrapper exists to capture identity for the audit-event options.
 *
 * <p>Two subclasses below preserve the {@link ParallelSourceFunction} marker — Flink uses it to
 * decide whether the source can be parallelised.
 */
@Internal
public class AuditingSourceFunction<T> extends RichSourceFunction<T> {

    private static final long serialVersionUID = 1L;

    protected final SourceFunction<T> delegate;
    protected final String sourceName;

    public AuditingSourceFunction(SourceFunction<T> delegate, String sourceName) {
        this.delegate = delegate;
        this.sourceName = sourceName;
    }

    public SourceFunction<T> unwrap() {
        return delegate;
    }

    public Map<String, String> auditOptions() {
        final Map<String, String> opts = new LinkedHashMap<>();
        opts.putIfAbsent("connector", delegate.getClass().getSimpleName());
        if (sourceName != null) {
            opts.putIfAbsent("source.name", sourceName);
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
    public void run(SourceContext<T> ctx) throws Exception {
        delegate.run(ctx);
    }

    @Override
    public void cancel() {
        delegate.cancel();
    }

    @Override
    public void close() throws Exception {
        if (delegate instanceof RichFunction) {
            ((RichFunction) delegate).close();
        }
    }

    /** Parallel variant — preserves the marker so Flink keeps the source parallelisable. */
    public static final class Parallel<T> extends AuditingSourceFunction<T>
            implements ParallelSourceFunction<T> {
        private static final long serialVersionUID = 1L;

        public Parallel(SourceFunction<T> delegate, String sourceName) {
            super(delegate, sourceName);
        }
    }
}
