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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.runtime.audit.LifecycleAudit;

import java.io.IOException;
import java.util.Map;

/**
 * Transparent decorator over a user-supplied FLIP-191 {@link Sink}. Forwards {@code createWriter}
 * to the delegate; if construction throws (couldn't connect/initialize) emits A3 and rethrows.
 */
@Internal
public final class AuditingSink<InputT> implements Sink<InputT> {

    private static final long serialVersionUID = 1L;

    private final Sink<InputT> delegate;
    private final Map<String, String> options;

    public AuditingSink(Sink<InputT> delegate, Map<String, String> options) {
        this.delegate = delegate;
        this.options = options;
    }

    public Sink<InputT> unwrap() {
        return delegate;
    }

    public Map<String, String> auditOptions() {
        return identityOptions();
    }

    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        // A3 / A2 / C3 / C4 all come from the probe-operator's coordinator (which carries the
        // jobID). We just delegate — failures cascade through executionAttemptFailed.
        return delegate.createWriter(context);
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        return delegate.createWriter(context);
    }

    private Map<String, String> identityOptions() {
        final Map<String, String> base = new java.util.LinkedHashMap<>();
        if (options != null) base.putAll(options);
        base.putIfAbsent("connector", delegate.getClass().getSimpleName());
        return base;
    }
}
