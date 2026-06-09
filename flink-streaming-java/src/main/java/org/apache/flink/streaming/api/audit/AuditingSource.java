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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.audit.LifecycleAudit;

import java.util.Map;

/**
 * Transparent decorator over a user-supplied FLIP-27 {@link Source}. Forwards every method to the
 * delegate; if the connector throws while constructing its enumerator or reader (i.e. couldn't
 * establish a connection to the external system) emits an A3 audit event and rethrows.
 *
 * <p>Installed automatically from {@link
 * org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#fromSource}, so user code
 * stays unchanged.
 */
@Internal
public final class AuditingSource<T, SplitT extends SourceSplit, EnumChkT>
        implements Source<T, SplitT, EnumChkT> {

    private static final long serialVersionUID = 1L;

    private final Source<T, SplitT, EnumChkT> delegate;
    private final String objectName;
    private final Map<String, String> options;

    public AuditingSource(
            Source<T, SplitT, EnumChkT> delegate,
            String objectName,
            Map<String, String> options) {
        this.delegate = delegate;
        this.objectName = objectName;
        this.options = options;
    }

    public Source<T, SplitT, EnumChkT> unwrap() {
        return delegate;
    }

    public Map<String, String> auditOptions() {
        return identityOptions();
    }

    @Override
    public Boundedness getBoundedness() {
        return delegate.getBoundedness();
    }

    @Override
    public SplitEnumerator<SplitT, EnumChkT> createEnumerator(
            SplitEnumeratorContext<SplitT> enumContext) throws Exception {
        return new AuditingSplitEnumerator<>(
                delegate.createEnumerator(enumContext), identityOptions());
    }

    @Override
    public SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(
            SplitEnumeratorContext<SplitT> enumContext, EnumChkT checkpoint) throws Exception {
        return new AuditingSplitEnumerator<>(
                delegate.restoreEnumerator(enumContext, checkpoint), identityOptions());
    }

    @Override
    public SourceReader<T, SplitT> createReader(SourceReaderContext readerContext) throws Exception {
        return delegate.createReader(readerContext);
    }

    @Override
    public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
        return delegate.getSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer() {
        return delegate.getEnumeratorCheckpointSerializer();
    }

    private Map<String, String> identityOptions() {
        // Best-effort introspection of the underlying connector (KafkaSource → connector=kafka,
        // topic=events, properties.bootstrap.servers=...) so the audit event identity matches what
        // the Table API planner produces from DDL options.
        final Map<String, String> base = new java.util.LinkedHashMap<>(
                ConnectorIntrospection.sourceOptions(delegate));
        if (options != null) {
            options.forEach(base::putIfAbsent);
        }
        if (objectName != null) {
            base.putIfAbsent("source.name", objectName);
        }
        return base;
    }
}
