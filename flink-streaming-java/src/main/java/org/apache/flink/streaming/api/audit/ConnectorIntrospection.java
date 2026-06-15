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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Best-effort reflection helpers that derive SQL-style identity (connector, topic, table,
 * bootstrap servers, ...) from FLIP-27 / FLIP-191 connector instances. Called from the DataStream
 * API audit patches in {@code DataStream}, {@code StreamExecutionEnvironment} and
 * {@code AsyncDataStream} to populate audit-event options on the DataStream side, matching what
 * the planner sets from DDL {@code WITH (...)} options on the Table API side.
 *
 * <p>Dispatch is by FQN substring so this class has no compile-time dependency on connector
 * modules. Unknown connectors fall back to {@code connector=<SimpleClassName>}.
 *
 * <p>Layout:
 * <ol>
 *   <li>PUBLIC API — entry points called from the patched DataStream classes.</li>
 *   <li>Per-connector reflection blocks — one section per connector (Kafka, Iceberg, HBase).
 *       Source/sink/lookup helpers for the same connector live together.</li>
 *   <li>Reflection utilities — generic {@code readField}/{@code rowTypeFieldNames}/etc.</li>
 * </ol>
 */
@Internal
public final class ConnectorIntrospection {

    private ConnectorIntrospection() {}

    // ========================================================================
    //  PUBLIC API
    //  Entry points used by the DataStream audit patches.
    // ========================================================================

    /**
     * Source-side identity for FLIP-27 {@code Source<T,?,?>} instances passed to
     * {@code StreamExecutionEnvironment.fromSource(...)}.
     */
    public static Map<String, String> sourceOptions(Object source) {
        final Map<String, String> opts = new LinkedHashMap<>();
        if (source == null) {
            return opts;
        }
        final String fqn = source.getClass().getName();
        if (fqn.contains("KafkaSource")) {
            opts.put("connector", "kafka");
            kafkaSource(source, opts);
        } else if (fqn.contains("IcebergSource")) {
            opts.put("connector", "iceberg");
            icebergSource(source, opts);
        } else {
            opts.put("connector", source.getClass().getSimpleName());
        }
        return opts;
    }

    /**
     * Sink-side identity for FLIP-191 {@code Sink<T>} instances passed to
     * {@code DataStream.sinkTo(...)}.
     */
    public static Map<String, String> sinkOptions(Object sink) {
        final Map<String, String> opts = new LinkedHashMap<>();
        if (sink == null) {
            return opts;
        }
        final String fqn = sink.getClass().getName();
        if (fqn.contains("KafkaSink")) {
            opts.put("connector", "kafka");
            kafkaSink(sink, opts);
        } else if (fqn.contains("IcebergSink")) {
            opts.put("connector", "iceberg");
            icebergSink(sink, opts);
        } else {
            opts.put("connector", sink.getClass().getSimpleName());
        }
        return opts;
    }

    /**
     * Lookup-function identity for {@link
     * org.apache.flink.streaming.api.datastream.AsyncDataStream} — both async and sync variants of
     * Flink connector lookup functions (HBase, ...). Custom user {@code AsyncFunction} classes
     * fall back to {@code connector=<SimpleClassName>}.
     */
    public static Map<String, String> asyncFunctionOptions(Object func) {
        final Map<String, String> opts = new LinkedHashMap<>();
        if (func == null) {
            return opts;
        }
        final String fqn = func.getClass().getName();
        if (fqn.contains(".hbase") && fqn.contains("LookupFunction")) {
            opts.put("connector", "hbase");
            hbaseLookup(func, opts);
        } else {
            opts.put("connector", func.getClass().getSimpleName());
        }
        return opts;
    }

    /**
     * Modified columns for a V2 {@code Sink<T>} — preferred entry point for {@code sinkTo(...)}.
     * First tries to extract the column list from the sink instance itself (eg Iceberg V2 caches
     * its target table schema in the {@code flinkRowType} field); falls back to {@link
     * #modifiedColumns(TypeInformation)} if the sink doesn't expose schema.
     *
     * <p>Needed because the common {@code DataStream<RowData>} → IcebergSink pipeline produces
     * {@code GenericTypeInfo<RowData>} (not {@code RowTypeInfo}) — typeInfo alone would always
     * return empty. The sink instance is already configured against the target table, so it
     * usually has the columns cached.
     */
    public static List<String> sinkColumns(Object sink, TypeInformation<?> typeInfo) {
        if (sink != null) {
            final String fqn = sink.getClass().getName();
            // Iceberg V2: IcebergSink.flinkRowType is a
            // org.apache.flink.table.types.logical.RowType — read field-names reflectively.
            if (fqn.contains("IcebergSink")) {
                final List<String> names = rowTypeFieldNames(readField(sink, "flinkRowType"));
                if (!names.isEmpty()) {
                    return names;
                }
            }
            // KafkaSink carries no schema — fall through to typeInfo (works for POJO/Tuple/Row).
        }
        return modifiedColumns(typeInfo);
    }

    /**
     * Modified columns from a stream's {@link TypeInformation} — field names for POJO/Tuple/Row,
     * empty list otherwise. Used directly for legacy V1 sinks ({@code addSink(SinkFunction)})
     * where the function carries no schema info; called as a fallback from {@link #sinkColumns}.
     */
    public static List<String> modifiedColumns(TypeInformation<?> typeInfo) {
        if (typeInfo == null) {
            return Collections.emptyList();
        }
        if (typeInfo instanceof PojoTypeInfo) {
            final PojoTypeInfo<?> pti = (PojoTypeInfo<?>) typeInfo;
            final List<String> out = new ArrayList<>(pti.getArity());
            for (int i = 0; i < pti.getArity(); i++) {
                out.add(pti.getPojoFieldAt(i).getField().getName());
            }
            return out;
        }
        if (typeInfo instanceof TupleTypeInfo) {
            return Arrays.asList(((TupleTypeInfo<?>) typeInfo).getFieldNames());
        }
        if (typeInfo instanceof RowTypeInfo) {
            return Arrays.asList(((RowTypeInfo) typeInfo).getFieldNames());
        }
        return Collections.emptyList();
    }

    // ========================================================================
    //  Kafka
    //  KafkaSource (FLIP-27)  +  KafkaSink (FLIP-191)
    // ========================================================================

    private static void kafkaSource(Object source, Map<String, String> opts) {
        // KafkaSource has: KafkaSubscriber subscriber, Properties props, Boundedness, ...
        // Subscriber variants:
        //   - TopicListSubscriber     → field topics: List<String>
        //   - TopicPatternSubscriber  → field topicPattern: Pattern
        //   - PartitionSetSubscriber  → not handled (rare; would carry topic-partitions list)
        final Object subscriber = readField(source, "subscriber");
        if (subscriber != null) {
            final Object topics = readField(subscriber, "topics");
            if (topics instanceof Collection) {
                opts.put("topic", join((Collection<?>) topics));
            }
            final Object pattern = readField(subscriber, "topicPattern");
            if (pattern != null) {
                opts.put("topic-pattern", pattern.toString());
            }
        }
        final Object props = readField(source, "props");
        if (props instanceof Properties) {
            final Properties p = (Properties) props;
            putIfPresent(opts, "properties.bootstrap.servers", p.getProperty("bootstrap.servers"));
            putIfPresent(opts, "properties.group.id", p.getProperty("group.id"));
        }
    }

    private static void kafkaSink(Object sink, Map<String, String> opts) {
        // KafkaSink has: KafkaRecordSerializationSchema recordSerializer,
        //                Properties kafkaProducerConfig,
        //                DeliveryGuarantee, String transactionalIdPrefix.
        final Object props = readField(sink, "kafkaProducerConfig");
        if (props instanceof Properties) {
            putIfPresent(
                    opts,
                    "properties.bootstrap.servers",
                    ((Properties) props).getProperty("bootstrap.servers"));
        }
        final Object serializer = readField(sink, "recordSerializer");
        if (serializer == null) {
            return;
        }
        // setTopic("events-out") wraps a constant-returning Function via CachingTopicSelector;
        // dynamic per-record selectors throw on apply(null) — swallow and leave topic unset.
        final Object selector = readField(serializer, "topicSelector");
        if (selector instanceof java.util.function.Function) {
            try {
                @SuppressWarnings({"rawtypes", "unchecked"})
                final Object topic = ((java.util.function.Function) selector).apply(null);
                if (topic instanceof String) {
                    opts.put("topic", (String) topic);
                }
            } catch (Throwable ignored) {
                // dynamic topic selector (per-record) — can't determine statically
            }
        }
    }

    // ========================================================================
    //  Iceberg
    //  IcebergSource (FLIP-27)  +  IcebergSink V2 (FLIP-191)
    //  Note: legacy FlinkSink doesn't go through sinkTo(...) — see DataStream.sinkTo patch
    //        for its (separate) upstream-walk identity detection.
    // ========================================================================

    private static void icebergSource(Object source, Map<String, String> opts) {
        // IcebergSource: String tableName ("events"), TableLoader tableLoader.
        final Object tableName = readField(source, "tableName");
        if (tableName instanceof String) {
            opts.put("catalog-table", (String) tableName);
        }
        // TableLoader$CatalogTableLoader.identifier carries the full "db.table" identifier.
        final Object loader = readField(source, "tableLoader");
        if (loader != null) {
            final Object identifier = readField(loader, "identifier");
            if (identifier != null) {
                opts.put("table", identifier.toString());
            }
        }
    }

    private static void icebergSink(Object sink, Map<String, String> opts) {
        // IcebergSink: TableLoader tableLoader, RowType flinkRowType, ...
        // (flinkRowType is read separately by sinkColumns(...) for modified_columns).
        final Object loader = readField(sink, "tableLoader");
        if (loader != null) {
            final Object identifier = readField(loader, "identifier");
            if (identifier != null) {
                final String full = identifier.toString();
                opts.put("table", full);
                // Surface the table part separately for the {connector}:{objectName} format.
                final int dot = full.lastIndexOf('.');
                opts.put("catalog-table", dot >= 0 ? full.substring(dot + 1) : full);
            }
        }
    }

    // ========================================================================
    //  HBase
    //  Lookup functions used via AsyncDataStream (async + sync variants).
    //  FQNs:
    //    org.apache.flink.connector.hbase{1,2}.source.HBaseRowDataAsyncLookupFunction
    //    org.apache.flink.connector.hbase{1,2}.source.HBaseRowDataLookupFunction
    // ========================================================================

    private static void hbaseLookup(Object func, Map<String, String> opts) {
        // Fields (vary slightly across versions):
        //   String hTableName               — target HBase table
        //   transient Configuration conf    — under name configuration/hbaseConf/config
        //   byte[] serializedConfig         — older versions serialize the conf instead
        // The hadoop Configuration carries "hbase.zookeeper.quorum" and friends.
        final Object tableName = readField(func, "hTableName");
        if (tableName instanceof String) {
            opts.put("table-name", (String) tableName);
        }
        Object conf = readField(func, "configuration");
        if (conf == null) {
            conf = readField(func, "hbaseConf");
        }
        if (conf == null) {
            conf = readField(func, "config");
        }
        if (conf == null || conf.getClass().isArray()) {
            // No live Configuration object (eg only serializedConfig byte[]).
            return;
        }
        // org.apache.hadoop.conf.Configuration#get(String) — reflectively to avoid a
        // compile-time dependency on hadoop-common.
        try {
            final Method get = conf.getClass().getMethod("get", String.class);
            putIfPresent(opts, "properties.hbase.zookeeper.quorum",
                    (String) get.invoke(conf, "hbase.zookeeper.quorum"));
            putIfPresent(opts, "properties.zookeeper.znode.parent",
                    (String) get.invoke(conf, "zookeeper.znode.parent"));
        } catch (Throwable ignored) {
            // not a hadoop Configuration, or method missing
        }
    }

    // ========================================================================
    //  Reflection utilities
    //  Generic, connector-agnostic helpers used by the blocks above.
    // ========================================================================

    /**
     * Walks the class hierarchy looking for the field by name. Best-effort: returns {@code null}
     * on any failure (missing field, access denied, ...) so call-sites can chain safely.
     */
    private static Object readField(Object instance, String name) {
        if (instance == null) {
            return null;
        }
        for (Class<?> c = instance.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
            try {
                final Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(instance);
            } catch (NoSuchFieldException ignored) {
                // try parent
            } catch (Throwable ignored) {
                return null;
            }
        }
        return null;
    }

    /**
     * Invokes {@code RowType.getFieldNames()} via reflection. flink-streaming-java has no
     * compile-time dependency on flink-table-common, so we can't reference RowType directly.
     */
    private static List<String> rowTypeFieldNames(Object rowType) {
        if (rowType == null) {
            return Collections.emptyList();
        }
        try {
            @SuppressWarnings("unchecked")
            final List<String> names =
                    (List<String>) rowType.getClass().getMethod("getFieldNames").invoke(rowType);
            return names != null ? names : Collections.emptyList();
        } catch (Throwable ignored) {
            return Collections.emptyList();
        }
    }

    private static void putIfPresent(Map<String, String> opts, String key, String value) {
        if (value != null && !value.isEmpty()) {
            opts.put(key, value);
        }
    }

    private static String join(Collection<?> values) {
        final StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Object v : values) {
            if (v == null) {
                continue;
            }
            if (!first) {
                sb.append(',');
            }
            sb.append(v);
            first = false;
        }
        return sb.toString();
    }
}
