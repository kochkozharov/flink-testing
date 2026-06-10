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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Best-effort reflection helpers that derive SQL-style identity (connector, topic, bootstrap
 * servers, ...) from FLIP-27 / FLIP-191 connector instances and {@link TypeInformation}. Used by
 * {@link AuditingSource} / {@link AuditingSink} to populate audit-event options on the DataStream
 * API side, matching what the planner sets from DDL {@code WITH (...)} options on the Table API
 * side.
 *
 * <p>Per-connector branches dispatch on the delegate's fully-qualified class name so the helper
 * has no compile-time dependency on connector modules. Unknown connectors fall back to {@code
 * connector=<SimpleClassName>}.
 */
@Internal
public final class ConnectorIntrospection {

    private ConnectorIntrospection() {}

    /** Source-side identity: connector kind + topic/table + relevant kafka/jdbc options. */
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
     * Async-function identity for {@link
     * org.apache.flink.streaming.api.datastream.AsyncDataStream}: connector kind + per-connector
     * options (eg HBase zookeeper quorum + table). Unknown async functions fall back to {@code
     * connector=<SimpleClassName>} same as source/sink dispatch.
     */
    public static Map<String, String> asyncFunctionOptions(Object func) {
        final Map<String, String> opts = new LinkedHashMap<>();
        if (func == null) {
            return opts;
        }
        final String fqn = func.getClass().getName();
        // Flink HBase connector lookup functions (both async and sync variants, HBase 1.x and 2.x):
        //   org.apache.flink.connector.hbase{1,2}.source.HBaseRowDataAsyncLookupFunction
        //   org.apache.flink.connector.hbase{1,2}.source.HBaseRowDataLookupFunction
        if (fqn.contains(".hbase") && fqn.contains("LookupFunction")) {
            opts.put("connector", "hbase");
            hbaseLookup(func, opts);
        } else {
            opts.put("connector", func.getClass().getSimpleName());
        }
        return opts;
    }

    /** Sink-side identity: connector kind + topic/table + relevant kafka/jdbc options. */
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
     * Best-effort extraction of "modified columns" for a sink — for DataStream there's no SQL
     * INSERT column list, so we approximate by listing the field names of the sink's input record
     * type. Works for POJOs, tuples and Row types; returns an empty list otherwise.
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

    // ------------------------------------------------------------------------
    //  Per-connector reflection. Branches MUST swallow all errors — best-effort.
    // ------------------------------------------------------------------------

    private static void kafkaSource(Object source, Map<String, String> opts) {
        // KafkaSource stores: KafkaSubscriber subscriber, Properties props, Boundedness, ...
        // KafkaSubscriber implementations: TopicListSubscriber (field topics: List<String>),
        // TopicPatternSubscriber (field topicPattern: Pattern), PartitionSetSubscriber, ...
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

    private static void icebergSource(Object source, Map<String, String> opts) {
        // IcebergSource has String tableName field (eg "events") and TableLoader tableLoader.
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
        final Object loader = readField(sink, "tableLoader");
        if (loader != null) {
            final Object identifier = readField(loader, "identifier");
            if (identifier != null) {
                opts.put("table", identifier.toString());
                // Also surface the table part separately for the connector:table objectName.
                final String full = identifier.toString();
                final int dot = full.lastIndexOf('.');
                opts.put("catalog-table", dot >= 0 ? full.substring(dot + 1) : full);
            }
        }
    }

    private static void hbaseLookup(Object func, Map<String, String> opts) {
        // HBaseRowData{Async}LookupFunction has fields:
        //   String hTableName            ← target HBase table
        //   transient Configuration configuration  (HBase 2.x), OR
        //   byte[] serializedConfig                (some versions serialize the conf)
        // The hadoop Configuration carries "hbase.zookeeper.quorum" and friends.
        final Object tableName = readField(func, "hTableName");
        if (tableName instanceof String) {
            opts.put("table-name", (String) tableName);
        }
        // Try common field names — different connector versions use different ones.
        Object conf = readField(func, "configuration");
        if (conf == null) {
            conf = readField(func, "hbaseConf");
        }
        if (conf == null) {
            conf = readField(func, "config");
        }
        if (conf != null && !conf.getClass().isArray()) {
            // org.apache.hadoop.conf.Configuration#get(String)
            try {
                final String quorum =
                        (String)
                                conf.getClass()
                                        .getMethod("get", String.class)
                                        .invoke(conf, "hbase.zookeeper.quorum");
                putIfPresent(opts, "properties.hbase.zookeeper.quorum", quorum);
                final String znode =
                        (String)
                                conf.getClass()
                                        .getMethod("get", String.class)
                                        .invoke(conf, "zookeeper.znode.parent");
                putIfPresent(opts, "properties.zookeeper.znode.parent", znode);
            } catch (Throwable ignored) {
                // not a hadoop Configuration, or method missing
            }
        }
    }

    private static void kafkaSink(Object sink, Map<String, String> opts) {
        // KafkaSink stores: KafkaRecordSerializationSchema recordSerializer, Properties
        // kafkaProducerConfig, DeliveryGuarantee, String transactionalIdPrefix.
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
        // setTopic("events-out") produces a constant-returning Function wrapped in
        // CachingTopicSelector; for dynamic per-record selectors apply(null) may throw — swallow.
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

    // ------------------------------------------------------------------------
    //  Reflection utilities
    // ------------------------------------------------------------------------

    /** Walks the class hierarchy looking for the field by name. Best-effort; returns null. */
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
