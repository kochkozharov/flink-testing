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

package org.apache.flink.runtime.audit;

import org.apache.flink.annotation.Internal;

import java.util.Map;

/**
 * Builds the audit event's {@code objectName} as {@code <host>:<connector>:<database>:<table>}.
 * Missing parts are dropped; {@code connector} is always present (falls back to {@link #UNDEFINED}).
 *
 * <p>Field-driven: each part is extracted by trying a list of common key names in priority order
 * — no connector-name dispatch. A new SQL connector that follows standard naming conventions
 * ({@code properties.bootstrap.servers}, {@code url}, {@code table-name}, {@code database-name},
 * ...) works without code changes. To support a new convention, add a key to the relevant list.
 *
 * <p>Sources of opts:
 * <ul>
 *   <li>Table API DDL — {@code getResolvedTable().getOptions()} surfaces every {@code WITH (...)}
 *       option verbatim.</li>
 *   <li>DataStream — {@code ConnectorIntrospection.{source,sink,asyncFunction}Options} fills the
 *       same standard keys via reflection, so the same extraction logic works on both sides.</li>
 * </ul>
 */
@Internal
final class ObjectNameBuilder {

    private static final String UNDEFINED = "UNDEFINED";

    private ObjectNameBuilder() {}

    static String build(Map<String, String> opts) {
        if (opts == null) {
            return UNDEFINED;
        }
        return join(host(opts), opts.get("connector"), database(opts), table(opts));
    }

    // ========================================================================
    //  Host — address of the external system.
    //  Priority: direct address keys → URL-like (need scheme stripping)
    //          → Iceberg-SQL src-catalog JSON (last because most specific).
    // ========================================================================

    /** Verbatim host[:port] keys — used as-is, no parsing. */
    private static final String[] HOST_DIRECT = {
        "properties.bootstrap.servers",       // Kafka (and any kafka-* connector)
        "zookeeper.quorum",                   // HBase SQL DDL
        "properties.hbase.zookeeper.quorum",  // HBase DataStream lookup
        "hostname", "host", "endpoint",       // generic SQL conventions
    };

    /** URL-like keys — scheme stripped to leave host[:port]. */
    private static final String[] HOST_URL_LIKE = {
        "url",                                // JDBC: jdbc:clickhouse://host:port
        "uri",                                // REST/HTTP
        "warehouse",                          // Iceberg: s3://bucket, hdfs://nn:8020/...
    };

    private static String host(Map<String, String> opts) {
        for (String k : HOST_DIRECT) {
            final String v = opts.get(k);
            if (notEmpty(v)) {
                return v;
            }
        }
        for (String k : HOST_URL_LIKE) {
            final String auth = extractAuthority(opts.get(k));
            if (notEmpty(auth)) {
                return auth;
            }
        }
        return null;
    }

    // ========================================================================
    //  Database — container (db/schema/namespace) the table lives in.
    //  Some connectors fold it into a compound identifier we split here.
    // ========================================================================

    /** Direct db-name keys. */
    private static final String[] DB_DIRECT = {
        "database-name",   // ClickHouse, JDBC, generic SQL
        "database",        // alternative naming
        "catalog-database",// catalog-based connectors
        "schema-name",     // some JDBC connectors
        "namespace",       // some catalog/object-store connectors
    };

    private static String database(Map<String, String> opts) {
        for (String k : DB_DIRECT) {
            final String v = opts.get(k);
            if (notEmpty(v)) {
                return v;
            }
        }
        // Compound identifier "db.table" in "table" (eg DataStream Iceberg).
        final String compoundDot = opts.get("table");
        if (notEmpty(compoundDot)) {
            final int dot = compoundDot.lastIndexOf('.');
            if (dot > 0) {
                return compoundDot.substring(0, dot);
            }
        }
        // Compound identifier "namespace:name" in "table-name" (HBase).
        final String compoundColon = opts.get("table-name");
        if (notEmpty(compoundColon)) {
            final int colon = compoundColon.lastIndexOf(':');
            if (colon > 0) {
                return compoundColon.substring(0, colon);
            }
        }
        return null;
    }

    // ========================================================================
    //  Table — leaf table/topic name.
    //  Compound identifiers (db.table, namespace:table) are stripped to the leaf.
    // ========================================================================

    private static String table(Map<String, String> opts) {
        // SQL DDL "table-name", possibly "namespace:name".
        final String tn = opts.get("table-name");
        if (notEmpty(tn)) {
            final int colon = tn.lastIndexOf(':');
            return colon >= 0 ? tn.substring(colon + 1) : tn;
        }
        // Kafka — topic is the leaf.
        final String topic = opts.get("topic");
        if (notEmpty(topic)) {
            return topic;
        }
        // Compound "db.table" (DataStream Iceberg).
        final String compound = opts.get("table");
        if (notEmpty(compound)) {
            final int dot = compound.lastIndexOf('.');
            return dot >= 0 ? compound.substring(dot + 1) : compound;
        }
        // File-based / catalog-based fallbacks.
        for (String k : new String[] {"catalog-table", "path"}) {
            final String v = opts.get(k);
            if (notEmpty(v)) {
                return v;
            }
        }
        return null;
    }

    // ========================================================================
    //  Helpers
    // ========================================================================

    private static String join(String host, String connector, String database, String table) {
        final StringBuilder sb = new StringBuilder();
        if (notEmpty(host)) {
            sb.append(host).append(':');
        }
        sb.append(notEmpty(connector) ? connector : UNDEFINED);
        if (notEmpty(database)) {
            sb.append(':').append(database);
        }
        if (notEmpty(table)) {
            sb.append(':').append(table);
        }
        return sb.toString();
    }

    private static boolean notEmpty(String s) {
        return s != null && !s.isEmpty();
    }

    /**
     * Strips {@code scheme://} prefix and trailing {@code /path?query} — leaves {@code host[:port]}.
     * Handles double-scheme JDBC URLs ({@code jdbc:clickhouse://host:port}) by matching the LAST
     * {@code "://"}. Returns null for null/empty input; returns input unchanged when neither
     * prefix nor suffix is present (already a bare host).
     */
    private static String extractAuthority(String url) {
        if (!notEmpty(url)) {
            return null;
        }
        final int schemeIdx = url.lastIndexOf("://");
        String rest = schemeIdx >= 0 ? url.substring(schemeIdx + 3) : url;
        int cut = rest.indexOf('/');
        final int q = rest.indexOf('?');
        if (q >= 0 && (cut < 0 || q < cut)) {
            cut = q;
        }
        return cut >= 0 ? rest.substring(0, cut) : rest;
    }

}
