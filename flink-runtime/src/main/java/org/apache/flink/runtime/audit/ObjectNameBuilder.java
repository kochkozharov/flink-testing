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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Synthesises the {@code objectName} field of an audit event as
 * {@code <host(s)>:<connector>:<database>:<table>} from a connector's options map. Each part is
 * dropped when not derivable (eg Kafka has no database concept). Connector is always present;
 * falls back to {@link #UNDEFINED}.
 *
 * <p>The same heuristics run on opts from SQL DDL ({@code getResolvedTable().getOptions()}),
 * DataStream reflection ({@code ConnectorIntrospection.{source,sink,asyncFunction}Options}), or
 * lookup-join — so every emit path ends up with the same objectName shape.
 *
 * <p>Targets four connectors: Kafka, ClickHouse, Iceberg, HBase. Adding a new connector usually
 * means recognising one or two extra option keys here, not a new code path.
 */
@Internal
final class ObjectNameBuilder {

    private static final String UNDEFINED = "UNDEFINED";

    private ObjectNameBuilder() {}

    static String build(Map<String, String> opts) {
        final String host = host(opts);
        final String connector = opts == null ? null : opts.get("connector");
        final String db = database(opts);
        final String table = tableName(opts);

        final StringBuilder sb = new StringBuilder();
        if (host != null && !host.isEmpty()) {
            sb.append(host).append(':');
        }
        sb.append(connector == null || connector.isEmpty() ? UNDEFINED : connector);
        if (db != null && !db.isEmpty()) {
            sb.append(':').append(db);
        }
        if (table != null && !table.isEmpty()) {
            sb.append(':').append(table);
        }
        return sb.toString();
    }

    // ----- host -----------------------------------------------------------------------------

    /**
     * Address(es) of the external system. The lookup order matches the four supported connectors:
     * Iceberg-SQL (src-catalog JSON), Kafka, HBase, ClickHouse (and DataStream-side Iceberg whose
     * reflection puts {@code uri}/{@code warehouse} into the map directly).
     */
    private static String host(Map<String, String> opts) {
        if (opts == null) {
            return null;
        }

        // Iceberg-SQL: every DDL option lands in one JSON-encoded "src-catalog" value, so plain
        // map lookups miss everything. Pluck the catalog URI out of it.
        final String icebergSqlUri = srcCatalogValue(opts, "uri");
        if (icebergSqlUri != null) {
            return extractAuthority(icebergSqlUri);
        }
        final String icebergSqlWarehouse = srcCatalogValue(opts, "warehouse");
        if (icebergSqlWarehouse != null) {
            return extractAuthority(icebergSqlWarehouse);
        }

        // Kafka: SQL DDL and DataStream reflection both use "properties.bootstrap.servers".
        final String kafka = opts.get("properties.bootstrap.servers");
        if (kafka != null && !kafka.isEmpty()) {
            return kafka;
        }

        // HBase: SQL DDL uses bare "zookeeper.quorum"; our async-lookup reflection prefixes it
        // with "properties.hbase." to keep the namespace flat with other "properties.*" keys.
        for (String key :
                new String[] {"zookeeper.quorum", "properties.hbase.zookeeper.quorum"}) {
            final String v = opts.get(key);
            if (v != null && !v.isEmpty()) {
                return v;
            }
        }

        // ClickHouse-SQL: "url"="jdbc:clickhouse://host:port".
        // Iceberg-DataStream: ConnectorIntrospection reflects "uri" (REST) or "warehouse" (Hadoop)
        // out of the TableLoader's catalog properties.
        for (String key : new String[] {"url", "uri", "warehouse"}) {
            final String v = opts.get(key);
            if (v != null && !v.isEmpty()) {
                final String parsed = extractAuthority(v);
                if (parsed != null && !parsed.isEmpty()) {
                    return parsed;
                }
            }
        }
        return null;
    }

    // ----- database -------------------------------------------------------------------------

    /**
     * The container the table lives in (database / namespace). Some connectors have an explicit
     * key for it; others fold it into a compound table identifier that we split.
     */
    private static String database(Map<String, String> opts) {
        if (opts == null) {
            return null;
        }

        // Iceberg-SQL: "catalog-database":"db" in src-catalog JSON.
        final String icebergSql = srcCatalogValue(opts, "catalog-database");
        if (icebergSql != null) {
            return icebergSql;
        }

        // ClickHouse-SQL: explicit "database-name" DDL option.
        final String clickhouse = opts.get("database-name");
        if (clickhouse != null && !clickhouse.isEmpty()) {
            return clickhouse;
        }

        // Iceberg-DataStream: ConnectorIntrospection puts the full "db.table" identifier into
        // "table"; split it here so buildObjectName gets the db part separately.
        final String compoundDot = opts.get("table");
        if (compoundDot != null) {
            final int dot = compoundDot.lastIndexOf('.');
            if (dot > 0) {
                return compoundDot.substring(0, dot);
            }
        }

        // HBase: SQL DDL "table-name" may be "namespace:name". Extract the namespace.
        final String compoundColon = opts.get("table-name");
        if (compoundColon != null) {
            final int col = compoundColon.lastIndexOf(':');
            if (col > 0) {
                return compoundColon.substring(0, col);
            }
        }
        return null;
    }

    // ----- table ----------------------------------------------------------------------------

    /**
     * Leaf table/topic name. Per-connector key precedence; compound identifiers ({@code db.table},
     * {@code namespace:table}) are stripped so only the trailing part remains.
     */
    private static String tableName(Map<String, String> opts) {
        if (opts == null) {
            return null;
        }

        // Iceberg-SQL: "catalog-table":"events" in src-catalog JSON.
        final String icebergSql = srcCatalogValue(opts, "catalog-table");
        if (icebergSql != null) {
            return icebergSql;
        }

        // ClickHouse-SQL + HBase: "table-name". HBase may carry "namespace:name" — drop ns.
        final String tn = opts.get("table-name");
        if (tn != null && !tn.isEmpty()) {
            final int col = tn.lastIndexOf(':');
            return col >= 0 ? tn.substring(col + 1) : tn;
        }

        // Kafka: no database concept — topic IS the leaf.
        final String topic = opts.get("topic");
        if (topic != null && !topic.isEmpty()) {
            return topic;
        }

        // Iceberg-DataStream: full identifier in "table"; "catalog-table" carries leaf only.
        final String compound = opts.get("table");
        if (compound != null && !compound.isEmpty()) {
            final int dot = compound.lastIndexOf('.');
            return dot >= 0 ? compound.substring(dot + 1) : compound;
        }
        final String leaf = opts.get("catalog-table");
        if (leaf != null && !leaf.isEmpty()) {
            return leaf;
        }
        return null;
    }

    // ----- shared helpers -------------------------------------------------------------------

    /**
     * Strips {@code scheme://} prefix and {@code /path?query} suffix from a URL, leaving the
     * authority part ({@code host[:port]}). Returns the input unchanged when neither is present —
     * useful for already-bare hosts. Handles double schemes like {@code jdbc:clickhouse://...}
     * by always matching the LAST {@code "://"}, so the kept rest is just the address.
     */
    private static String extractAuthority(String url) {
        if (url == null) {
            return null;
        }
        final int schemeIdx = url.indexOf("://");
        String rest = schemeIdx >= 0 ? url.substring(schemeIdx + 3) : url;
        int cut = -1;
        final int slash = rest.indexOf('/');
        if (slash >= 0) {
            cut = slash;
        }
        final int q = rest.indexOf('?');
        if (q >= 0 && (cut < 0 || q < cut)) {
            cut = q;
        }
        if (cut >= 0) {
            rest = rest.substring(0, cut);
        }
        return rest;
    }

    /**
     * Iceberg-SQL specifically folds every DDL option (incl. nested {@code catalog-props})
     * into a single JSON-encoded value under the {@code src-catalog} key. Rather than bring a
     * JSON parser into flink-runtime, we match {@code "key":"value"} with a regex — works for
     * both top-level and nested keys. Returns {@code null} when src-catalog is missing or the
     * key isn't found.
     */
    private static String srcCatalogValue(Map<String, String> opts, String key) {
        final String src = opts.get("src-catalog");
        if (src == null || src.isEmpty()) {
            return null;
        }
        final Matcher m =
                Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\"([^\"]*)\"")
                        .matcher(src);
        return m.find() ? m.group(1) : null;
    }
}
