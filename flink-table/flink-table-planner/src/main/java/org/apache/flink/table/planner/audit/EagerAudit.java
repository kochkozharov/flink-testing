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

package org.apache.flink.table.planner.audit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.audit.LifecycleAudit;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Eager (plan-translation) connector audit. Each connector ({@link DynamicTableSource} / {@link
 * DynamicTableSink}) is wrapped in a transparent audit proxy at its single birth point (the {@code
 * FactoryUtil.createDynamicTable*} call sites). The proxy emits C2 (source) / C4 (sink) — with the
 * identity captured once at creation — if creation throws or if any later connector call throws
 * ({@code getScanRuntimeProvider}, {@code getLookupRuntimeProvider}, {@code getSinkRuntimeProvider},
 * {@code getChangelogMode}, pushdown/overwrite abilities, ...).
 *
 * <p>The one thing that is not a connector method is schema validation ({@code
 * validateSchemaAndApplyImplicitCast} inside {@code convertSinkToRel}); a single catch there calls
 * {@link #emit} directly. So there is no top-level "net" and no per-statement guessing — every
 * failure is attributed to exactly the connector that raised it, which is correct even for a {@code
 * STATEMENT SET} with several sinks.
 *
 * <p>{@link #emit} is first-wins within one translation (scoped by {@link #begin()}/{@link
 * #clear()}): translation aborts on the first failure, and the guard also dedups an inner proxy emit
 * against an outer catch for the same exception. Everything is best-effort and swallowed: audit
 * never breaks translation and stays quiet in session mode.
 */
@Internal
public final class EagerAudit {

    private static final ThreadLocal<Boolean> EMITTED = new ThreadLocal<>();

    private EagerAudit() {}

    /** Opens a fresh window for one {@code translate(...)} call. */
    public static void begin() {
        EMITTED.set(Boolean.FALSE);
    }

    /** Closes the window. Always call from a {@code finally}. */
    public static void clear() {
        EMITTED.remove();
    }

    /**
     * Emits C4 (sink) or C2 (source) for {@code ctx}, at most once per translation (first wins).
     * Called by the connector proxies and by the schema-validation catch.
     */
    public static void emit(boolean sink, ContextResolvedTable ctx, String reason) {
        emit(sink, ctx, reason, null);
    }

    /**
     * Same as {@link #emit(boolean, ContextResolvedTable, String)} but additionally attaches the
     * SQL-intended modified columns of the sink (only meaningful for {@code sink == true}; pass
     * {@code null} on the source side).
     */
    public static void emit(
            boolean sink,
            ContextResolvedTable ctx,
            String reason,
            List<String> modifiedColumns) {
        try {
            if (Boolean.TRUE.equals(EMITTED.get())) {
                return; // already logged in this translation (inner proxy or earlier failure)
            }
            if (ctx == null) {
                return; // nothing to identify — don't consume the emit slot
            }
            EMITTED.set(Boolean.TRUE);
            final Map<String, String> options = ctx.getResolvedTable().getOptions();
            if (sink) {
                LifecycleAudit.writeFailed(null, options, reason, modifiedColumns);
            } else {
                LifecycleAudit.readFailed(null, options, reason);
            }
        } catch (Throwable ignored) {
            // Best-effort: never let audit break plan translation.
        }
    }

    // ------------------------------------------------------------------------
    //  Target-column resolution — turns the planner's {@code int[][]} target column
    //  path encoding into human-readable, SQL-intent column names.
    //  - {@code null} or empty targetColumns means "INSERT INTO sink SELECT ..." → all columns.
    //  - Top-level paths return the column name; nested paths walk into RowType using dot
    //    notation (e.g. {@code addr.city}); unresolvable indices fall back to {@code #N}.
    //  Nested paths are unreachable from vanilla Flink 1.20 SQL (parser rejects dotted INSERT
    //  targets and UPDATE returns empty {@code int[0][]}), but the encoding leaves room for
    //  arbitrarily deep paths — kept for forks/future FLIPs that surface them.
    // ------------------------------------------------------------------------

    /** Resolve target-column index paths against the sink's resolved schema. */
    public static List<String> targetColumnNames(
            ResolvedSchema schema, int[][] targetColumns) {
        if (schema == null) {
            return Collections.emptyList();
        }
        final List<Column> cols = schema.getColumns();
        if (targetColumns == null || targetColumns.length == 0) {
            return cols.stream().map(Column::getName).collect(Collectors.toList());
        }
        final List<String> out = new ArrayList<>(targetColumns.length);
        for (int[] path : targetColumns) {
            if (path == null || path.length == 0) {
                continue;
            }
            if (path[0] < 0 || path[0] >= cols.size()) {
                continue;
            }
            final Column top = cols.get(path[0]);
            out.add(buildColumnPath(top.getName(), top.getDataType(), path));
        }
        return out;
    }

    private static String buildColumnPath(String topName, DataType topType, int[] path) {
        final StringBuilder sb = new StringBuilder(topName);
        DataType cur = topType;
        for (int i = 1; i < path.length; i++) {
            if (cur != null && cur.getLogicalType() instanceof RowType) {
                final RowType rt = (RowType) cur.getLogicalType();
                final int idx = path[i];
                if (idx >= 0 && idx < rt.getFieldCount()) {
                    sb.append('.').append(rt.getFieldNames().get(idx));
                    cur = cur.getChildren().get(idx);
                    continue;
                }
            }
            sb.append(".#").append(path[i]);
            cur = null;
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------------
    //  Proxy installers (one per connector birth site)
    // ------------------------------------------------------------------------

    /** Creates a source via {@code create} and returns it wrapped in an audit proxy. */
    public static DynamicTableSource source(
            ContextResolvedTable ctx, Supplier<DynamicTableSource> create) {
        final DynamicTableSource raw;
        try {
            raw = create.get();
        } catch (Throwable t) {
            emit(false, ctx, t.getMessage());
            throw t;
        }
        return (DynamicTableSource) wrap(false, ctx, raw, null);
    }

    /** Creates a sink via {@code create} and returns it wrapped in an audit proxy. */
    public static DynamicTableSink sink(
            ContextResolvedTable ctx, Supplier<DynamicTableSink> create) {
        return sink(ctx, create, null);
    }

    /**
     * Same as {@link #sink(ContextResolvedTable, Supplier)} but also carries the SQL-intended
     * modified columns of the sink — the proxy attaches them to every C4 it emits, so failures from
     * later connector methods ({@code getSinkRuntimeProvider}, ability application, ...) carry the
     * same {@code modified_columns} as the runtime C3/C4.
     */
    public static DynamicTableSink sink(
            ContextResolvedTable ctx,
            Supplier<DynamicTableSink> create,
            List<String> modifiedColumns) {
        final DynamicTableSink raw;
        try {
            raw = create.get();
        } catch (Throwable t) {
            emit(true, ctx, t.getMessage(), modifiedColumns);
            throw t;
        }
        return (DynamicTableSink) wrap(true, ctx, raw, modifiedColumns);
    }

    private static Object wrap(
            boolean sink,
            ContextResolvedTable ctx,
            Object delegate,
            List<String> modifiedColumns) {
        if (delegate == null) {
            return null;
        }
        try {
            // Don't double-wrap (e.g. a spec reusing the rel's already-proxied connector).
            if (Proxy.isProxyClass(delegate.getClass())
                    && Proxy.getInvocationHandler(delegate) instanceof AuditHandler) {
                return delegate;
            }
            return Proxy.newProxyInstance(
                    delegate.getClass().getClassLoader(),
                    allInterfaces(delegate.getClass()),
                    new AuditHandler(sink, ctx, delegate, modifiedColumns));
        } catch (Throwable ignored) {
            // If proxying isn't possible, fall back to the raw connector — audit must never break
            // translation (worst case: a creation failure was still caught above; later method
            // failures on this instance simply go unaudited).
            return delegate;
        }
    }

    /** Forwards every call to the real connector; emits on failure; re-wraps {@code copy()}. */
    private static final class AuditHandler implements InvocationHandler {

        private final boolean sink;
        private final ContextResolvedTable ctx;
        private final Object delegate;
        private final List<String> modifiedColumns;

        AuditHandler(
                boolean sink,
                ContextResolvedTable ctx,
                Object delegate,
                List<String> modifiedColumns) {
            this.sink = sink;
            this.ctx = ctx;
            this.delegate = delegate;
            this.modifiedColumns = modifiedColumns;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            final String name = method.getName();
            if (args != null && args.length == 1 && "equals".equals(name)) {
                return proxy == args[0] || delegate.equals(unwrap(args[0]));
            }
            if (args == null && "hashCode".equals(name)) {
                return delegate.hashCode();
            }
            if (args == null && "toString".equals(name)) {
                return delegate.toString();
            }
            final Object result;
            try {
                result = method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                final Throwable cause = e.getCause() != null ? e.getCause() : e;
                emit(sink, ctx, cause.getMessage(), modifiedColumns);
                throw cause;
            }
            // Keep the audit on copies (pushdown / ability application produce copies).
            if (args == null
                    && "copy".equals(name)
                    && (result instanceof DynamicTableSource || result instanceof DynamicTableSink)) {
                return wrap(sink, ctx, result, modifiedColumns);
            }
            return result;
        }
    }

    private static Object unwrap(Object o) {
        if (o != null
                && Proxy.isProxyClass(o.getClass())
                && Proxy.getInvocationHandler(o) instanceof AuditHandler) {
            return ((AuditHandler) Proxy.getInvocationHandler(o)).delegate;
        }
        return o;
    }

    /** All interfaces implemented anywhere in the class hierarchy (so the proxy is a drop-in). */
    private static Class<?>[] allInterfaces(Class<?> type) {
        final Set<Class<?>> ifaces = new LinkedHashSet<>();
        final Deque<Class<?>> queue = new ArrayDeque<>();
        for (Class<?> c = type; c != null; c = c.getSuperclass()) {
            queue.addAll(Arrays.asList(c.getInterfaces()));
        }
        while (!queue.isEmpty()) {
            final Class<?> i = queue.poll();
            if (ifaces.add(i)) {
                queue.addAll(Arrays.asList(i.getInterfaces()));
            }
        }
        return ifaces.toArray(new Class<?>[0]);
    }
}
