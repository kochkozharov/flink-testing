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
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.SinkModifyOperation;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Coordinates eager (plan-translation) audit emission so that every connector lifecycle failure
 * during {@code planner.translate(...)} produces exactly one C2/C4 — with the right side and the
 * right table identity — and never more.
 *
 * <p>Two tiers cooperate through one {@link #emitOnce} entry point and a per-translation dedup set:
 *
 * <ul>
 *   <li><b>Precise hooks</b> sit at the connector boundaries where identity is still known (sink
 *       factory creation + {@code convertSinkToRel}; source {@code toRel}; {@code
 *       getSinkRuntimeProvider}/{@code getScanRuntimeProvider}). Each calls {@link #emitOnce}.
 *   <li><b>The safety net</b> ({@link #netFallback}) runs from the single catch in {@code
 *       PlannerBase.translate}. It fires <em>only</em> when no precise hook emitted for this
 *       translation, covering the residual phases (optimize, exec-graph) with a best-effort C4.
 * </ul>
 *
 * <p>The dedup set is a {@link ThreadLocal} scoped to one {@code translate(...)} call via {@link
 * #begin()}/{@link #clear()}. Everything is best-effort and swallowed: audit must never break
 * translation, and in session mode (nothing wired) it simply produces no log.
 */
@Internal
public final class EagerAudit {

    private static final ThreadLocal<Set<String>> EMITTED = new ThreadLocal<>();

    private EagerAudit() {}

    /** Opens a dedup window for one {@code translate(...)} call. */
    public static void begin() {
        EMITTED.set(new HashSet<>());
    }

    /** Closes the dedup window. Always call from a {@code finally}. */
    public static void clear() {
        EMITTED.remove();
    }

    /**
     * Emits C4 (sink) or C2 (source) for the given table at most once per translation. Outside a
     * dedup window (e.g. a source {@code toRel} failure during SQL parsing) it simply emits.
     */
    public static void emitOnce(boolean sink, ContextResolvedTable ctx, String reason) {
        try {
            if (ctx == null) {
                return;
            }
            final String key = (sink ? "sink:" : "source:") + ctx.getIdentifier().asSummaryString();
            final Set<String> set = EMITTED.get();
            if (set != null && !set.add(key)) {
                return; // a precise hook already logged this connector in this translation
            }
            final Map<String, String> options = ctx.getResolvedTable().getOptions();
            if (sink) {
                LifecycleAudit.writeFailed(null, options, reason);
            } else {
                LifecycleAudit.readFailed(null, options, reason);
            }
        } catch (Throwable ignored) {
            // Best-effort: never let audit break plan translation.
        }
    }

    /**
     * Last-resort emit from the catch in {@code PlannerBase.translate}. Stays silent if a precise
     * hook already emitted (the common path), so it never reintroduces over-emit. Otherwise it
     * attributes the failure to the always-identifiable sink(s) of the failed statement — covering
     * optimize/exec-graph phases that have no clean per-connector boundary.
     */
    public static void netFallback(List<ModifyOperation> modifyOperations, Throwable t) {
        try {
            final Set<String> set = EMITTED.get();
            if (set == null || !set.isEmpty()) {
                return;
            }
            final String reason = t == null ? null : t.getMessage();
            for (ModifyOperation op : modifyOperations) {
                if (op instanceof SinkModifyOperation) {
                    emitOnce(true, ((SinkModifyOperation) op).getContextResolvedTable(), reason);
                }
            }
        } catch (Throwable ignored) {
            // Best-effort.
        }
    }
}
