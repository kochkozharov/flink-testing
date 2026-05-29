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
import org.apache.flink.api.common.JobID;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Single entry point that turns a connector lifecycle transition into an {@link AuditEvent} and
 * ships it via {@link AuditLogger}. Used identically from every path — eager (plan translation) and
 * runtime, source and sink, V1 and V2 — so all four events (C1–C4) come out in one shape.
 *
 * <p>Inputs are just the JobID and the connector's DDL {@code WITH (...)} options: {@code
 * objectName} is built from {@code connector} + the real table/topic name, {@code objectProperties}
 * is the full option list. Identity comes from {@link Sessions} (constant in this branch).
 *
 * <p>Every method is fully guarded: any failure (no session, null inputs, ...) is swallowed, so
 * audit never breaks the job. In session mode where nothing is wired, this simply produces no log.
 */
@Internal
public final class LifecycleAudit {

    private static final String SUCCESS = "SUCCESS";
    private static final String FAIL = "FAIL";
    private static final String UNDEFINED = "UNDEFINED";

    private static final DateTimeFormatter DATE =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final ZoneId TZ = ZoneId.of("Europe/Moscow");

    private LifecycleAudit() {}

    /** C1 — source started reading. */
    public static void readStarted(JobID jobId, Map<String, String> withOptions) {
        emit(AuditSubtypeId.C1, SUCCESS, jobId, withOptions, null, null);
    }

    /** C2 — source failed reading. */
    public static void readFailed(JobID jobId, Map<String, String> withOptions, String reason) {
        emit(AuditSubtypeId.C2, FAIL, jobId, withOptions, reason, null);
    }

    /** C3 — sink started writing. */
    public static void writeStarted(JobID jobId, Map<String, String> withOptions) {
        writeStarted(jobId, withOptions, null);
    }

    /**
     * C3 with the SQL-intended modified columns of the sink (target columns of INSERT/UPDATE; all
     * columns for {@code INSERT INTO sink SELECT ...}). Pass {@code null}/empty if unknown.
     */
    public static void writeStarted(
            JobID jobId, Map<String, String> withOptions, List<String> modifiedColumns) {
        emit(AuditSubtypeId.C3, SUCCESS, jobId, withOptions, null, modifiedColumns);
    }

    /** C4 — sink failed writing. */
    public static void writeFailed(JobID jobId, Map<String, String> withOptions, String reason) {
        writeFailed(jobId, withOptions, reason, null);
    }

    /** C4 with the SQL-intended modified columns of the sink. */
    public static void writeFailed(
            JobID jobId,
            Map<String, String> withOptions,
            String reason,
            List<String> modifiedColumns) {
        emit(AuditSubtypeId.C4, FAIL, jobId, withOptions, reason, modifiedColumns);
    }

    private static void emit(
            AuditSubtypeId subtype,
            String status,
            JobID jobId,
            Map<String, String> opts,
            String reason,
            List<String> modifiedColumns) {
        try {
            final SessionInfo s = Sessions.forJob(jobId);
            final String connector = opts == null ? null : opts.get("connector");
            final String table = tableName(opts);
            final String objectName =
                    (connector == null ? UNDEFINED : connector)
                            + (table == null ? "" : ":" + table);

            final List<String> props = new ArrayList<>();
            if (opts != null) {
                for (Map.Entry<String, String> e : opts.entrySet()) {
                    props.add(e.getKey() + "=" + e.getValue());
                }
            }
            // SQL-intended modified columns of the sink (target columns of the INSERT/UPDATE).
            // For C3/C4 it carries what the sink WAS ASKED to write; not what the connector
            // actually persisted. Null/empty for source events and when unknown.
            if (modifiedColumns != null && !modifiedColumns.isEmpty()) {
                props.add("modified_columns=[" + String.join(",", modifiedColumns) + "]");
            }

            final AuditEvent event =
                    AuditEvent.builder()
                            .subtypeId(subtype.name())
                            .bEvent(subtype.getOpCode())
                            .status(status)
                            .processName(jobId == null ? UNDEFINED : jobId.toString())
                            .objectName(objectName)
                            .objectId(UNDEFINED)
                            .objectProperties(props)
                            .sessionId(s.getSessionId())
                            .userLogin(s.getUserLogin())
                            .ipAddress(s.getIpAddress())
                            .fqdnAddress(s.getFqdnAddress())
                            .reason(reason)
                            .operationDate(
                                    ZonedDateTime.ofInstant(Instant.now(), TZ).format(DATE))
                            .build();

            AuditLogger.log(event);
        } catch (Throwable ignored) {
            // Audit is best-effort: never let it break the job (and stay quiet in session mode).
        }
    }

    /** Best-effort real table/topic name from common WITH-option keys. */
    private static String tableName(Map<String, String> opts) {
        if (opts == null) {
            return null;
        }
        for (String key : new String[] {"table-name", "topic", "path", "catalog-table"}) {
            final String v = opts.get(key);
            if (v != null && !v.isEmpty()) {
                return v;
            }
        }
        return null;
    }
}
