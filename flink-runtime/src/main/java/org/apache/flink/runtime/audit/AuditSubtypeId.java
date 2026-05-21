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

/**
 * Audit event subtypes for connector data-plane lifecycle:
 *
 * <ul>
 *   <li>{@link #C1} — source read started (success)
 *   <li>{@link #C2} — source read failed
 *   <li>{@link #C3} — sink write started (success)
 *   <li>{@link #C4} — sink write failed
 * </ul>
 *
 * <p>Placeholder in this branch — in the fork this maps to the real audit taxonomy. {@code opCode}
 * stands in for the fork's {@code getOpCode()}.
 */
@Internal
public enum AuditSubtypeId {
    C1("c1"),
    C2("c2"),
    C3("c3"),
    C4("c4");

    private final String opCode;

    AuditSubtypeId(String opCode) {
        this.opCode = opCode;
    }

    public String getOpCode() {
        return opCode;
    }
}
