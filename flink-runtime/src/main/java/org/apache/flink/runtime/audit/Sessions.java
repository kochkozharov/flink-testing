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

/**
 * Placeholder for the fork's global session store keyed by {@link JobID}. The fork resolves the
 * submitting session here; in this branch {@link #forJob(JobID)} returns a constant stand-in (the
 * application-mode identity), so call sites read identical. Never returns {@code null} and never
 * throws — keeps audit emission session-safe.
 */
@Internal
public final class Sessions {

    /** Constant stand-in used wherever the real session would be looked up. */
    public static final SessionInfo PLACEHOLDER =
            new SessionInfo("UNDEFINED", "UNDEFINED", "127.0.0.1", "localhost");

    private Sessions() {}

    public static SessionInfo forJob(JobID jobId) {
        // Fork: look up the real session by jobId. Here: constant application-mode identity.
        return PLACEHOLDER;
    }
}
