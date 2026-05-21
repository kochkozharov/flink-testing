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

import java.util.List;

/**
 * Audit event mirroring the fork's lombok-based {@code org.apache.flink.model.AuditEvent}. Same
 * field set and same {@code builder()} call-site shape, but with a hand-written builder so it
 * compiles in vanilla Flink. Swap for the fork's class on port.
 */
@Internal
public final class AuditEvent {

    private final String subtypeId;
    private final String bEvent;
    private final String ipAddress;
    private final String fqdnAddress;
    private final String userLogin;
    private final String status;
    private final String operationDate;
    private final String reason;
    private final String sessionId;
    private final String processName;
    private final String description;
    private final String objectName;
    private final String objectId;
    private final List<String> objectProperties;

    private AuditEvent(Builder b) {
        this.subtypeId = b.subtypeId;
        this.bEvent = b.bEvent;
        this.ipAddress = b.ipAddress;
        this.fqdnAddress = b.fqdnAddress;
        this.userLogin = b.userLogin;
        this.status = b.status;
        this.operationDate = b.operationDate;
        this.reason = b.reason;
        this.sessionId = b.sessionId;
        this.processName = b.processName;
        this.description = b.description;
        this.objectName = b.objectName;
        this.objectId = b.objectId;
        this.objectProperties = b.objectProperties;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getSubtypeId() {
        return subtypeId;
    }

    public String getStatus() {
        return status;
    }

    public String getObjectName() {
        return objectName;
    }

    public List<String> getObjectProperties() {
        return objectProperties;
    }

    @Override
    public String toString() {
        return "AuditEvent{subtypeId="
                + subtypeId
                + ", bEvent="
                + bEvent
                + ", status="
                + status
                + ", objectName="
                + objectName
                + ", objectId="
                + objectId
                + ", objectProperties="
                + objectProperties
                + ", reason="
                + reason
                + ", processName="
                + processName
                + ", sessionId="
                + sessionId
                + ", userLogin="
                + userLogin
                + ", ipAddress="
                + ipAddress
                + ", fqdnAddress="
                + fqdnAddress
                + ", operationDate="
                + operationDate
                + ", description="
                + description
                + "}";
    }

    /** Hand-written stand-in for lombok {@code @Builder}. */
    public static final class Builder {
        private String subtypeId;
        private String bEvent;
        private String ipAddress;
        private String fqdnAddress;
        private String userLogin;
        private String status;
        private String operationDate;
        private String reason;
        private String sessionId;
        private String processName;
        private String description;
        private String objectName;
        private String objectId;
        private List<String> objectProperties;

        public Builder subtypeId(String v) {
            this.subtypeId = v;
            return this;
        }

        public Builder bEvent(String v) {
            this.bEvent = v;
            return this;
        }

        public Builder ipAddress(String v) {
            this.ipAddress = v;
            return this;
        }

        public Builder fqdnAddress(String v) {
            this.fqdnAddress = v;
            return this;
        }

        public Builder userLogin(String v) {
            this.userLogin = v;
            return this;
        }

        public Builder status(String v) {
            this.status = v;
            return this;
        }

        public Builder operationDate(String v) {
            this.operationDate = v;
            return this;
        }

        public Builder reason(String v) {
            this.reason = v;
            return this;
        }

        public Builder sessionId(String v) {
            this.sessionId = v;
            return this;
        }

        public Builder processName(String v) {
            this.processName = v;
            return this;
        }

        public Builder description(String v) {
            this.description = v;
            return this;
        }

        public Builder objectName(String v) {
            this.objectName = v;
            return this;
        }

        public Builder objectId(String v) {
            this.objectId = v;
            return this;
        }

        public Builder objectProperties(List<String> v) {
            this.objectProperties = v;
            return this;
        }

        public AuditEvent build() {
            return new AuditEvent(this);
        }
    }
}
