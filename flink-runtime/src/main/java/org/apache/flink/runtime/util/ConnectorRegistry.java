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

package org.apache.flink.runtime.util;

import org.apache.flink.api.common.JobID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ConnectorRegistry {
    private final ConcurrentMap<JobID, List<String>> sinks = new ConcurrentHashMap<>();
    private final ConcurrentMap<JobID, List<String>> sources = new ConcurrentHashMap<>();

    private ConnectorRegistry() {};

    private static <T> void registerConnector(
            JobID jobId, T connector, ConcurrentMap<JobID, List<T>> connectors) {
        connectors.compute(
                jobId,
                (key, oldValue) -> {
                    if (oldValue == null) {
                        List<T> newList = new ArrayList<>();
                        newList.add(connector);
                        return newList;
                    } else {
                        oldValue.add(connector);
                        return oldValue;
                    }
                });
    }

    private static <T> void registerConnectors(
            JobID jobId, List<T> connectorList, ConcurrentMap<JobID, List<T>> connectors) {
        connectors.compute(
                jobId,
                (key, oldValue) -> {
                    if (oldValue == null) {
                        return new ArrayList<T>(connectorList);
                    } else {
                        oldValue.addAll(connectorList);
                        return oldValue;
                    }
                });
    }

    public void registerSink(JobID jobId, String sink) {
        registerConnector(jobId, sink, this.sinks);
    }

    public void registerSinks(JobID jobId, List<String> sink) {
        registerConnectors(jobId, sink, this.sinks);
    }

    public void registerSource(JobID jobId, String source) {
        registerConnector(jobId, source, this.sources);
    }

    public void registerSources(JobID jobId, List<String> source) {
        registerConnectors(jobId, source, this.sources);
    }

    public List<String> getAllSinks(JobID jobId) {
        return sinks.getOrDefault(jobId, Collections.emptyList());
    }

    public List<String> getAllSources(JobID jobId) {
        return sources.getOrDefault(jobId, Collections.emptyList());
    }

    public static ConnectorRegistry getInstance() {
        return ConnectorRegistryHolder.INSTANCE;
    }

    private static class ConnectorRegistryHolder {
        private static final ConnectorRegistry INSTANCE = new ConnectorRegistry();
    }
}
