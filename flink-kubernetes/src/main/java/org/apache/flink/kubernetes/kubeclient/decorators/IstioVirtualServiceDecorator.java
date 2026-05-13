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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.KUBERNETES_ISTIO_VIRTUAL_SERVICE_CLUSTER_HOST;
import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.KUBERNETES_ISTIO_VIRTUAL_SERVICE_ENABLED;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Creates an Istio VirtualService routing traffic from
 * 'flink-ui-&lt;namespace&gt;.&lt;cluster-host&gt;' to the JobManager REST service. The VirtualService
 * is built as a {@link GenericKubernetesResource} so no Istio client dependency is required.
 */
public class IstioVirtualServiceDecorator extends AbstractKubernetesStepDecorator {

    private static final String API_VERSION = "networking.istio.io/v1beta1";
    private static final String KIND = "VirtualService";
    private static final String HOST_PREFIX = "flink-ui-";
    private static final List<String> GATEWAYS = Arrays.asList("flink-ui-gw", "mesh");

    private final KubernetesJobManagerParameters kubernetesJobManagerParameters;

    public IstioVirtualServiceDecorator(
            KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() {
        final Configuration configuration =
                kubernetesJobManagerParameters.getFlinkConfiguration();
        if (!configuration.get(KUBERNETES_ISTIO_VIRTUAL_SERVICE_ENABLED)) {
            return Collections.emptyList();
        }

        final String clusterHost =
                configuration.get(KUBERNETES_ISTIO_VIRTUAL_SERVICE_CLUSTER_HOST);

        final String clusterId = kubernetesJobManagerParameters.getClusterId();
        final String namespace = kubernetesJobManagerParameters.getNamespace();
        final int restPort = kubernetesJobManagerParameters.getRestPort();

        final String host = HOST_PREFIX + namespace + "." + clusterHost;
        final String pathPrefix = "/" + clusterId + "/";
        final String destinationHost = clusterId + Constants.FLINK_REST_SERVICE_SUFFIX;

        final Map<String, Object> spec =
                buildSpec(host, pathPrefix, destinationHost, restPort);

        final GenericKubernetesResource virtualService =
                new GenericKubernetesResourceBuilder()
                        .withApiVersion(API_VERSION)
                        .withKind(KIND)
                        .withNewMetadata()
                        .withName(getVirtualServiceName(clusterId))
                        .withNamespace(namespace)
                        .withLabels(kubernetesJobManagerParameters.getCommonLabels())
                        .endMetadata()
                        .build();
        virtualService.setAdditionalProperty("spec", spec);

        return Collections.singletonList(virtualService);
    }

    public static String getVirtualServiceName(String clusterId) {
        return clusterId + Constants.FLINK_REST_SERVICE_SUFFIX;
    }

    private static Map<String, Object> buildSpec(
            String host, String pathPrefix, String destinationHost, int destinationPort) {
        final Map<String, Object> uri = new LinkedHashMap<>();
        uri.put("prefix", pathPrefix);

        final Map<String, Object> match = new LinkedHashMap<>();
        match.put("uri", uri);

        final Map<String, Object> rewrite = new LinkedHashMap<>();
        rewrite.put("uri", "/");

        final Map<String, Object> port = new LinkedHashMap<>();
        port.put("number", destinationPort);

        final Map<String, Object> destination = new LinkedHashMap<>();
        destination.put("host", destinationHost);
        destination.put("port", port);

        final Map<String, Object> route = new LinkedHashMap<>();
        route.put("destination", destination);

        final Map<String, Object> http = new LinkedHashMap<>();
        http.put("match", Collections.singletonList(match));
        http.put("rewrite", rewrite);
        http.put("route", Collections.singletonList(route));

        final Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("exportTo", Collections.singletonList("."));
        spec.put("gateways", GATEWAYS);
        spec.put("hosts", Collections.singletonList(host));
        spec.put("http", Collections.singletonList(http));
        return spec;
    }
}