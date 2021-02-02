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

package org.apache.flink.kubernetes.kubeclient.parameters;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class helps parse, verify, and manage the Kubernetes side parameters that are used for
 * constructing the JobManager Pod and all accompanying resources connected to it.
 */
public class KubernetesJobManagerParameters extends AbstractKubernetesParameters {

    public static final String JOB_MANAGER_MAIN_CONTAINER_NAME = "flink-job-manager";

    private final ClusterSpecification clusterSpecification;

    public KubernetesJobManagerParameters(
            Configuration flinkConfig, ClusterSpecification clusterSpecification) {
        super(flinkConfig);
        this.clusterSpecification = checkNotNull(clusterSpecification);
    }

    @Override
    public Map<String, String> getLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.putAll(
                flinkConfig
                        .getOptional(KubernetesConfigOptions.JOB_MANAGER_LABELS)
                        .orElse(Collections.emptyMap()));
        labels.putAll(getCommonLabels());
        labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
        return Collections.unmodifiableMap(labels);
    }

    @Override
    public Map<String, String> getNodeSelector() {
        return Collections.unmodifiableMap(
                flinkConfig
                        .getOptional(KubernetesConfigOptions.JOB_MANAGER_NODE_SELECTOR)
                        .orElse(Collections.emptyMap()));
    }

    @Override
    public Map<String, String> getEnvironments() {
        return ConfigurationUtils.getPrefixedKeyValuePairs(
                ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX, flinkConfig);
    }

    @Override
    public Map<String, String> getAnnotations() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS)
                .orElse(Collections.emptyMap());
    }

    @Override
    public List<Map<String, String>> getTolerations() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.JOB_MANAGER_TOLERATIONS)
                .orElse(Collections.emptyList());
    }

    public Map<String, String> getRestServiceAnnotations() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.REST_SERVICE_ANNOTATIONS)
                .orElse(Collections.emptyMap());
    }

    public String getJobManagerMainContainerName() {
        return JOB_MANAGER_MAIN_CONTAINER_NAME;
    }

    public int getJobManagerMemoryMB() {
        return clusterSpecification.getMasterMemoryMB();
    }

    public double getJobManagerCPU() {
        return flinkConfig.getDouble(KubernetesConfigOptions.JOB_MANAGER_CPU);
    }

    public int getRestPort() {
        return flinkConfig.getInteger(RestOptions.PORT);
    }

    public int getRestBindPort() {
        return Integer.valueOf(flinkConfig.getString(RestOptions.BIND_PORT));
    }

    public int getRPCPort() {
        return flinkConfig.getInteger(JobManagerOptions.PORT);
    }

    public int getBlobServerPort() {
        final int blobServerPort = KubernetesUtils.parsePort(flinkConfig, BlobServerOptions.PORT);
        checkArgument(blobServerPort > 0, "%s should not be 0.", BlobServerOptions.PORT.key());
        return blobServerPort;
    }

    public String getServiceAccount() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT)
                .orElse(flinkConfig.getString(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT));
    }

    public String getEntrypointClass() {
        final String entrypointClass =
                flinkConfig.getString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS);
        checkNotNull(
                entrypointClass,
                KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS + " must be specified!");

        return entrypointClass;
    }

    public KubernetesConfigOptions.ServiceExposedType getRestServiceExposedType() {
        return flinkConfig.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);
    }

    public boolean isInternalServiceEnabled() {
        return !HighAvailabilityMode.isHighAvailabilityModeActivated(flinkConfig);
    }
}
