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

import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class helps parse, verify, and manage the Kubernetes side parameters that are used for
 * constructing the TaskManager Pod.
 */
public class KubernetesTaskManagerParameters extends AbstractKubernetesParameters {

    private final String podName;

    private final String dynamicProperties;

    private final String jvmMemOptsEnv;

    private final ContaineredTaskManagerParameters containeredTaskManagerParameters;

    private final Map<String, String> taskManagerExternalResourceConfigKeys;

    public KubernetesTaskManagerParameters(
            Configuration flinkConfig,
            String podName,
            String dynamicProperties,
            String jvmMemOptsEnv,
            ContaineredTaskManagerParameters containeredTaskManagerParameters,
            Map<String, String> taskManagerExternalResourceConfigKeys) {
        super(flinkConfig);
        this.podName = checkNotNull(podName);
        this.dynamicProperties = checkNotNull(dynamicProperties);
        this.jvmMemOptsEnv = checkNotNull(jvmMemOptsEnv);
        this.containeredTaskManagerParameters = checkNotNull(containeredTaskManagerParameters);
        this.taskManagerExternalResourceConfigKeys =
                checkNotNull(taskManagerExternalResourceConfigKeys);
    }

    @Override
    public Map<String, String> getLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.putAll(
                flinkConfig
                        .getOptional(KubernetesConfigOptions.TASK_MANAGER_LABELS)
                        .orElse(Collections.emptyMap()));
        labels.putAll(KubernetesUtils.getTaskManagerLabels(getClusterId()));
        return Collections.unmodifiableMap(labels);
    }

    @Override
    public Map<String, String> getNodeSelector() {
        return Collections.unmodifiableMap(
                flinkConfig
                        .getOptional(KubernetesConfigOptions.TASK_MANAGER_NODE_SELECTOR)
                        .orElse(Collections.emptyMap()));
    }

    @Override
    public Map<String, String> getEnvironments() {
        return this.containeredTaskManagerParameters.taskManagerEnv();
    }

    @Override
    public Map<String, String> getAnnotations() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS)
                .orElse(Collections.emptyMap());
    }

    @Override
    public List<Map<String, String>> getTolerations() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.TASK_MANAGER_TOLERATIONS)
                .orElse(Collections.emptyList());
    }

    public String getPodName() {
        return podName;
    }

    public int getTaskManagerMemoryMB() {
        return containeredTaskManagerParameters
                .getTaskExecutorProcessSpec()
                .getTotalProcessMemorySize()
                .getMebiBytes();
    }

    public double getTaskManagerCPU() {
        return containeredTaskManagerParameters
                .getTaskExecutorProcessSpec()
                .getCpuCores()
                .getValue()
                .doubleValue();
    }

    public Map<String, ExternalResource> getTaskManagerExternalResources() {
        return containeredTaskManagerParameters.getTaskExecutorProcessSpec().getExtendedResources();
    }

    public String getServiceAccount() {
        return flinkConfig.get(KubernetesConfigOptions.TASK_MANAGER_SERVICE_ACCOUNT);
    }

    public Map<String, String> getTaskManagerExternalResourceConfigKeys() {
        return Collections.unmodifiableMap(taskManagerExternalResourceConfigKeys);
    }

    public int getRPCPort() {
        final int taskManagerRpcPort =
                KubernetesUtils.parsePort(flinkConfig, TaskManagerOptions.RPC_PORT);
        checkArgument(
                taskManagerRpcPort > 0, "%s should not be 0.", TaskManagerOptions.RPC_PORT.key());
        return taskManagerRpcPort;
    }

    public String getDynamicProperties() {
        return dynamicProperties;
    }

    public String getJvmMemOptsEnv() {
        return jvmMemOptsEnv;
    }

    public ContaineredTaskManagerParameters getContaineredTaskManagerParameters() {
        return containeredTaskManagerParameters;
    }
}
