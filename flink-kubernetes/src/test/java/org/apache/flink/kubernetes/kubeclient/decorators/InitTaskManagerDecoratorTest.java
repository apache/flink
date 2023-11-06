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

import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Toleration;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** General tests for the {@link InitJobManagerDecorator}. */
class InitTaskManagerDecoratorTest extends KubernetesTaskManagerTestBase {

    private static final String SERVICE_ACCOUNT_NAME = "service-test";
    private static final List<String> IMAGE_PULL_SECRETS = Arrays.asList("s1", "s2", "s3");
    private static final Map<String, String> ANNOTATIONS =
            new HashMap<String, String>() {
                {
                    put("a1", "v1");
                    put("a2", "v2");
                }
            };
    private static final String TOLERATION_STRING =
            "key:key1,operator:Equal,value:value1,effect:NoSchedule;"
                    + "KEY:key2,operator:Exists,Effect:NoExecute,tolerationSeconds:6000";
    private static final List<Toleration> TOLERATION =
            Arrays.asList(
                    new Toleration("NoSchedule", "key1", "Equal", null, "value1"),
                    new Toleration("NoExecute", "key2", "Exists", 6000L, null));

    private static final String RESOURCE_NAME = "test";
    private static final Long RESOURCE_AMOUNT = 2L;
    private static final String RESOURCE_CONFIG_KEY = "test.com/test";

    private static final String USER_DEFINED_FLINK_LOG_DIR = "/path/of/flink-log";

    private Pod resultPod;
    private Container resultMainContainer;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        this.flinkConfig.set(
                KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT, SERVICE_ACCOUNT_NAME);
        this.flinkConfig.set(
                KubernetesConfigOptions.CONTAINER_IMAGE_PULL_SECRETS, IMAGE_PULL_SECRETS);
        this.flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS, ANNOTATIONS);
        this.flinkConfig.setString(
                KubernetesConfigOptions.TASK_MANAGER_TOLERATIONS.key(), TOLERATION_STRING);

        // Set up external resource configs
        flinkConfig.setString(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST.key(), RESOURCE_NAME);
        flinkConfig.setLong(
                ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(
                        RESOURCE_NAME, ExternalResourceOptions.EXTERNAL_RESOURCE_AMOUNT_SUFFIX),
                RESOURCE_AMOUNT);
        flinkConfig.setString(
                ExternalResourceOptions.getSystemConfigKeyConfigOptionForResource(
                        RESOURCE_NAME,
                        KubernetesConfigOptions.EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX),
                RESOURCE_CONFIG_KEY);
        this.flinkConfig.set(KubernetesConfigOptions.FLINK_LOG_DIR, USER_DEFINED_FLINK_LOG_DIR);
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        final InitTaskManagerDecorator initTaskManagerDecorator =
                new InitTaskManagerDecorator(kubernetesTaskManagerParameters);

        final FlinkPod resultFlinkPod =
                initTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod);
        this.resultPod = resultFlinkPod.getPodWithoutMainContainer();
        this.resultMainContainer = resultFlinkPod.getMainContainer();
    }

    @Test
    void testApiVersion() {
        assertThat(this.resultPod.getApiVersion()).isEqualTo(Constants.API_VERSION);
    }

    @Test
    void testMainContainerName() {
        assertThat(this.resultMainContainer.getName()).isEqualTo(Constants.MAIN_CONTAINER_NAME);
    }

    @Test
    void testMainContainerImage() {
        assertThat(this.resultMainContainer.getImage()).isEqualTo(CONTAINER_IMAGE);
    }

    @Test
    void testMainContainerImagePullPolicy() {
        assertThat(this.resultMainContainer.getImagePullPolicy())
                .isEqualTo(CONTAINER_IMAGE_PULL_POLICY.name());
    }

    @Test
    void testMainContainerResourceRequirements() {
        final ResourceRequirements resourceRequirements = this.resultMainContainer.getResources();

        final Map<String, Quantity> requests = resourceRequirements.getRequests();
        assertThat(requests.get("cpu").getAmount()).isEqualTo(Double.toString(TASK_MANAGER_CPU));
        assertThat(requests.get("memory").getAmount())
                .isEqualTo(String.valueOf(TOTAL_PROCESS_MEMORY));

        final Map<String, Quantity> limits = resourceRequirements.getLimits();
        assertThat(limits.get("cpu").getAmount())
                .isEqualTo(Double.toString(TASK_MANAGER_CPU * TASK_MANAGER_CPU_LIMIT_FACTOR));
        assertThat(limits.get("memory").getAmount())
                .isEqualTo(
                        Integer.toString(
                                (int) (TOTAL_PROCESS_MEMORY * TASK_MANAGER_MEMORY_LIMIT_FACTOR)));
    }

    @Test
    void testExternalResourceInResourceRequirements() {
        final ResourceRequirements resourceRequirements = this.resultMainContainer.getResources();

        final Map<String, Quantity> requests = resourceRequirements.getRequests();
        assertThat(requests.get(RESOURCE_CONFIG_KEY).getAmount())
                .isEqualTo(Long.toString(RESOURCE_AMOUNT));

        final Map<String, Quantity> limits = resourceRequirements.getLimits();
        assertThat(limits.get(RESOURCE_CONFIG_KEY).getAmount())
                .isEqualTo(Long.toString(RESOURCE_AMOUNT));
    }

    @Test
    void testMainContainerPorts() {
        final List<ContainerPort> expectedContainerPorts =
                Collections.singletonList(
                        new ContainerPortBuilder()
                                .withName(Constants.TASK_MANAGER_RPC_PORT_NAME)
                                .withContainerPort(RPC_PORT)
                                .build());

        assertThat(this.resultMainContainer.getPorts()).isEqualTo(expectedContainerPorts);
    }

    @Test
    void testMainContainerEnv() {
        final Map<String, String> expectedEnvVars = new HashMap<>(customizedEnvs);
        final Map<String, String> resultEnvVars = new HashMap<>();
        this.resultMainContainer
                .getEnv()
                .forEach(envVar -> resultEnvVars.put(envVar.getName(), envVar.getValue()));
        expectedEnvVars.forEach((k, v) -> assertThat(resultEnvVars.get(k)).isEqualTo(v));
    }

    @Test
    void testNodeIdEnv() {
        assertThat(this.resultMainContainer.getEnv())
                .anyMatch(
                        envVar ->
                                envVar.getName().equals(Constants.ENV_FLINK_POD_NODE_ID)
                                        && envVar.getValueFrom()
                                                .getFieldRef()
                                                .getApiVersion()
                                                .equals(Constants.API_VERSION)
                                        && envVar.getValueFrom()
                                                .getFieldRef()
                                                .getFieldPath()
                                                .equals(Constants.POD_NODE_ID_FIELD_PATH));
    }

    @Test
    void testPodName() {
        assertThat(this.resultPod.getMetadata().getName()).isEqualTo(POD_NAME);
    }

    @Test
    void testPodLabels() {
        final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
        expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
        expectedLabels.putAll(userLabels);

        assertThat(this.resultPod.getMetadata().getLabels()).isEqualTo(expectedLabels);
    }

    @Test
    void testPodAnnotations() {
        final Map<String, String> resultAnnotations =
                kubernetesTaskManagerParameters.getAnnotations();
        assertThat(resultAnnotations).isEqualTo(ANNOTATIONS);
    }

    @Test
    void testPodServiceAccountName() {
        assertThat(this.resultPod.getSpec().getServiceAccountName())
                .isEqualTo(SERVICE_ACCOUNT_NAME);
    }

    @Test
    void testRestartPolicy() {
        final String resultRestartPolicy = this.resultPod.getSpec().getRestartPolicy();

        assertThat(resultRestartPolicy).isEqualTo(Constants.RESTART_POLICY_OF_NEVER);
    }

    @Test
    void testImagePullSecrets() {
        final List<String> resultSecrets =
                this.resultPod.getSpec().getImagePullSecrets().stream()
                        .map(LocalObjectReference::getName)
                        .collect(Collectors.toList());

        assertThat(resultSecrets).isEqualTo(IMAGE_PULL_SECRETS);
    }

    @Test
    void testNodeSelector() {
        assertThat(this.resultPod.getSpec().getNodeSelector()).isEqualTo(nodeSelector);
    }

    @Test
    void testPodTolerations() {
        assertThat(this.resultPod.getSpec().getTolerations())
                .containsExactlyInAnyOrderElementsOf(TOLERATION);
    }

    @Test
    void testFlinkLogDirEnvShouldBeSetIfConfiguredViaOptions() {
        final List<EnvVar> envVars = this.resultMainContainer.getEnv();
        assertThat(envVars)
                .anyMatch(
                        envVar ->
                                envVar.getName().equals(Constants.ENV_FLINK_LOG_DIR)
                                        && envVar.getValue().equals(USER_DEFINED_FLINK_LOG_DIR));
    }

    @Test
    void testNodeAffinity() {
        List<NodeSelectorTerm> nodeSelectorTerms =
                this.resultPod
                        .getSpec()
                        .getAffinity()
                        .getNodeAffinity()
                        .getRequiredDuringSchedulingIgnoredDuringExecution()
                        .getNodeSelectorTerms();
        assertThat(nodeSelectorTerms.size()).isEqualTo(1);

        List<NodeSelectorRequirement> requirements = nodeSelectorTerms.get(0).getMatchExpressions();
        for (int i = 0; i < requirements.size(); i++) {
            Collections.sort(requirements.get(i).getValues());
            requirements.get(i).setValues(requirements.get(i).getValues());
        }

        List<NodeSelectorRequirement> expectedRequirements =
                Collections.singletonList(
                        new NodeSelectorRequirement(
                                flinkConfig.getString(
                                        KubernetesConfigOptions.KUBERNETES_NODE_NAME_LABEL),
                                "NotIn",
                                new ArrayList<>(BLOCKED_NODES)));

        for (int i = 0; i < expectedRequirements.size(); i++) {
            Collections.sort(expectedRequirements.get(i).getValues());
        }
        assertEquals(requirements, expectedRequirements);
    }
}
