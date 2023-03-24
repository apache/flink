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

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Toleration;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** General tests for the {@link InitJobManagerDecorator}. */
class InitJobManagerDecoratorTest extends KubernetesJobManagerTestBase {

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
        this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS, ANNOTATIONS);
        this.flinkConfig.setString(
                KubernetesConfigOptions.JOB_MANAGER_TOLERATIONS.key(), TOLERATION_STRING);
        this.flinkConfig.set(KubernetesConfigOptions.FLINK_LOG_DIR, USER_DEFINED_FLINK_LOG_DIR);
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        final InitJobManagerDecorator initJobManagerDecorator =
                new InitJobManagerDecorator(this.kubernetesJobManagerParameters);
        final FlinkPod resultFlinkPod = initJobManagerDecorator.decorateFlinkPod(this.baseFlinkPod);

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
        assertThat(this.resultMainContainer.getImagePullPolicy())
                .isEqualTo(CONTAINER_IMAGE_PULL_POLICY.name());
    }

    @Test
    void testMainContainerResourceRequirements() {
        final ResourceRequirements resourceRequirements = this.resultMainContainer.getResources();

        final Map<String, Quantity> requests = resourceRequirements.getRequests();
        assertThat(requests.get("cpu").getAmount()).isEqualTo(Double.toString(JOB_MANAGER_CPU));
        assertThat(requests.get("memory").getAmount())
                .isEqualTo(String.valueOf(JOB_MANAGER_MEMORY));

        final Map<String, Quantity> limits = resourceRequirements.getLimits();
        assertThat(limits.get("cpu").getAmount())
                .isEqualTo(Double.toString(JOB_MANAGER_CPU * JOB_MANAGER_CPU_LIMIT_FACTOR));
        assertThat(limits.get("memory").getAmount())
                .isEqualTo(
                        Integer.toString(
                                (int) (JOB_MANAGER_MEMORY * JOB_MANAGER_MEMORY_LIMIT_FACTOR)));
    }

    @Test
    void testMainContainerPorts() {
        final List<ContainerPort> expectedContainerPorts =
                Arrays.asList(
                        new ContainerPortBuilder()
                                .withName(Constants.REST_PORT_NAME)
                                .withContainerPort(REST_PORT)
                                .build(),
                        new ContainerPortBuilder()
                                .withName(Constants.JOB_MANAGER_RPC_PORT_NAME)
                                .withContainerPort(RPC_PORT)
                                .build(),
                        new ContainerPortBuilder()
                                .withName(Constants.BLOB_SERVER_PORT_NAME)
                                .withContainerPort(BLOB_SERVER_PORT)
                                .build());

        assertThat(this.resultMainContainer.getPorts()).isEqualTo(expectedContainerPorts);
    }

    @Test
    void testMainContainerEnv() {
        final List<EnvVar> envVars = this.resultMainContainer.getEnv();

        final Map<String, String> envs = new HashMap<>();
        envVars.forEach(env -> envs.put(env.getName(), env.getValue()));
        this.customizedEnvs.forEach((k, v) -> assertThat(v).isEqualTo(envs.get(k)));

        assertThat(envVars)
                .anyMatch(
                        env ->
                                env.getName().equals(Constants.ENV_FLINK_POD_IP_ADDRESS)
                                        && env.getValueFrom()
                                                .getFieldRef()
                                                .getApiVersion()
                                                .equals(Constants.API_VERSION)
                                        && env.getValueFrom()
                                                .getFieldRef()
                                                .getFieldPath()
                                                .equals(Constants.POD_IP_FIELD_PATH));
    }

    @Test
    void testPodLabels() {
        final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
        expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
        expectedLabels.putAll(userLabels);

        assertThat(this.resultPod.getMetadata().getLabels()).isEqualTo(expectedLabels);
    }

    @Test
    void testPodAnnotations() {
        final Map<String, String> resultAnnotations =
                kubernetesJobManagerParameters.getAnnotations();
        assertThat(resultAnnotations).isEqualTo(ANNOTATIONS);
    }

    @Test
    void testPodServiceAccountName() {
        assertThat(this.resultPod.getSpec().getServiceAccountName())
                .isEqualTo(SERVICE_ACCOUNT_NAME);
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
    void testDNSPolicyDefaultValue() {
        assertThat(this.resultPod.getSpec().getDnsPolicy()).isEqualTo(Constants.DNS_POLICY_DEFAULT);
    }
}
