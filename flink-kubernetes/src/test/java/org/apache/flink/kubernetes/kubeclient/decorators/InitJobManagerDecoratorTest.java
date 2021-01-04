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
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
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
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** General tests for the {@link InitJobManagerDecorator}. */
public class InitJobManagerDecoratorTest extends KubernetesJobManagerTestBase {

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
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        final InitJobManagerDecorator initJobManagerDecorator =
                new InitJobManagerDecorator(this.kubernetesJobManagerParameters);
        final FlinkPod resultFlinkPod = initJobManagerDecorator.decorateFlinkPod(this.baseFlinkPod);

        this.resultPod = resultFlinkPod.getPod();
        this.resultMainContainer = resultFlinkPod.getMainContainer();
    }

    @Test
    public void testApiVersion() {
        assertEquals(Constants.API_VERSION, this.resultPod.getApiVersion());
    }

    @Test
    public void testMainContainerName() {
        assertEquals(
                KubernetesJobManagerParameters.JOB_MANAGER_MAIN_CONTAINER_NAME,
                this.resultMainContainer.getName());
    }

    @Test
    public void testMainContainerImage() {
        assertEquals(CONTAINER_IMAGE, this.resultMainContainer.getImage());
        assertEquals(
                CONTAINER_IMAGE_PULL_POLICY.name(), this.resultMainContainer.getImagePullPolicy());
    }

    @Test
    public void testMainContainerResourceRequirements() {
        final ResourceRequirements resourceRequirements = this.resultMainContainer.getResources();

        final Map<String, Quantity> requests = resourceRequirements.getRequests();
        assertEquals(Double.toString(JOB_MANAGER_CPU), requests.get("cpu").getAmount());
        assertEquals(String.valueOf(JOB_MANAGER_MEMORY), requests.get("memory").getAmount());

        final Map<String, Quantity> limits = resourceRequirements.getLimits();
        assertEquals(Double.toString(JOB_MANAGER_CPU), limits.get("cpu").getAmount());
        assertEquals(String.valueOf(JOB_MANAGER_MEMORY), limits.get("memory").getAmount());
    }

    @Test
    public void testMainContainerPorts() {
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

        assertEquals(expectedContainerPorts, this.resultMainContainer.getPorts());
    }

    @Test
    public void testMainContainerEnv() {
        final List<EnvVar> envVars = this.resultMainContainer.getEnv();

        final Map<String, String> envs = new HashMap<>();
        envVars.forEach(env -> envs.put(env.getName(), env.getValue()));
        this.customizedEnvs.forEach((k, v) -> assertEquals(envs.get(k), v));

        assertTrue(
                envVars.stream()
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
                                                        .equals(Constants.POD_IP_FIELD_PATH)));
    }

    @Test
    public void testPodLabels() {
        final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
        expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
        expectedLabels.putAll(userLabels);

        assertEquals(expectedLabels, this.resultPod.getMetadata().getLabels());
    }

    @Test
    public void testPodAnnotations() {
        final Map<String, String> resultAnnotations =
                kubernetesJobManagerParameters.getAnnotations();
        assertThat(resultAnnotations, is(equalTo(ANNOTATIONS)));
    }

    @Test
    public void testPodServiceAccountName() {
        assertEquals(SERVICE_ACCOUNT_NAME, this.resultPod.getSpec().getServiceAccountName());
    }

    @Test
    public void testImagePullSecrets() {
        final List<String> resultSecrets =
                this.resultPod.getSpec().getImagePullSecrets().stream()
                        .map(LocalObjectReference::getName)
                        .collect(Collectors.toList());

        assertEquals(IMAGE_PULL_SECRETS, resultSecrets);
    }

    @Test
    public void testNodeSelector() {
        assertThat(this.resultPod.getSpec().getNodeSelector(), is(equalTo(nodeSelector)));
    }

    @Test
    public void testPodTolerations() {
        assertThat(
                this.resultPod.getSpec().getTolerations(),
                Matchers.containsInAnyOrder(TOLERATION.toArray()));
    }
}
