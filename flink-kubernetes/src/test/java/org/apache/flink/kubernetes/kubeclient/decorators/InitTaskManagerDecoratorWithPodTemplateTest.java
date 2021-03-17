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

import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ContainerPort;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link InitTaskManagerDecorator} with pod template. */
public class InitTaskManagerDecoratorWithPodTemplateTest extends DecoratorWithPodTemplateTestBase {

    private static final String POD_NAME = "taskmanager-" + UUID.randomUUID().toString();

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();
        this.flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS, ANNOTATIONS);
        this.flinkConfig.setString(
                KubernetesConfigOptions.TASK_MANAGER_TOLERATIONS.key(), TOLERATION_STRING);
        this.customizedEnvs.forEach(
                (k, v) ->
                        this.flinkConfig.setString(
                                ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX + k,
                                v));
        this.flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_LABELS, userLabels);
        this.flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_NODE_SELECTOR, nodeSelector);
    }

    @Override
    public FlinkPod getResultPod(FlinkPod podTemplate) {
        final KubernetesTaskManagerParameters kubernetesTaskManagerParameters =
                KubernetesTestUtils.createTaskManagerParameters(flinkConfig, POD_NAME);
        final InitTaskManagerDecorator initTaskManagerDecorator =
                new InitTaskManagerDecorator(kubernetesTaskManagerParameters);
        return initTaskManagerDecorator.decorateFlinkPod(podTemplate);
    }

    @Test
    public void testTaskManagerMainContainerPortsMerging() {
        final List<String> expectedContainerPorts = new ArrayList<>();
        expectedContainerPorts.add(Constants.TASK_MANAGER_RPC_PORT_NAME);
        // Add port from pod template
        expectedContainerPorts.add("testing-port");
        assertThat(
                this.resultPod.getMainContainer().getPorts().stream()
                        .map(ContainerPort::getName)
                        .collect(Collectors.toList()),
                containsInAnyOrder(expectedContainerPorts.toArray()));
    }

    @Test
    public void testTaskManagerPodRestartPolicyOverwritten() {
        assertThat(
                resultPod.getPodWithoutMainContainer().getSpec().getRestartPolicy(),
                is(equalToIgnoringCase(Constants.RESTART_POLICY_OF_NEVER)));
    }

    @Test
    public void testTaskManagerPodNameOverwritten() {
        assertThat(resultPod.getPodWithoutMainContainer().getMetadata().getName(), is(POD_NAME));
    }
}
