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

import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** General tests for the {@link CmdJobManagerDecorator}. */
class CmdJobManagerDecoratorTest extends KubernetesJobManagerTestBase {

    private CmdJobManagerDecorator cmdJobManagerDecorator;

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        this.cmdJobManagerDecorator = new CmdJobManagerDecorator(kubernetesJobManagerParameters);
    }

    @Test
    void testContainerIsDecorated() {
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        final FlinkPod resultFlinkPod = cmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod);
        assertThat(resultFlinkPod.getPodWithoutMainContainer())
                .isEqualTo(baseFlinkPod.getPodWithoutMainContainer());
        assertThat(resultFlinkPod.getMainContainer()).isNotEqualTo(baseFlinkPod.getMainContainer());
    }

    @Test
    void testSessionClusterCommandsAndArgs() {
        testJobManagerCommandsAndArgs(KubernetesDeploymentTarget.SESSION.getName());
    }

    @Test
    void testApplicationClusterCommandsAndArgs() {
        testJobManagerCommandsAndArgs(KubernetesDeploymentTarget.APPLICATION.getName());
    }

    @Test
    public void testUnsupportedDeploymentTargetShouldFail() {
        assertThatThrownBy(() -> testJobManagerCommandsAndArgs("unsupported-deployment-target"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void testJobManagerCommandsAndArgs(String target) {
        flinkConfig.set(DeploymentOptions.TARGET, target);
        final FlinkPod resultFlinkPod = cmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod);
        final String entryCommand = flinkConfig.get(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH);
        assertThat(resultFlinkPod.getMainContainer().getCommand())
                .containsExactlyInAnyOrder(entryCommand);
        List<String> flinkCommands =
                KubernetesUtils.getStartCommandWithBashWrapper(
                        Constants.KUBERNETES_JOB_MANAGER_SCRIPT_PATH
                                + " "
                                + target
                                + " "
                                + ENTRYPOINT_ARGS);
        assertThat(resultFlinkPod.getMainContainer().getArgs())
                .containsExactlyElementsOf(flinkCommands);
    }
}
