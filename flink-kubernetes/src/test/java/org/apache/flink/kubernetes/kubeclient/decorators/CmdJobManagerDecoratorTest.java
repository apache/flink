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

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.EnvVar;
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
        // The command decorator resolves the deployment target, so it must be set for all tests.
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());

        this.cmdJobManagerDecorator = new CmdJobManagerDecorator(kubernetesJobManagerParameters);
    }

    @Test
    void testContainerIsDecorated() {
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

    @Test
    void testActiveProcessorCountLimitedToCpuRequest() {
        final FlinkPod resultFlinkPod = cmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod);
        assertThat(jvmArgsEnvValue(resultFlinkPod)).isEqualTo("-XX:ActiveProcessorCount=2");
    }

    @Test
    void testUserActiveProcessorCountInDefaultJvmOptionsTakesPrecedence() {
        flinkConfig.set(CoreOptions.FLINK_DEFAULT_JM_JVM_OPTIONS, "-XX:ActiveProcessorCount=8");
        assertNoJvmArgs(cmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod), baseFlinkPod);
    }

    @Test
    void testActiveProcessorCountLimitDisabled() {
        flinkConfig.set(
                KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT_ACTIVE_PROCESSOR_COUNT_ENABLED,
                false);
        assertNoJvmArgs(cmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod), baseFlinkPod);
    }

    @Test
    void testTaskManagerLimitFactorDoesNotLimitJobManager() {
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT_FACTOR, 1.0);
        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_FACTOR, 4.0);
        assertNoJvmArgs(cmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod), baseFlinkPod);
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

    private static String jvmArgsEnvValue(FlinkPod flinkPod) {
        return KubernetesTestUtils.getEnvVarValue(
                flinkPod.getMainContainer(), Constants.ENV_JVM_ARGS);
    }

    private static void assertNoJvmArgs(FlinkPod resultFlinkPod, FlinkPod inputFlinkPod) {
        assertThat(resultFlinkPod.getMainContainer().getEnv())
                .extracting(EnvVar::getName)
                .doesNotContain(Constants.ENV_JVM_ARGS);
        assertThat(resultFlinkPod.getMainContainer().getEnv())
                .containsAll(inputFlinkPod.getMainContainer().getEnv());
    }
}
