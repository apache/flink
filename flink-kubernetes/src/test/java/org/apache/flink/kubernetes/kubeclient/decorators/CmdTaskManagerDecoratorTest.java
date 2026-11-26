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

import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** General tests for the{@link CmdTaskManagerDecorator}. */
class CmdTaskManagerDecoratorTest extends KubernetesTaskManagerTestBase {

    private String mainClassArgs;

    private CmdTaskManagerDecorator cmdTaskManagerDecorator;

    @Override
    public void onSetup() throws Exception {
        super.onSetup();

        this.mainClassArgs =
                TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec);

        this.cmdTaskManagerDecorator =
                new CmdTaskManagerDecorator(this.kubernetesTaskManagerParameters);
    }

    @Test
    void testContainerIsDecorated() {
        final FlinkPod resultFlinkPod = cmdTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod);
        assertThat(resultFlinkPod.getPodWithoutMainContainer())
                .isEqualTo(baseFlinkPod.getPodWithoutMainContainer());
        assertThat(resultFlinkPod.getMainContainer()).isNotEqualTo(baseFlinkPod.getMainContainer());
    }

    @Test
    void testTaskManagerStartCommandsAndArgs() {
        final FlinkPod resultFlinkPod = cmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod);
        final String entryCommand = flinkConfig.get(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH);
        assertThat(resultFlinkPod.getMainContainer().getCommand())
                .containsExactlyInAnyOrder(entryCommand);
        List<String> flinkCommands =
                KubernetesUtils.getStartCommandWithBashWrapper(
                        Constants.KUBERNETES_TASK_MANAGER_SCRIPT_PATH
                                + " "
                                + DYNAMIC_PROPERTIES
                                + " "
                                + mainClassArgs
                                + " "
                                + ENTRYPOINT_ARGS);
        assertThat(resultFlinkPod.getMainContainer().getArgs())
                .containsExactlyElementsOf(flinkCommands);
    }

    @Test
    void testTaskManagerJvmMemOptsEnv() {
        final FlinkPod resultFlinkPod = cmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod);
        assertThat(resultFlinkPod.getMainContainer().getEnv())
                .contains(
                        new EnvVarBuilder()
                                .withName(Constants.ENV_TM_JVM_MEM_OPTS)
                                .withValue(JVM_MEM_OPTS_ENV)
                                .build());
    }

    @Test
    void testActiveProcessorCountLimitedToCpuRequest() {
        final FlinkPod resultFlinkPod = cmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod);
        assertThat(jvmArgsEnvValue(resultFlinkPod)).isEqualTo("-XX:ActiveProcessorCount=2");
    }

    @Test
    void testUserActiveProcessorCountInPodTemplateEnvTakesPrecedence() {
        final EnvVar userJavaOpts =
                new EnvVar(
                        Constants.ENV_FLINK_ENV_JAVA_OPTS_TM, "-XX:ActiveProcessorCount=8", null);
        final FlinkPod podWithEnv =
                new FlinkPod.Builder(baseFlinkPod)
                        .withMainContainer(
                                new ContainerBuilder(baseFlinkPod.getMainContainer())
                                        .addToEnv(userJavaOpts)
                                        .build())
                        .build();
        assertNoJvmArgs(cmdTaskManagerDecorator.decorateFlinkPod(podWithEnv), podWithEnv);
    }

    @Test
    void testActiveProcessorCountLimitDisabled() {
        flinkConfig.set(
                KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_ACTIVE_PROCESSOR_COUNT_ENABLED,
                false);
        assertNoJvmArgs(cmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod), baseFlinkPod);
    }

    @Test
    void testJobManagerLimitFactorDoesNotLimitTaskManager() {
        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_FACTOR, 1.0);
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT_FACTOR, 4.0);
        assertNoJvmArgs(cmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod), baseFlinkPod);
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
