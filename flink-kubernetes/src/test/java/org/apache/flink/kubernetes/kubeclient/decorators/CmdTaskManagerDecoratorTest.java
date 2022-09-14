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
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;

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
                .containsExactly(
                        new EnvVarBuilder()
                                .withName(Constants.ENV_TM_JVM_MEM_OPTS)
                                .withValue(JVM_MEM_OPTS_ENV)
                                .build());
    }
}
