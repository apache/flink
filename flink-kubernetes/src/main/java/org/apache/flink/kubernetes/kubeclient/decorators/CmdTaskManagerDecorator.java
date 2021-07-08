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

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Attach the command and args to the main container for running the TaskManager. */
public class CmdTaskManagerDecorator extends AbstractKubernetesStepDecorator {

    private final KubernetesTaskManagerParameters kubernetesTaskManagerParameters;

    public CmdTaskManagerDecorator(
            KubernetesTaskManagerParameters kubernetesTaskManagerParameters) {
        this.kubernetesTaskManagerParameters = checkNotNull(kubernetesTaskManagerParameters);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final Container mainContainerWithStartCmd =
                new ContainerBuilder(flinkPod.getMainContainer())
                        .withCommand(kubernetesTaskManagerParameters.getContainerEntrypoint())
                        .withArgs(getTaskManagerStartCommand())
                        .addToEnv(
                                new EnvVarBuilder()
                                        .withName(Constants.ENV_TM_JVM_MEM_OPTS)
                                        .withValue(
                                                kubernetesTaskManagerParameters.getJvmMemOptsEnv())
                                        .build())
                        .build();

        return new FlinkPod.Builder(flinkPod).withMainContainer(mainContainerWithStartCmd).build();
    }

    private List<String> getTaskManagerStartCommand() {
        final String resourceArgs =
                TaskExecutorProcessUtils.generateDynamicConfigsStr(
                        kubernetesTaskManagerParameters
                                .getContaineredTaskManagerParameters()
                                .getTaskExecutorProcessSpec());

        final String dynamicProperties = kubernetesTaskManagerParameters.getDynamicProperties();

        return KubernetesUtils.getStartCommandWithBashWrapper(
                Constants.KUBERNETES_TASK_MANAGER_SCRIPT_PATH
                        + " "
                        + dynamicProperties
                        + " "
                        + resourceArgs);
    }
}
