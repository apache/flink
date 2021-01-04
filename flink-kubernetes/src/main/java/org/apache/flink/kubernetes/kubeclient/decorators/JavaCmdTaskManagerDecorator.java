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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.taskmanager.KubernetesTaskExecutorRunner;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.entrypoint.parser.CommandLineOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Attach the jvm command and args to the main container for running the TaskManager code. */
public class JavaCmdTaskManagerDecorator extends AbstractKubernetesStepDecorator {

    private final KubernetesTaskManagerParameters kubernetesTaskManagerParameters;

    public JavaCmdTaskManagerDecorator(
            KubernetesTaskManagerParameters kubernetesTaskManagerParameters) {
        this.kubernetesTaskManagerParameters = checkNotNull(kubernetesTaskManagerParameters);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final Container mainContainerWithStartCmd =
                new ContainerBuilder(flinkPod.getMainContainer())
                        .withCommand(kubernetesTaskManagerParameters.getContainerEntrypoint())
                        .withArgs(getTaskManagerStartCommand())
                        .build();

        return new FlinkPod.Builder(flinkPod).withMainContainer(mainContainerWithStartCmd).build();
    }

    private List<String> getTaskManagerStartCommand() {
        final String confDirInPod = kubernetesTaskManagerParameters.getFlinkConfDirInPod();

        final String logDirInPod = kubernetesTaskManagerParameters.getFlinkLogDirInPod();

        final String mainClassArgs =
                "--"
                        + CommandLineOptions.CONFIG_DIR_OPTION.getLongOpt()
                        + " "
                        + confDirInPod
                        + " "
                        + kubernetesTaskManagerParameters.getDynamicProperties();

        final String javaCommand =
                getTaskManagerStartCommand(
                        kubernetesTaskManagerParameters.getFlinkConfiguration(),
                        kubernetesTaskManagerParameters.getContaineredTaskManagerParameters(),
                        confDirInPod,
                        logDirInPod,
                        kubernetesTaskManagerParameters.hasLogback(),
                        kubernetesTaskManagerParameters.hasLog4j(),
                        KubernetesTaskExecutorRunner.class.getCanonicalName(),
                        mainClassArgs);
        return KubernetesUtils.getStartCommandWithBashWrapper(javaCommand);
    }

    private static String getTaskManagerStartCommand(
            Configuration flinkConfig,
            ContaineredTaskManagerParameters tmParams,
            String configDirectory,
            String logDirectory,
            boolean hasLogback,
            boolean hasLog4j,
            String mainClass,
            String mainArgs) {
        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                tmParams.getTaskExecutorProcessSpec();
        final String jvmMemOpts =
                ProcessMemoryUtils.generateJvmParametersStr(taskExecutorProcessSpec);
        String args = TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec);
        if (mainArgs != null) {
            args += " " + mainArgs;
        }

        return KubernetesUtils.getCommonStartCommand(
                flinkConfig,
                KubernetesUtils.ClusterComponent.TASK_MANAGER,
                jvmMemOpts,
                configDirectory,
                logDirectory,
                hasLogback,
                hasLog4j,
                mainClass,
                args);
    }
}
