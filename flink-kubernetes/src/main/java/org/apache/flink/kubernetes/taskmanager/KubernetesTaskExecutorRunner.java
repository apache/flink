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

package org.apache.flink.kubernetes.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class is the executable entry point for running a TaskExecutor in a Kubernetes pod. */
public class KubernetesTaskExecutorRunner {

    protected static final Logger LOG = LoggerFactory.getLogger(KubernetesTaskExecutorRunner.class);

    public static void main(String[] args) {
        EnvironmentInformation.logEnvironmentInfo(LOG, "Kubernetes TaskExecutor runner", args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        runTaskManagerSecurely(args);
    }

    private static void runTaskManagerSecurely(String[] args) {
        Configuration configuration = null;

        try {
            configuration = TaskManagerRunner.loadConfiguration(args);
            final String nodeId = System.getenv().get(Constants.ENV_FLINK_POD_NODE_ID);
            Preconditions.checkState(
                    nodeId != null,
                    "The environment variable %s is not set, "
                            + "which is used to identify the node where the task manager is located.",
                    Constants.ENV_FLINK_POD_NODE_ID);
            configuration.setString(TaskManagerOptionsInternal.TASK_MANAGER_NODE_ID, nodeId);
        } catch (FlinkParseException fpe) {
            LOG.error("Could not load the configuration.", fpe);
            System.exit(TaskManagerRunner.FAILURE_EXIT_CODE);
        }

        TaskManagerRunner.runTaskManagerProcessSecurely(checkNotNull(configuration));
    }
}
