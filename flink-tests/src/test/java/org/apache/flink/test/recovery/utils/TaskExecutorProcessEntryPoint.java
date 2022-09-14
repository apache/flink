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

package org.apache.flink.test.recovery.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The entry point for the TaskExecutor JVM. Simply configures and runs a TaskExecutor. */
public class TaskExecutorProcessEntryPoint {

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorProcessEntryPoint.class);

    public static void main(String[] args) {
        try {
            final ParameterTool parameterTool = ParameterTool.fromArgs(args);
            Configuration cfg = parameterTool.getConfiguration();
            final PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(cfg);
            TaskExecutorResourceUtils.adjustForLocalExecution(cfg);

            TaskManagerRunner.runTaskManager(cfg, pluginManager);
        } catch (Throwable t) {
            LOG.error("Failed to run the TaskManager process", t);
            System.exit(1);
        }
    }
}
