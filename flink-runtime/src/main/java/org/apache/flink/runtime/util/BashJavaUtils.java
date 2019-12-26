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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Utility class for using java utilities in bash scripts.
 */
public class BashJavaUtils {

	public static void main(String[] args) throws Exception {
		checkArgument(args.length > 0, "Command not specified.");

		switch (Command.valueOf(args[0])) {
			case GET_TM_RESOURCE_DYNAMIC_CONFIGS:
				getTmResourceDynamicConfigs(args);
				break;
			case GET_TM_RESOURCE_JVM_PARAMS:
				getTmResourceJvmParams(args);
				break;
			default:
				// unexpected, Command#valueOf should fail if a unknown command is passed in
				throw new RuntimeException("Unexpected, something is wrong.");
		}
	}

	private static void getTmResourceDynamicConfigs(String[] args) throws Exception {
		Configuration configuration = getConfigurationForStandaloneTaskManagers(args);
		TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);
		System.out.println(TaskExecutorResourceUtils.generateDynamicConfigsStr(taskExecutorResourceSpec));
	}

	private static void getTmResourceJvmParams(String[] args) throws Exception {
		Configuration configuration = getConfigurationForStandaloneTaskManagers(args);
		TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);
		System.out.println(TaskExecutorResourceUtils.generateJvmParametersStr(taskExecutorResourceSpec));
	}

	private static Configuration getConfigurationForStandaloneTaskManagers(String[] args) throws Exception {
		Configuration configuration = TaskManagerRunner.loadConfiguration(Arrays.copyOfRange(args, 1, args.length));
		return TaskExecutorResourceUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
			configuration, TaskManagerOptions.TOTAL_FLINK_MEMORY);
	}

	/**
	 * Commands that BashJavaUtils supports.
	 */
	public enum Command {
		/**
		 * Get dynamic configs of task executor resources.
		 */
		GET_TM_RESOURCE_DYNAMIC_CONFIGS,

		/**
		 * Get JVM parameters of task executor resources.
		 */
		GET_TM_RESOURCE_JVM_PARAMS
	}
}
