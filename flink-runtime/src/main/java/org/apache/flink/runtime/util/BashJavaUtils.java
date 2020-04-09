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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.entrypoint.ClusterConfigurationParserFactory;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.Options;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Utility class for using java utilities in bash scripts.
 */
public class BashJavaUtils {

	@VisibleForTesting
	public static final String EXECUTION_PREFIX = "BASH_JAVA_UTILS_EXEC_RESULT:";

	public static void main(String[] args) throws Exception {
		checkArgument(args.length > 0, "Command not specified.");

		switch (Command.valueOf(args[0])) {
			case GET_TM_RESOURCE_PARAMS:
				getTmResourceParams(Arrays.copyOfRange(args, 1, args.length));
				break;
			default:
				// unexpected, Command#valueOf should fail if a unknown command is passed in
				throw new RuntimeException("Unexpected, something is wrong.");
		}
	}

	/**
	 * Generate and print JVM parameters and dynamic configs of task executor resources. The last two lines of
	 * the output should be JVM parameters and dynamic configs respectively.
	 */
	private static void getTmResourceParams(String[] args) throws Exception {
		Configuration configuration = getConfigurationForStandaloneTaskManagers(args);
		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(configuration);
		System.out.println(EXECUTION_PREFIX + ProcessMemoryUtils.generateJvmParametersStr(taskExecutorProcessSpec));
		System.out.println(EXECUTION_PREFIX + TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec));
	}

	private static Configuration getConfigurationForStandaloneTaskManagers(String[] args) throws Exception {
		Configuration configuration = loadConfiguration(args);
		return TaskExecutorProcessUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
			configuration, TaskManagerOptions.TOTAL_FLINK_MEMORY);
	}

	@VisibleForTesting
	static Configuration loadConfiguration(String[] args) throws FlinkException {
		return ConfigurationParserUtils.loadCommonConfiguration(
			filterCmdArgs(args),
			BashJavaUtils.class.getSimpleName());
	}

	private static String[] filterCmdArgs(String[] args) {
		final Options options = ClusterConfigurationParserFactory.options();
		final List<String> filteredArgs = new ArrayList<>();
		final Iterator<String> iter = Arrays.asList(args).iterator();

		while (iter.hasNext()) {
			String token = iter.next();
			if (options.hasOption(token)) {
				filteredArgs.add(token);
				if (options.getOption(token).hasArg() && iter.hasNext()) {
					filteredArgs.add(iter.next());
				}
			} else if (token.startsWith("-D")) {
				// "-Dkey=value"
				filteredArgs.add(token);
			}
		}

		return filteredArgs.toArray(new String[0]);
	}

	/**
	 * Commands that BashJavaUtils supports.
	 */
	public enum Command {
		/**
		 * Get JVM parameters and dynamic configs of task executor resources.
		 */
		GET_TM_RESOURCE_PARAMS
	}
}
