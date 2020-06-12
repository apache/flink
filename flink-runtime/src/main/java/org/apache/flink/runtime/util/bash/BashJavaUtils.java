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

package org.apache.flink.runtime.util.bash;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.jobmanager.JobManagerProcessSpec;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemory;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemory;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Utility class for using java utilities in bash scripts.
 */
public class BashJavaUtils {
	private static final Logger LOG = LoggerFactory.getLogger(BashJavaUtils.class);

	@VisibleForTesting
	public static final String EXECUTION_PREFIX = "BASH_JAVA_UTILS_EXEC_RESULT:";

	private BashJavaUtils() {
	}

	public static void main(String[] args) throws FlinkException {
		checkArgument(args.length > 0, "Command not specified.");

		Command command = Command.valueOf(args[0]);
		String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);
		List<String> output = runCommand(command, commandArgs);
		for (String outputLine : output) {
			System.out.println(EXECUTION_PREFIX + outputLine);
		}
	}

	private static List<String> runCommand(Command command, String[] commandArgs) throws FlinkException {
		Configuration configuration = FlinkConfigLoader.loadConfiguration(commandArgs);
		switch (command) {
			case GET_TM_RESOURCE_PARAMS:
				return getTmResourceParams(configuration);
			case GET_JM_RESOURCE_PARAMS:
				return getJmResourceParams(configuration);
			default:
				// unexpected, Command#valueOf should fail if a unknown command is passed in
				throw new RuntimeException("Unexpected, something is wrong.");
		}
	}

	/**
	 * Generate and print JVM parameters and dynamic configs of task executor resources. The last two lines of
	 * the output should be JVM parameters and dynamic configs respectively.
	 */
	private static List<String> getTmResourceParams(Configuration configuration) {
		Configuration configurationWithFallback = TaskExecutorProcessUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
			configuration,
			TaskManagerOptions.TOTAL_FLINK_MEMORY);
		TaskExecutorProcessSpec taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(configurationWithFallback);

		logTaskExecutorConfiguration(taskExecutorProcessSpec);

		return Arrays.asList(
			ProcessMemoryUtils.generateJvmParametersStr(taskExecutorProcessSpec),
			TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec));
	}

	/**
	 * Generate and print JVM parameters of Flink Master resources as one line.
	 */
	@VisibleForTesting
	static List<String> getJmResourceParams(Configuration configuration) {
		JobManagerProcessSpec jobManagerProcessSpec = JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
			configuration,
			JobManagerOptions.JVM_HEAP_MEMORY);

		logMasterConfiguration(jobManagerProcessSpec);

		return Collections.singletonList(ProcessMemoryUtils.generateJvmParametersStr(jobManagerProcessSpec));
	}

	private static void logMasterConfiguration(JobManagerProcessSpec spec) {
		JobManagerFlinkMemory flinkMemory = spec.getFlinkMemory();
		LOG.info("Final Master Memory configuration:");
		LOG.info("  Total Process Memory: {}", spec.getTotalProcessMemorySize().toHumanReadableString());
		LOG.info("    Total Flink Memory: {}", flinkMemory.getTotalFlinkMemorySize().toHumanReadableString());
		LOG.info("      JVM Heap:         {}", flinkMemory.getJvmHeapMemorySize().toHumanReadableString());
		LOG.info("      Off-heap:         {}", flinkMemory.getJvmDirectMemorySize().toHumanReadableString());
		LOG.info("    JVM Metaspace:      {}", spec.getJvmMetaspaceSize().toHumanReadableString());
		LOG.info("    JVM Overhead:       {}", spec.getJvmOverheadSize().toHumanReadableString());
	}

	private static void logTaskExecutorConfiguration(TaskExecutorProcessSpec spec) {
		TaskExecutorFlinkMemory flinkMemory = spec.getFlinkMemory();
		MemorySize totalOffHeapMemory = flinkMemory.getManaged().add(flinkMemory.getJvmDirectMemorySize());
		LOG.info("Final TaskExecutor Memory configuration:");
		LOG.info("  Total Process Memory:          {}", spec.getTotalProcessMemorySize().toHumanReadableString());
		LOG.info("    Total Flink Memory:          {}", flinkMemory.getTotalFlinkMemorySize().toHumanReadableString());
		LOG.info("      Total JVM Heap Memory:     {}", flinkMemory.getJvmHeapMemorySize().toHumanReadableString());
		LOG.info("        Framework:               {}", flinkMemory.getFrameworkHeap().toHumanReadableString());
		LOG.info("        Task:                    {}", flinkMemory.getTaskHeap().toHumanReadableString());
		LOG.info("      Total Off-heap Memory:     {}", totalOffHeapMemory.toHumanReadableString());
		LOG.info("        Managed:                 {}", flinkMemory.getManaged().toHumanReadableString());
		LOG.info("        Total JVM Direct Memory: {}", flinkMemory.getJvmDirectMemorySize().toHumanReadableString());
		LOG.info("          Framework:             {}", flinkMemory.getFrameworkOffHeap().toHumanReadableString());
		LOG.info("          Task:                  {}", flinkMemory.getTaskOffHeap().toHumanReadableString());
		LOG.info("          Network:               {}", flinkMemory.getNetwork().toHumanReadableString());
		LOG.info("    JVM Metaspace:               {}", spec.getJvmMetaspaceSize().toHumanReadableString());
		LOG.info("    JVM Overhead:                {}", spec.getJvmOverheadSize().toHumanReadableString());
	}

	/**
	 * Commands that BashJavaUtils supports.
	 */
	public enum Command {
		/**
		 * Get JVM parameters and dynamic configs of task executor resources.
		 */
		GET_TM_RESOURCE_PARAMS,

		/**
		 * Get JVM parameters and dynamic configs of job manager resources.
		 */
		GET_JM_RESOURCE_PARAMS
	}
}
