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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.CommonProcessMemorySpec;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverheadOptions;
import org.apache.flink.runtime.util.config.memory.LegacyMemoryOptions;
import org.apache.flink.runtime.util.config.memory.MemoryBackwardsCompatibilityUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemory;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemoryUtils;

import java.util.Collections;

/**
 * JobManager utils to calculate {@link JobManagerProcessSpec} and JVM args.
 */
public class JobManagerProcessUtils {

	static final ProcessMemoryOptions JM_PROCESS_MEMORY_OPTIONS = new ProcessMemoryOptions(
		Collections.singletonList(JobManagerOptions.JVM_HEAP_MEMORY),
		JobManagerOptions.TOTAL_FLINK_MEMORY,
		JobManagerOptions.TOTAL_PROCESS_MEMORY,
		new JvmMetaspaceAndOverheadOptions(
			JobManagerOptions.JVM_METASPACE,
			JobManagerOptions.JVM_OVERHEAD_MIN,
			JobManagerOptions.JVM_OVERHEAD_MAX,
			JobManagerOptions.JVM_OVERHEAD_FRACTION));

	@SuppressWarnings("deprecation")
	static final LegacyMemoryOptions JM_LEGACY_HEAP_OPTIONS =
		new LegacyMemoryOptions(
			"FLINK_JM_HEAP",
			JobManagerOptions.JOB_MANAGER_HEAP_MEMORY,
			JobManagerOptions.JOB_MANAGER_HEAP_MEMORY_MB);

	private static final ProcessMemoryUtils<JobManagerFlinkMemory> PROCESS_MEMORY_UTILS = new ProcessMemoryUtils<>(
		JM_PROCESS_MEMORY_OPTIONS,
		new JobManagerFlinkMemoryUtils());

	private static final MemoryBackwardsCompatibilityUtils LEGACY_MEMORY_UTILS = new MemoryBackwardsCompatibilityUtils(JM_LEGACY_HEAP_OPTIONS);

	private JobManagerProcessUtils() {
	}

	public static JobManagerProcessSpec processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
			Configuration config,
			ConfigOption<MemorySize> newOptionToInterpretLegacyHeap) {
		return processSpecFromConfig(
			getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(config, newOptionToInterpretLegacyHeap));
	}

	static JobManagerProcessSpec processSpecFromConfig(Configuration config) {
		return createMemoryProcessSpec(PROCESS_MEMORY_UTILS.memoryProcessSpecFromConfig(config));
	}

	private static JobManagerProcessSpec createMemoryProcessSpec(
			CommonProcessMemorySpec<JobManagerFlinkMemory> processMemory) {
		return new JobManagerProcessSpec(processMemory.getFlinkMemory(), processMemory.getJvmMetaspaceAndOverhead());
	}

	static Configuration getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
			Configuration configuration,
			ConfigOption<MemorySize> configOption) {
		return LEGACY_MEMORY_UTILS.getConfWithLegacyHeapSizeMappedToNewConfigOption(configuration, configOption);
	}

	@VisibleForTesting
	public static JobManagerProcessSpec createDefaultJobManagerProcessSpec(int totalProcessMemoryMb) {
		Configuration configuration = new Configuration();
		configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(totalProcessMemoryMb));
		return processSpecFromConfig(configuration);
	}
}
