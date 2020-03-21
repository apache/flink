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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverhead;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverheadOptions;
import org.apache.flink.runtime.util.config.memory.LegacyHeapOptions;
import org.apache.flink.runtime.util.config.memory.LegacyMemoryUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemorySpecBase;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemory;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * JobManager utils to calculate {@link JobManagerProcessSpec} and JVM args.
 */
public class JobManagerProcessUtils {
	private static final Logger LOG = LoggerFactory.getLogger(JobManagerProcessUtils.class);

	static final List<ConfigOption<MemorySize>> TM_REQUIRED_FINE_GRAINED_OPTIONS =
		Collections.singletonList(JobManagerOptions.JVM_HEAP_MEMORY);

	static final JvmMetaspaceAndOverheadOptions JM_JVM_METASPACE_AND_OVERHEAD_OPTIONS =
		new JvmMetaspaceAndOverheadOptions(
			JobManagerOptions.JVM_METASPACE,
			JobManagerOptions.JVM_OVERHEAD_MIN,
			JobManagerOptions.JVM_OVERHEAD_MAX,
			JobManagerOptions.JVM_OVERHEAD_FRACTION);

	@SuppressWarnings("deprecation")
	static final LegacyHeapOptions JM_LEGACY_HEAP_OPTIONS =
		new LegacyHeapOptions(
			"FLINK_JM_HEAP",
			JobManagerOptions.JOB_MANAGER_HEAP_MEMORY,
			JobManagerOptions.JOB_MANAGER_HEAP_MEMORY_MB);

	private static final ProcessMemoryUtils<JobManagerFlinkMemory> PROCESS_MEMORY_UTILS = new ProcessMemoryUtils<>(
		TM_REQUIRED_FINE_GRAINED_OPTIONS,
		new JobManagerFlinkMemoryUtils(),
		JobManagerOptions.TOTAL_FLINK_MEMORY,
		JobManagerOptions.TOTAL_PROCESS_MEMORY,
		JM_JVM_METASPACE_AND_OVERHEAD_OPTIONS);

	private static final LegacyMemoryUtils LEGACY_MEMORY_UTILS = new LegacyMemoryUtils(JM_LEGACY_HEAP_OPTIONS);

	private JobManagerProcessUtils() {
	}

	public static JobManagerProcessSpec processSpecFromConfig(Configuration config) {
		return createMemoryProcessSpec(config, PROCESS_MEMORY_UTILS.memoryProcessSpecFromConfig(config));
	}

	private static JobManagerProcessSpec createMemoryProcessSpec(
			Configuration config,
			ProcessMemorySpecBase<JobManagerFlinkMemory> processMemory) {
		JobManagerFlinkMemory flinkMemory = processMemory.getFlinkMemory();
		JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = processMemory.getJvmMetaspaceAndOverhead();
		verifyJvmHeapSize(flinkMemory.getJvmHeapMemorySize());
		verifyJobStoreCacheSize(config, flinkMemory.getJvmHeapMemorySize());
		return new JobManagerProcessSpec(flinkMemory, jvmMetaspaceAndOverhead);
	}

	private static void verifyJvmHeapSize(MemorySize jvmHeapSize) {
		if (jvmHeapSize.compareTo(JobManagerOptions.MIN_JVM_HEAP_SIZE) < 1) {
			LOG.warn(
				"The configured or derived JVM heap memory size ({}) is less than its recommended minimum value ({})",
				jvmHeapSize.toHumanReadableString(),
				JobManagerOptions.MIN_JVM_HEAP_SIZE);
		}
	}

	private static void verifyJobStoreCacheSize(Configuration config, MemorySize jvmHeapSize) {
		MemorySize jobStoreCacheHeapSize =
			MemorySize.parse(config.getLong(JobManagerOptions.JOB_STORE_CACHE_SIZE) + "b");
		if (jvmHeapSize.compareTo(jobStoreCacheHeapSize) < 1) {
			LOG.warn(
				"The configured or derived JVM heap memory size ({}: {}) is less than the configured or default size " +
					"of the job store cache ({}: {})",
				JobManagerOptions.JOB_STORE_CACHE_SIZE.key(),
				jvmHeapSize.toHumanReadableString(),
				JobManagerOptions.JVM_HEAP_MEMORY.key(),
				jobStoreCacheHeapSize.toHumanReadableString());
		}
	}

	public static Configuration getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
			Configuration configuration,
			ConfigOption<MemorySize> configOption) {
		return LEGACY_MEMORY_UTILS.getConfWithLegacyHeapSizeMappedToNewConfigOption(configuration, configOption);
	}
}
