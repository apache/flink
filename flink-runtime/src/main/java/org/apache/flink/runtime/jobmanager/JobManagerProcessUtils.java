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
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.ProcessMemoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * JobManager utils to calculate {@link JobManagerProcessSpec} and JVM args.
 */
public final class JobManagerProcessUtils
		extends ProcessMemoryUtils<JobManagerProcessSpec, JmFlinkInternalMemory> {
	private static final Logger LOG = LoggerFactory.getLogger(JobManagerProcessUtils.class);

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

	private static final JobManagerProcessUtils INSTANCE = new JobManagerProcessUtils();

	private JobManagerProcessUtils() {
		super(
			Collections.singletonList(JobManagerOptions.JVM_HEAP_MEMORY),
			JobManagerOptions.TOTAL_FLINK_MEMORY,
			JobManagerOptions.TOTAL_PROCESS_MEMORY,
			JM_JVM_METASPACE_AND_OVERHEAD_OPTIONS,
			JM_LEGACY_HEAP_OPTIONS);
	}

	@Override
	protected JmFlinkInternalMemory deriveTotalFlinkMemoryFromInternalMemory(Configuration config) {
		MemorySize jvmHeapMemorySize = getMemorySizeFromConfig(config, JobManagerOptions.JVM_HEAP_MEMORY);
		MemorySize offHeapMemorySize = getMemorySizeFromConfig(config, JobManagerOptions.OFF_HEAP_MEMORY);
		MemorySize derivedTotalFlinkMemorySize = jvmHeapMemorySize.add(offHeapMemorySize);

		if (config.contains(JobManagerOptions.TOTAL_FLINK_MEMORY)) {
			// derive network memory from total flink memory, and check against network min/max
			MemorySize totalFlinkMemorySize = getMemorySizeFromConfig(config, JobManagerOptions.TOTAL_FLINK_MEMORY);
			if (derivedTotalFlinkMemorySize.getBytes() != totalFlinkMemorySize.getBytes()) {
				throw new IllegalConfigurationException(String.format(
					"Sum of the configured JVM Heap Memory (%s) and the configured or default Off-heap Memory (%s) " +
						"exceeds the configured Total Flink Memory (%s). Please, make the configuration consistent " +
						"or configure only one option: either JVM Heap or Total Flink Memory.",
					jvmHeapMemorySize.toHumanReadableString(),
					offHeapMemorySize.toHumanReadableString(),
					totalFlinkMemorySize.toHumanReadableString()));
			}
		}

		return new JmFlinkInternalMemory(jvmHeapMemorySize, offHeapMemorySize);
	}

	@Override
	protected JmFlinkInternalMemory deriveInternalMemoryFromTotalFlinkMemory(
		Configuration config,
		MemorySize totalFlinkMemorySize) {
		MemorySize offHeapMemorySize = getMemorySizeFromConfig(config, JobManagerOptions.OFF_HEAP_MEMORY);
		if (totalFlinkMemorySize.compareTo(offHeapMemorySize) < 1) {
			throw new IllegalConfigurationException(
				"The configured Total Flink Memory (%s) is less than the configured Off-heap Memory (%s).",
				totalFlinkMemorySize.toHumanReadableString(),
				offHeapMemorySize.toHumanReadableString());
		}
		MemorySize derivedJvmHeapMemorySize = totalFlinkMemorySize.subtract(offHeapMemorySize);
		return new JmFlinkInternalMemory(derivedJvmHeapMemorySize, offHeapMemorySize);
	}

	@Override
	protected JobManagerProcessSpec createMemoryProcessSpec(
			Configuration config,
			JmFlinkInternalMemory internalMemory,
			JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
		verifyJvmHeapSize(internalMemory.getJvmHeap());
		verifyJobStoreCacheSize(config, internalMemory.getJvmHeap());
		return new JobManagerProcessSpec(
			internalMemory.getJvmHeap(),
			internalMemory.getOffHeapMemory(),
			jvmMetaspaceAndOverhead.getMetaspace(),
			jvmMetaspaceAndOverhead.getOverhead());
	}

	@Override
	protected JobManagerProcessSpec failIfMandatoryOptionsNotConfigured() {
		throw new IllegalConfigurationException(String.format(
			"Either JVM Heap Memory size (%s) or Total Flink Memory size (%s), or Total Process Memory size (%s) " +
				"need to be configured explicitly.",
			JobManagerOptions.JVM_HEAP_MEMORY.key(),
			JobManagerOptions.TOTAL_FLINK_MEMORY.key(),
			JobManagerOptions.TOTAL_PROCESS_MEMORY.key()));
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

	public static JobManagerProcessSpec processSpecFromConfig(Configuration config) {
		return INSTANCE.memoryProcessSpecFromConfig(config);
	}

	public static Configuration getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
			Configuration configuration,
			ConfigOption<MemorySize> configOption) {
		return INSTANCE.getConfWithLegacyHeapSizeMappedToNewConfigOption(configuration, configOption);
	}
}
