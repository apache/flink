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
import org.apache.flink.runtime.util.MemoryProcessUtils;
import org.apache.flink.runtime.util.MemoryProcessUtils.JvmMetaspaceAndOverhead;
import org.apache.flink.runtime.util.MemoryProcessUtils.JvmMetaspaceAndOverheadOptions;
import org.apache.flink.runtime.util.MemoryProcessUtils.LegacyHeapOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.util.MemoryProcessUtils.deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory;
import static org.apache.flink.runtime.util.MemoryProcessUtils.deriveJvmMetaspaceAndOverheadWithTotalProcessMemory;
import static org.apache.flink.runtime.util.MemoryProcessUtils.getMemorySizeFromConfig;

/**
 * JobManager utils to calculate {@link JobManagerProcessSpec} and JVM args.
 */
public final class JobManagerProcessUtils {
	private static final Logger LOG = LoggerFactory.getLogger(JobManagerProcessUtils.class);

	static final JvmMetaspaceAndOverheadOptions JM_JVM_METASPACE_AND_OVERHEAD_OPTIONS =
		new JvmMetaspaceAndOverheadOptions(
			JobManagerOptions.TOTAL_PROCESS_MEMORY,
			JobManagerOptions.JVM_METASPACE,
			JobManagerOptions.JVM_OVERHEAD_MIN,
			JobManagerOptions.JVM_OVERHEAD_MAX,
			JobManagerOptions.JVM_OVERHEAD_FRACTION
		);

	@SuppressWarnings("deprecation")
	static final LegacyHeapOptions JM_LEGACY_HEAP_OPTIONS =
		new LegacyHeapOptions(
			"FLINK_JM_HEAP",
			JobManagerOptions.JOB_MANAGER_HEAP_MEMORY,
			JobManagerOptions.JOB_MANAGER_HEAP_MEMORY_MB
		);

	public static JobManagerProcessSpec processSpecFromConfig(Configuration config) {
		if (config.contains(JobManagerOptions.JVM_HEAP_MEMORY)) {
			// jvm heap memory is configured, use these to derive total Flink and process memory
			return deriveProcessSpecWithExplicitJvmHeap(config);
		} else if (config.contains(JobManagerOptions.TOTAL_FLINK_MEMORY)) {
			// jvm heap memory is not configured, total Flink memory is configured,
			// derive from total flink memory
			return deriveProcessSpecWithTotalFlinkMemory(config);
		} else if (config.contains(JobManagerOptions.TOTAL_PROCESS_MEMORY)) {
			// total Flink memory is not configured, total process memory is configured,
			// derive from total process memory
			return deriveProcessSpecWithTotalProcessMemory(config);
		}

		throw new IllegalConfigurationException(String.format(
			"Either JVM Heap Memory size (%s) or Total Flink Memory size (%s), or Total Process Memory size (%s) " +
				"need to be configured explicitly.",
			JobManagerOptions.JVM_HEAP_MEMORY.key(),
			JobManagerOptions.TOTAL_FLINK_MEMORY.key(),
			JobManagerOptions.TOTAL_PROCESS_MEMORY.key()));
	}

	private static JobManagerProcessSpec deriveProcessSpecWithExplicitJvmHeap(Configuration config) {
		// derive flink internal memory from explicitly configure jvm heap memory size

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

		JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(
			config,
			derivedTotalFlinkMemorySize,
			JM_JVM_METASPACE_AND_OVERHEAD_OPTIONS);

		return createJobManagerProcessSpec(
			config,
			new FlinkInternalMemory(jvmHeapMemorySize, offHeapMemorySize),
			jvmMetaspaceAndOverhead);
	}

	private static JobManagerProcessSpec deriveProcessSpecWithTotalFlinkMemory(Configuration config) {
		MemorySize totalFlinkMemorySize = getMemorySizeFromConfig(config, JobManagerOptions.TOTAL_FLINK_MEMORY);
		FlinkInternalMemory internalMemory = deriveInternalMemoryFromTotalFlinkMemory(config, totalFlinkMemorySize);
		JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(
			config,
			totalFlinkMemorySize,
			JM_JVM_METASPACE_AND_OVERHEAD_OPTIONS);

		return createJobManagerProcessSpec(config, internalMemory, jvmMetaspaceAndOverhead);
	}

	private static JobManagerProcessSpec deriveProcessSpecWithTotalProcessMemory(Configuration config) {
		JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead =
			deriveJvmMetaspaceAndOverheadWithTotalProcessMemory(config, JM_JVM_METASPACE_AND_OVERHEAD_OPTIONS);
		MemorySize totalProcessMemorySize = getMemorySizeFromConfig(config, JobManagerOptions.TOTAL_PROCESS_MEMORY);
		MemorySize totalFlinkMemorySize = totalProcessMemorySize.subtract(
			jvmMetaspaceAndOverhead.getTotalJvmMetaspaceAndOverheadSize());
		FlinkInternalMemory internalMemory = deriveInternalMemoryFromTotalFlinkMemory(config, totalFlinkMemorySize);
		return createJobManagerProcessSpec(config, internalMemory, jvmMetaspaceAndOverhead);
	}

	private static JobManagerProcessSpec createJobManagerProcessSpec(
			Configuration config,
			FlinkInternalMemory internalMemory,
			JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
		verifyJvmHeapSize(internalMemory.jvmHeap);
		verifyJobStoreCacheSize(config, internalMemory.jvmHeap);
		return new JobManagerProcessSpec(
			internalMemory.jvmHeap,
			internalMemory.offHeapMemory,
			jvmMetaspaceAndOverhead.getMetaspace(),
			jvmMetaspaceAndOverhead.getOverhead());
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

	private static FlinkInternalMemory deriveInternalMemoryFromTotalFlinkMemory(
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
		return new FlinkInternalMemory(derivedJvmHeapMemorySize, offHeapMemorySize);
	}

	static Configuration getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
			Configuration configuration,
			ConfigOption<MemorySize> configOption) {
		return MemoryProcessUtils.getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
			configuration,
			JM_LEGACY_HEAP_OPTIONS,
			configOption);
	}

	private static class FlinkInternalMemory {
		private final MemorySize jvmHeap;
		private final MemorySize offHeapMemory;

		private FlinkInternalMemory(MemorySize jvmHeap, MemorySize offHeapMemory) {
			this.jvmHeap = jvmHeap;
			this.offHeapMemory = offHeapMemory;
		}
	}

	private JobManagerProcessUtils() {
	}
}
