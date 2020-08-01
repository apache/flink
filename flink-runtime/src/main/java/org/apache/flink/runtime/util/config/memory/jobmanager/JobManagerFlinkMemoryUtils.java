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

package org.apache.flink.runtime.util.config.memory.jobmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.FlinkMemoryUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FlinkMemoryUtils} for Job Manager.
 *
 * <p>The required fine-grained component is {@link JobManagerOptions#JVM_HEAP_MEMORY}.
 */
public class JobManagerFlinkMemoryUtils implements FlinkMemoryUtils<JobManagerFlinkMemory> {
	private static final Logger LOG = LoggerFactory.getLogger(JobManagerFlinkMemoryUtils.class);

	@Override
	public JobManagerFlinkMemory deriveFromRequiredFineGrainedOptions(Configuration config) {
		MemorySize jvmHeapMemorySize = ProcessMemoryUtils.getMemorySizeFromConfig(config, JobManagerOptions.JVM_HEAP_MEMORY);
		MemorySize offHeapMemorySize = ProcessMemoryUtils.getMemorySizeFromConfig(config, JobManagerOptions.OFF_HEAP_MEMORY);

		if (config.contains(JobManagerOptions.TOTAL_FLINK_MEMORY)) {
			// derive network memory from total flink memory, and check against network min/max
			MemorySize totalFlinkMemorySize = ProcessMemoryUtils.getMemorySizeFromConfig(config, JobManagerOptions.TOTAL_FLINK_MEMORY);
			if (config.contains(JobManagerOptions.OFF_HEAP_MEMORY)) {
				// off-heap memory is explicitly set by user
				sanityCheckTotalFlinkMemory(totalFlinkMemorySize, jvmHeapMemorySize, totalFlinkMemorySize);
			} else {
				// off-heap memory is not explicitly set by user, derive it from Total Flink Memory and JVM Heap
				offHeapMemorySize = deriveOffHeapMemory(jvmHeapMemorySize, totalFlinkMemorySize, offHeapMemorySize);
			}
		}

		return createJobManagerFlinkMemory(jvmHeapMemorySize, offHeapMemorySize);
	}

	private static void sanityCheckTotalFlinkMemory(
			MemorySize totalFlinkMemorySize,
			MemorySize jvmHeapMemorySize,
			MemorySize offHeapMemorySize) {
		MemorySize derivedTotalFlinkMemorySize = jvmHeapMemorySize.add(offHeapMemorySize);
		if (derivedTotalFlinkMemorySize.getBytes() != totalFlinkMemorySize.getBytes()) {
			throw new IllegalConfigurationException(String.format(
				"Sum of the configured JVM Heap Memory (%s) and the configured Off-heap Memory (%s) " +
					"does not match the configured Total Flink Memory (%s). Please, make the configuration consistent " +
					"or configure only one option: either JVM Heap or Total Flink Memory.",
				jvmHeapMemorySize.toHumanReadableString(),
				offHeapMemorySize.toHumanReadableString(),
				totalFlinkMemorySize.toHumanReadableString()));
		}
	}

	private static MemorySize deriveOffHeapMemory(
			MemorySize jvmHeapMemorySize,
			MemorySize totalFlinkMemorySize,
			MemorySize defaultOffHeapMemorySize) {
		if (totalFlinkMemorySize.getBytes() < jvmHeapMemorySize.getBytes()) {
			throw new IllegalConfigurationException(String.format(
				"The configured JVM Heap Memory (%s) exceeds the configured Total Flink Memory (%s). " +
					"Please, make the configuration consistent or configure only one option: either JVM Heap " +
					"or Total Flink Memory.",
				jvmHeapMemorySize.toHumanReadableString(),
				totalFlinkMemorySize.toHumanReadableString()));
		}
		MemorySize offHeapMemorySize = totalFlinkMemorySize.subtract(jvmHeapMemorySize);
		if (offHeapMemorySize.getBytes() != defaultOffHeapMemorySize.getBytes()) {
			LOG.info(
				"The Off-Heap Memory size ({}) is derived the configured Total Flink Memory size ({}) minus " +
					"the configured JVM Heap Memory size ({}). The default Off-Heap Memory size ({}) is ignored.",
				offHeapMemorySize.toHumanReadableString(),
				totalFlinkMemorySize.toHumanReadableString(),
				jvmHeapMemorySize.toHumanReadableString(),
				defaultOffHeapMemorySize.toHumanReadableString());
		}
		return offHeapMemorySize;
	}

	@Override
	public JobManagerFlinkMemory deriveFromTotalFlinkMemory(Configuration config, MemorySize totalFlinkMemorySize) {
		MemorySize offHeapMemorySize = ProcessMemoryUtils.getMemorySizeFromConfig(config, JobManagerOptions.OFF_HEAP_MEMORY);
		if (totalFlinkMemorySize.compareTo(offHeapMemorySize) < 1) {
			throw new IllegalConfigurationException(
				"The configured Total Flink Memory (%s) is less than the configured Off-heap Memory (%s).",
				totalFlinkMemorySize.toHumanReadableString(),
				offHeapMemorySize.toHumanReadableString());
		}
		MemorySize derivedJvmHeapMemorySize = totalFlinkMemorySize.subtract(offHeapMemorySize);
		return createJobManagerFlinkMemory(derivedJvmHeapMemorySize, offHeapMemorySize);
	}

	private static JobManagerFlinkMemory createJobManagerFlinkMemory(
			MemorySize jvmHeap,
			MemorySize offHeapMemory) {
		verifyJvmHeapSize(jvmHeap);
		return new JobManagerFlinkMemory(jvmHeap, offHeapMemory);
	}

	private static void verifyJvmHeapSize(MemorySize jvmHeapSize) {
		if (jvmHeapSize.compareTo(JobManagerOptions.MIN_JVM_HEAP_SIZE) < 0) {
			LOG.warn(
				"The configured or derived JVM heap memory size ({}) is less than its recommended minimum value ({})",
				jvmHeapSize.toHumanReadableString(),
				JobManagerOptions.MIN_JVM_HEAP_SIZE.toHumanReadableString());
		}
	}
}
