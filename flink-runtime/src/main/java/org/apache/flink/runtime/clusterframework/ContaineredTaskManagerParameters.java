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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * This class describes the basic parameters for launching a TaskManager process.
 */
public class ContaineredTaskManagerParameters implements java.io.Serializable {

	private static final long serialVersionUID = -3096987654278064670L;

	/** Total container cpu cores. */
	private final double totalContainerCpuCore;

	/** Total container memory, in bytes. */
	private final long totalContainerMemoryMB;

	/** Heap size to be used for the Java process. */
	private final long taskManagerHeapSizeMB;

	/** Young generation size for heap */
	private int youngMemoryMB;

	/** Direct memory limit for the Java process. */
	private final long taskManagerDirectMemoryLimitMB;

	/** The number of slots per TaskManager. */
	private final int numSlots;

	/** Environment variables to add to the Java process. */
	private final HashMap<String, String> taskManagerEnv;

	public ContaineredTaskManagerParameters(
			long totalContainerMemoryMB,
			long taskManagerHeapSizeMB,
			long taskManagerDirectMemoryLimitMB,
			int numSlots,
			HashMap<String, String> taskManagerEnv) {

		this(totalContainerMemoryMB, taskManagerHeapSizeMB, taskManagerDirectMemoryLimitMB, numSlots, taskManagerEnv, 0);
	}

	public ContaineredTaskManagerParameters(
			long totalContainerMemoryMB,
			long taskManagerHeapSizeMB,
			long taskManagerDirectMemoryLimitMB,
			int numSlots,
			HashMap<String, String> taskManagerEnv,
			int youngMemoryMB) {

		this(totalContainerMemoryMB, taskManagerHeapSizeMB, taskManagerDirectMemoryLimitMB, numSlots, taskManagerEnv, youngMemoryMB, -1.0);
	}

	public ContaineredTaskManagerParameters(
		long totalContainerMemoryMB,
		long taskManagerHeapSizeMB,
		long taskManagerDirectMemoryLimitMB,
		int numSlots,
		HashMap<String, String> taskManagerEnv,
		int youngMemoryMB,
		double totalContainerCpuCore) {

		this.totalContainerMemoryMB = totalContainerMemoryMB;
		this.taskManagerHeapSizeMB = taskManagerHeapSizeMB;
		this.taskManagerDirectMemoryLimitMB = taskManagerDirectMemoryLimitMB;
		this.numSlots = numSlots;
		this.taskManagerEnv = taskManagerEnv;
		this.youngMemoryMB = youngMemoryMB;
		this.totalContainerCpuCore = totalContainerCpuCore;

	}

	// ------------------------------------------------------------------------

	public long taskManagerTotalMemoryMB() {
		return totalContainerMemoryMB;
	}

	public long taskManagerHeapSizeMB() {
		return taskManagerHeapSizeMB;
	}

	public int getYoungMemoryMB() {
		return youngMemoryMB;
	}

	public long taskManagerDirectMemoryLimitMB() {
		return taskManagerDirectMemoryLimitMB;
	}

	public int numSlots() {
		return numSlots;
	}

	public Map<String, String> taskManagerEnv() {
		return taskManagerEnv;
	}

	public double taskManagerTotalCpuCore() {
		return totalContainerCpuCore;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "TaskManagerParameters {" +
			"totalContainerMemory=" + totalContainerMemoryMB +
			", taskManagerHeapSize=" + taskManagerHeapSizeMB +
			", youngMemoryMB=" + youngMemoryMB +
			", taskManagerDirectMemoryLimit=" + taskManagerDirectMemoryLimitMB +
			", numSlots=" + numSlots +
			", taskManagerEnv=" + taskManagerEnv +
			'}';
	}

	// ------------------------------------------------------------------------
	//  Factory
	// ------------------------------------------------------------------------

	/**
	 * Calcuate cutoff memory size used by container, it will throw an {@link IllegalArgumentException}
	 * if the config is invalid or return the cutoff value if valid.
	 *
	 * @param config The Flink configuration.
	 * @param containerMemoryMB The size of the complete container, in megabytes.
	 *
	 * @return cutoff memory size used by container.
	 */
	public static long calculateCutoffMB(Configuration config, long containerMemoryMB) {
		Preconditions.checkArgument(containerMemoryMB > 0);

		// (1) check cutoff ratio
		final float memoryCutoffRatio = config.getFloat(
			ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO);

		if (memoryCutoffRatio >= 1 || memoryCutoffRatio <= 0) {
			throw new IllegalArgumentException("The configuration value '"
				+ ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO.key() + "' must be between 0 and 1. Value given="
				+ memoryCutoffRatio);
		}

		// (2) check min cutoff value
		final int minCutoff = config.getInteger(
			ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN);

		if (minCutoff >= containerMemoryMB) {
			throw new IllegalArgumentException("The configuration value '"
				+ ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN.key() + "'='" + minCutoff
				+ "' is larger than the total container memory " + containerMemoryMB);
		}

		// (3) check between heap and off-heap
		long cutoff = (long) (containerMemoryMB * memoryCutoffRatio);
		if (cutoff < minCutoff) {
			cutoff = minCutoff;
		}
		return cutoff;
	}

	/**
	 * Computes the parameters to be used to start a TaskManager Java process.
	 *
	 * @param config The Flink configuration.
	 * @param containerMemoryMB The size of the complete container, in megabytes.
	 * @return The parameters to start the TaskManager processes with.
	 */
	public static ContaineredTaskManagerParameters create(
			Configuration config,
			long containerMemoryMB,
			int numSlots) {
		// (1) try to compute how much memory used by container
		final long cutoffMB = calculateCutoffMB(config, containerMemoryMB);

		// (2) split the remaining Java memory between heap and off-heap
		final long heapSizeMB = TaskManagerServices.calculateHeapSizeMB(containerMemoryMB - cutoffMB, config);
		// use the cut-off memory for off-heap (that was its intention)
		final long offHeapSizeMB = containerMemoryMB - heapSizeMB;

		return create(config,
			containerMemoryMB, (int)heapSizeMB, (int)offHeapSizeMB, numSlots, 0);
	}

	public static ContaineredTaskManagerParameters create(
			Configuration config,
			long containerMemorySizeMB,
			int heapMemorySizeMB,
			int directMemorySizeMB,
			int numSlots,
			int youngMemorySizeMB) {

		return create(config, containerMemorySizeMB, heapMemorySizeMB, directMemorySizeMB, numSlots, youngMemorySizeMB, -1.0);
	}

	public static ContaineredTaskManagerParameters create(
		Configuration config,
		long containerMemorySizeMB,
		int heapMemorySizeMB,
		int directMemorySizeMB,
		int numSlots,
		int youngMemorySizeMB,
		double containerCpuCore) {

		// obtain the additional environment variables from the configuration
		final HashMap<String, String> envVars = new HashMap<>();
		final String prefix = ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX;
		for (String key : config.keySet()) {
			if (key.startsWith(prefix) && key.length() > prefix.length()) {
				// remove prefix
				String envVarKey = key.substring(prefix.length());
				envVars.put(envVarKey, config.getString(key, null));
			}
		}

		return new ContaineredTaskManagerParameters(
			containerMemorySizeMB,
			heapMemorySizeMB,
			directMemorySizeMB,
			numSlots,
			envVars,
			youngMemorySizeMB,
			containerCpuCore);
	}
}
