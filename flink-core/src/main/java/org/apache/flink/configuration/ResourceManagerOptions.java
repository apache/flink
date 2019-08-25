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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.description.Description;

/**
 * The set of configuration options relating to the ResourceManager.
 */
@PublicEvolving
public class ResourceManagerOptions {

	/**
	 * Timeout for jobs which don't have a job manager as leader assigned.
	 */
	public static final ConfigOption<String> JOB_TIMEOUT = ConfigOptions
		.key("resourcemanager.job.timeout")
		.defaultValue("5 minutes")
		.withDescription("Timeout for jobs which don't have a job manager as leader assigned.");

	/**
	 * The number of resource managers start.
	 */
	public static final ConfigOption<Integer> LOCAL_NUMBER_RESOURCE_MANAGER = ConfigOptions
		.key("local.number-resourcemanager")
		.defaultValue(1)
		.withDescription("The number of resource managers start.");

	/**
	 * Defines the network port to connect to for communication with the resource manager.
	 * By default, the port of the JobManager, because the same ActorSystem is used. Its not
	 * possible to use this configuration key to define port ranges.
	 */
	public static final ConfigOption<Integer> IPC_PORT = ConfigOptions
		.key("resourcemanager.rpc.port")
		.defaultValue(0)
		.withDescription("Defines the network port to connect to for communication with the resource manager. By" +
			" default, the port of the JobManager, because the same ActorSystem is used." +
			" Its not possible to use this configuration key to define port ranges.");

	/**
	 * Percentage of heap space to remove from containers (YARN / Mesos), to compensate
	 * for other JVM memory usage.
	 */
	public static final ConfigOption<Float> CONTAINERIZED_HEAP_CUTOFF_RATIO = ConfigOptions
		.key("containerized.heap-cutoff-ratio")
		.defaultValue(0.25f)
		.withDeprecatedKeys("yarn.heap-cutoff-ratio")
		.withDescription("Percentage of heap space to remove from containers (YARN / Mesos), to compensate" +
			" for other JVM memory usage.");

	/**
	 * Minimum amount of heap memory to remove in containers, as a safety margin.
	 */
	public static final ConfigOption<Integer> CONTAINERIZED_HEAP_CUTOFF_MIN = ConfigOptions
		.key("containerized.heap-cutoff-min")
		.defaultValue(600)
		.withDeprecatedKeys("yarn.heap-cutoff-min")
		.withDescription("Minimum amount of heap memory to remove in containers, as a safety margin.");

	/**
	 * The timeout for a slot request to be discarded, in milliseconds.
	 * @deprecated Use {@link JobManagerOptions#SLOT_REQUEST_TIMEOUT}.
	 */
	@Deprecated
	public static final ConfigOption<Long> SLOT_REQUEST_TIMEOUT = ConfigOptions
		.key("slotmanager.request-timeout")
		.defaultValue(-1L)
		.withDescription("The timeout for a slot request to be discarded.");

	/**
	 * Time in milliseconds of the start-up period of a standalone cluster.
	 * During this time, resource manager of the standalone cluster expects new task executors to be registered, and
	 * will not fail slot requests that can not be satisfied by any current registered slots.
	 * After this time, it will fail pending and new coming requests immediately that can not be satisfied by registered
	 * slots.
	 * If not set, {@link #SLOT_REQUEST_TIMEOUT} will be used by default.
	 */
	public static final ConfigOption<Long> STANDALONE_CLUSTER_STARTUP_PERIOD_TIME = ConfigOptions
		.key("resourcemanager.standalone.start-up-time")
		.defaultValue(-1L)
		.withDescription("Time in milliseconds of the start-up period of a standalone cluster. During this time, "
			+ "resource manager of the standalone cluster expects new task executors to be registered, and will not "
			+ "fail slot requests that can not be satisfied by any current registered slots. After this time, it will "
			+ "fail pending and new coming requests immediately that can not be satisfied by registered slots. If not "
			+ "set, 'slotmanager.request-timeout' will be used by default.");

	/**
	 * The timeout for an idle task manager to be released, in milliseconds.
	 * @deprecated Use {@link #TASK_MANAGER_TIMEOUT}.
	 */
	@Deprecated
	public static final ConfigOption<Long> SLOT_MANAGER_TASK_MANAGER_TIMEOUT = ConfigOptions
		.key("slotmanager.taskmanager-timeout")
		.defaultValue(30000L)
		.withDescription("The timeout for an idle task manager to be released.");

	/**
	 * The timeout for an idle task manager to be released, in milliseconds.
	 */
	public static final ConfigOption<Long> TASK_MANAGER_TIMEOUT = ConfigOptions
		.key("resourcemanager.taskmanager-timeout")
		.defaultValue(30000L)
		.withDeprecatedKeys(SLOT_MANAGER_TASK_MANAGER_TIMEOUT.key())
		.withDescription(Description.builder()
			.text("The timeout for an idle task manager to be released.")
			.build());

	/**
	 * Release task executor only when each produced result partition is either consumed or failed.
	 *
	 * <p>Currently, produced result partition is released when it fails or consumer sends close request
	 * to confirm successful end of consumption and to close the communication channel.
	 *
	 * @deprecated The default value should be reasonable enough in all cases, this option is to fallback to older behaviour
	 * which will be removed or refactored in future.
	 */
	@Deprecated
	public static final ConfigOption<Boolean> TASK_MANAGER_RELEASE_WHEN_RESULT_CONSUMED = ConfigOptions
		.key("resourcemanager.taskmanager-release.wait.result.consumed")
		.defaultValue(true)
		.withDescription(Description.builder()
			.text("Release task executor only when each produced result partition is either consumed or failed. " +
				"'True' is default. 'False' means that idle task executor release is not blocked " +
				"by receiver confirming consumption of result partition " +
				"and can happen right away after 'resourcemanager.taskmanager-timeout' has elapsed. " +
				"Setting this option to 'false' can speed up task executor release but can lead to unexpected failures " +
				"if end of consumption is slower than 'resourcemanager.taskmanager-timeout'.")
			.build());

	/**
	 * Prefix for passing custom environment variables to Flink's master process.
	 * For example for passing LD_LIBRARY_PATH as an env variable to the AppMaster, set:
	 * containerized.master.env.LD_LIBRARY_PATH: "/usr/lib/native"
	 * in the flink-conf.yaml.
	 */
	public static final String CONTAINERIZED_MASTER_ENV_PREFIX = "containerized.master.env.";

	/**
	 * Similar to the {@see CONTAINERIZED_MASTER_ENV_PREFIX}, this configuration prefix allows
	 * setting custom environment variables for the workers (TaskManagers).
	 */
	public static final String CONTAINERIZED_TASK_MANAGER_ENV_PREFIX = "containerized.taskmanager.env.";

	// ---------------------------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private ResourceManagerOptions() {}
}
