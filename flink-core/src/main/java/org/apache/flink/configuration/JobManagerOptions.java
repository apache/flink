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
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Configuration options for the JobManager.
 */
@PublicEvolving
public class JobManagerOptions {

	/**
	 * The config parameter defining the network address to connect to
	 * for communication with the job manager.
	 *
	 * <p>This value is only interpreted in setups where a single JobManager with static
	 * name or address exists (simple standalone setups, or container setups with dynamic
	 * service name resolution). It is not used in many high-availability setups, when a
	 * leader-election service (like ZooKeeper) is used to elect and discover the JobManager
	 * leader from potentially multiple standby JobManagers.
	 */
	public static final ConfigOption<String> ADDRESS =
		key("jobmanager.rpc.address")
		.noDefaultValue()
		.withDescription("The config parameter defining the network address to connect to" +
			" for communication with the job manager." +
			" This value is only interpreted in setups where a single JobManager with static" +
			" name or address exists (simple standalone setups, or container setups with dynamic" +
			" service name resolution). It is not used in many high-availability setups, when a" +
			" leader-election service (like ZooKeeper) is used to elect and discover the JobManager" +
			" leader from potentially multiple standby JobManagers.");

	/**
	 * The config parameter defining the network port to connect to
	 * for communication with the job manager.
	 *
	 * <p>Like {@link JobManagerOptions#ADDRESS}, this value is only interpreted in setups where
	 * a single JobManager with static name/address and port exists (simple standalone setups,
	 * or container setups with dynamic service name resolution).
	 * This config option is not used in many high-availability setups, when a
	 * leader-election service (like ZooKeeper) is used to elect and discover the JobManager
	 * leader from potentially multiple standby JobManagers.
	 */
	public static final ConfigOption<Integer> PORT =
		key("jobmanager.rpc.port")
		.defaultValue(6123)
		.withDescription("The config parameter defining the network port to connect to" +
			" for communication with the job manager." +
			" Like " + ADDRESS.key() + ", this value is only interpreted in setups where" +
			" a single JobManager with static name/address and port exists (simple standalone setups," +
			" or container setups with dynamic service name resolution)." +
			" This config option is not used in many high-availability setups, when a" +
			" leader-election service (like ZooKeeper) is used to elect and discover the JobManager" +
			" leader from potentially multiple standby JobManagers.");

	/**
	 * JVM heap size for the JobManager with memory size.
	 */
	@Documentation.CommonOption(position = Documentation.CommonOption.POSITION_MEMORY)
	public static final ConfigOption<String> JOB_MANAGER_HEAP_MEMORY =
		key("jobmanager.heap.size")
		.defaultValue("1024m")
		.withDescription("JVM heap size for the JobManager.");

	/**
	 * JVM heap size (in megabytes) for the JobManager.
	 * @deprecated use {@link #JOB_MANAGER_HEAP_MEMORY}
	 */
	@Deprecated
	public static final ConfigOption<Integer> JOB_MANAGER_HEAP_MEMORY_MB =
		key("jobmanager.heap.mb")
		.defaultValue(1024)
		.withDescription("JVM heap size (in megabytes) for the JobManager.");

	/**
	 * The maximum number of prior execution attempts kept in history.
	 */
	public static final ConfigOption<Integer> MAX_ATTEMPTS_HISTORY_SIZE =
		key("jobmanager.execution.attempts-history-size")
			.defaultValue(16)
			.withDeprecatedKeys("job-manager.max-attempts-history-size")
			.withDescription("The maximum number of prior execution attempts kept in history.");

	/**
	 * This option specifies the failover strategy, i.e. how the job computation recovers from task failures.
	 *
	 * <p>The option "individual" is intentionally not included for its known limitations.
	 * It only works when all tasks are not connected, in which case the "region"
	 * failover strategy would also restart failed tasks individually.
	 * The new "region" strategy supersedes "individual" strategy and should always work.
	 */
	public static final ConfigOption<String> EXECUTION_FAILOVER_STRATEGY =
		key("jobmanager.execution.failover-strategy")
			.defaultValue("full")
			.withDescription(Description.builder()
				.text("This option specifies how the job computation recovers from task failures. " +
					"Accepted values are:")
				.list(
					text("'full': Restarts all tasks to recover the job."),
					text("'region': Restarts all tasks that could be affected by the task failure. " +
						"More details can be found %s.",
						link(
							"../dev/task_failure_recovery.html#restart-pipelined-region-failover-strategy",
							"here"))
				).build());

	/**
	 * The location where the JobManager stores the archives of completed jobs.
	 */
	public static final ConfigOption<String> ARCHIVE_DIR =
		key("jobmanager.archive.fs.dir")
			.noDefaultValue()
			.withDescription("Dictionary for JobManager to store the archives of completed jobs.");

	/**
	 * The job store cache size in bytes which is used to keep completed
	 * jobs in memory.
	 */
	public static final ConfigOption<Long> JOB_STORE_CACHE_SIZE =
		key("jobstore.cache-size")
		.defaultValue(50L * 1024L * 1024L)
		.withDescription("The job store cache size in bytes which is used to keep completed jobs in memory.");

	/**
	 * The time in seconds after which a completed job expires and is purged from the job store.
	 */
	public static final ConfigOption<Long> JOB_STORE_EXPIRATION_TIME =
		key("jobstore.expiration-time")
		.defaultValue(60L * 60L)
		.withDescription("The time in seconds after which a completed job expires and is purged from the job store.");

	/**
	 * The max number of completed jobs that can be kept in the job store.
	 */
	public static final ConfigOption<Integer> JOB_STORE_MAX_CAPACITY =
		key("jobstore.max-capacity")
			.defaultValue(Integer.MAX_VALUE)
			.withDescription("The max number of completed jobs that can be kept in the job store.");

	/**
	 * The timeout in milliseconds for requesting a slot from Slot Pool.
	 */
	public static final ConfigOption<Long> SLOT_REQUEST_TIMEOUT =
		key("slot.request.timeout")
		.defaultValue(5L * 60L * 1000L)
		.withDescription("The timeout in milliseconds for requesting a slot from Slot Pool.");

	/**
	 * The timeout in milliseconds for a idle slot in Slot Pool.
	 */
	public static final ConfigOption<Long> SLOT_IDLE_TIMEOUT =
		key("slot.idle.timeout")
			// default matches heartbeat.timeout so that sticky allocation is not lost on timeouts for local recovery
			.defaultValue(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT.defaultValue())
			.withDescription("The timeout in milliseconds for a idle slot in Slot Pool.");
	/**
	 * Config parameter determining the scheduler implementation.
	 */
	@Documentation.ExcludeFromDocumentation("SchedulerNG is still in development.")
	public static final ConfigOption<String> SCHEDULER =
		key("jobmanager.scheduler")
			.stringType()
			.defaultValue("ng")
			.withDescription(Description.builder()
				.text("Determines which scheduler implementation is used to schedule tasks. Accepted values are:")
				.list(
					text("'legacy': legacy scheduler"),
					text("'ng': new generation scheduler"))
				.build());
	/**
	 * Config parameter controlling whether partitions should already be released during the job execution.
	 */
	@Documentation.ExcludeFromDocumentation("User normally should not be expected to deactivate this feature. " +
		"We aim at removing this flag eventually.")
	public static final ConfigOption<Boolean> PARTITION_RELEASE_DURING_JOB_EXECUTION =
		key("jobmanager.partition.release-during-job-execution")
			.defaultValue(true)
			.withDescription("Controls whether partitions should already be released during the job execution.");

	// ---------------------------------------------------------------------------------------------

	private JobManagerOptions() {
		throw new IllegalAccessError();
	}
}
