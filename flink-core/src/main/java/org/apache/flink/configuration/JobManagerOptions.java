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

import static org.apache.flink.configuration.ConfigOptions.key;

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
	 * JVM heap size (in megabytes) for the JobManager.
	 */
	public static final ConfigOption<Integer> JOB_MANAGER_HEAP_MEMORY =
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
	 * The strategy to handle task failures.
	 * 'full' failover strategy will restart all tasks in the job.
	 * 'region' failover strategy will restart the tasks in the same region with the failed task.
	 * Regions are PIPELINED connected task groups in a job.
	 */
	public static final ConfigOption<String> EXECUTION_FAILOVER_STRATEGY =
		key("jobmanager.execution.failover-strategy")
			.defaultValue("full")
			.withDescription("The strategy to handle task failures. " +
				"'full' failover strategy will restart all tasks in the job. " +
				"'region' failover strategy will restart the tasks in the same region with the failed task. " +
				"Regions are PIPELINED connected task groups in a job.");

	/**
	 * The maximum number that a region can attempt to restart before triggering job failures.
	 */
	public static final ConfigOption<Integer> EXECUTION_FAILOVER_STRATEGY_REGION_MAX_ATTEMPTS =
		key("jobmanager.execution.failover-strategy.region.attempts")
			.defaultValue(100)
			.withDescription("The maximum number that a region can attempt to restart before triggering job failures. " +
				"This only works with 'region' failover strategy.");

	/**
	 * The class name of the graph manager plugin.
	 */
	public static final ConfigOption<String> GRAPH_MANAGER_PLUGIN =
		key("jobmanager.execution.graph-manager-plugin")
			.noDefaultValue()
			.withDescription("The class name of the graph manager plugin.");

	/**
	 * This option specifies the interval in order to trigger a resource manager reconnection if the connection
	 * to the resource manager has been lost.
	 *
	 * <p>This option is only intended for internal use.
	 */
	public static final ConfigOption<Long> RESOURCE_MANAGER_RECONNECT_INTERVAL =
		key("jobmanager.resourcemanager.reconnect-interval")
		.defaultValue(2000L)
		.withDescription("This option specifies the interval in order to trigger a resource manager reconnection if the connection" +
			" to the resource manager has been lost. This option is only intended for internal use.");

	/**
	 * The location where the JobManager stores the archives of completed jobs.
	 */
	public static final ConfigOption<String> ARCHIVE_DIR =
		key("jobmanager.archive.fs.dir")
			.noDefaultValue();

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

	public static final ConfigOption<Long> UPDATE_PARTITION_INFO_SEND_INTERVAL =
		key("jobmanager.update-partition-info.send-interval")
		.defaultValue(10L)
		.withDescription("The interval of send update-partition-info message.");

	public static final ConfigOption<Long> SLOT_REQUEST_TIMEOUT =
		key("slot.request.timeout")
		.defaultValue(5L * 60L * 1000L)
		.withDescription("The timeout in milliseconds for requesting a slot from Slot Pool.");

	public static final ConfigOption<Long> SLOT_IDLE_TIMEOUT =
		key("slot.idle.timeout")
			// default matches heartbeat.timeout so that sticky allocation is not lost on timeouts for local recovery
			.defaultValue(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT.defaultValue())
			.withDescription("The timeout in milliseconds for a idle slot in Slot Pool.");

	public static final ConfigOption<Boolean> SLOT_ENABLE_SHARED_SLOT =
		key("slot.enable-shared-slot")
			.defaultValue(true)
			.withDescription("Whether to enable slot sharing group when allocating slots in Slot Pool.");

	public static final ConfigOption<Long> JOB_RECONCILE_TIMEOUT =
		key("jobmanager.failover.reconcile-timeout")
			.defaultValue(60L)
			.withDescription("The timeout for job master to reconcile with task executors for recovering the execution status.");

	public static final ConfigOption<String> OPERATION_LOG_STORE =
		key("jobmanager.failover.operation-log-store")
			.defaultValue("none")
			.withDescription("The operation log store type for job master failover.");

	public static final ConfigOption<Integer> OPLOG_FLUSH_INTERVAL =
			key("jobmanager.failover.operation-log-flush-interval")
					.defaultValue(3000)
					.withDescription("The operation log store flush interval in ms.");

	// ---------------------------------------------------------------------------------------------

	private JobManagerOptions() {
		throw new IllegalAccessError();
	}
}
