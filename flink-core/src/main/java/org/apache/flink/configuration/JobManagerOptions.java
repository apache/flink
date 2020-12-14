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

	public static final MemorySize MIN_JVM_HEAP_SIZE = MemorySize.ofMebiBytes(128);

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
	@Documentation.Section({Documentation.Sections.COMMON_HOST_PORT, Documentation.Sections.ALL_JOB_MANAGER})
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
	 * The local address of the network interface that the job manager binds to.
	 */
	public static final ConfigOption<String> BIND_HOST =
		key("jobmanager.bind-host")
			.stringType()
			.noDefaultValue()
			.withDescription("The local address of the network interface that the job manager binds to. If not" +
				" configured, '0.0.0.0' will be used.");

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
	@Documentation.Section({Documentation.Sections.COMMON_HOST_PORT, Documentation.Sections.ALL_JOB_MANAGER})
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
	 * The local port that the job manager binds to.
	 */
	public static final ConfigOption<Integer> RPC_BIND_PORT =
		key("jobmanager.rpc.bind-port")
			.intType()
			.noDefaultValue()
			.withDescription("The local RPC port that the JobManager binds to. If not configured, the external port" +
				" (configured by '" + PORT.key() + "') will be used.");

	/**
	 * JVM heap size for the JobManager with memory size.
	 * @deprecated use {@link #TOTAL_FLINK_MEMORY} for standalone setups and {@link #TOTAL_PROCESS_MEMORY} for containerized setups.
	 */
	@Deprecated
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<MemorySize> JOB_MANAGER_HEAP_MEMORY =
		key("jobmanager.heap.size")
		.memoryType()
		.noDefaultValue()
		.withDescription("JVM heap size for the JobManager.");

	/**
	 * JVM heap size (in megabytes) for the JobManager.
	 * @deprecated use {@link #TOTAL_FLINK_MEMORY} for standalone setups and {@link #TOTAL_PROCESS_MEMORY} for containerized setups.
	 */
	@Deprecated
	public static final ConfigOption<Integer> JOB_MANAGER_HEAP_MEMORY_MB =
		key("jobmanager.heap.mb")
		.intType()
		.noDefaultValue()
		.withDescription("JVM heap size (in megabytes) for the JobManager.");

	/**
	 * Total Process Memory size for the JobManager.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> TOTAL_PROCESS_MEMORY =
		key("jobmanager.memory.process.size")
			.memoryType()
			.noDefaultValue()
			.withDescription("Total Process Memory size for the JobManager. This includes all the memory that a " +
				"JobManager JVM process consumes, consisting of Total Flink Memory, JVM Metaspace, and JVM Overhead. " +
				"In containerized setups, this should be set to the container memory. See also " +
				"'jobmanager.memory.flink.size' for Total Flink Memory size configuration.");

	/**
	 * Total Flink Memory size for the JobManager.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> TOTAL_FLINK_MEMORY =
		key("jobmanager.memory.flink.size")
			.memoryType()
			.noDefaultValue()
			.withDescription(String.format(
				"Total Flink Memory size for the JobManager. This includes all the " +
					"memory that a JobManager consumes, except for JVM Metaspace and JVM Overhead. It consists of " +
					"JVM Heap Memory and Off-heap Memory. See also '%s' for total process memory size configuration.",
				TOTAL_PROCESS_MEMORY.key()));

	/**
	 * JVM Heap Memory size for the JobManager.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> JVM_HEAP_MEMORY =
		key("jobmanager.memory.heap.size")
			.memoryType()
			.noDefaultValue()
			.withDescription("JVM Heap Memory size for JobManager. The minimum recommended JVM Heap size is " +
				MIN_JVM_HEAP_SIZE.toHumanReadableString() + '.');

	/**
	 * Off-heap Memory size for the JobManager.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> OFF_HEAP_MEMORY =
		key("jobmanager.memory.off-heap.size")
			.memoryType()
			.defaultValue(MemorySize.ofMebiBytes(128))
			.withDescription(Description
				.builder()
				.text(
					"Off-heap Memory size for JobManager. This option covers all off-heap memory usage including " +
						"direct and native memory allocation. The JVM direct memory limit of the JobManager process " +
						"(-XX:MaxDirectMemorySize) will be set to this value if the limit is enabled by " +
						"'jobmanager.memory.enable-jvm-direct-memory-limit'. ")
				.build());

	/**
	 * Off-heap Memory size for the JobManager.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<Boolean> JVM_DIRECT_MEMORY_LIMIT_ENABLED =
		key("jobmanager.memory.enable-jvm-direct-memory-limit")
			.booleanType()
			.defaultValue(false)
			.withDescription(Description
				.builder()
				.text(
					"Whether to enable the JVM direct memory limit of the JobManager process " +
						"(-XX:MaxDirectMemorySize). The limit will be set to the value of '%s' option. ",
					text(OFF_HEAP_MEMORY.key()))
				.build());

	/**
	 * JVM Metaspace Size for the JobManager.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> JVM_METASPACE =
		key("jobmanager.memory.jvm-metaspace.size")
			.memoryType()
			.defaultValue(MemorySize.ofMebiBytes(256))
			.withDescription("JVM Metaspace Size for the JobManager.");

	private static final String JVM_OVERHEAD_DESCRIPTION = "This is off-heap memory reserved for JVM " +
		"overhead, such as thread stack space, compile cache, etc. This includes native memory but not direct " +
		"memory, and will not be counted when Flink calculates JVM max direct memory size parameter. The size " +
		"of JVM Overhead is derived to make up the configured fraction of the Total Process Memory. If the " +
		"derived size is less or greater than the configured min or max size, the min or max size will be used. The " +
		"exact size of JVM Overhead can be explicitly specified by setting the min and max size to the same value.";

	/**
	 * Min JVM Overhead size for the JobManager.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> JVM_OVERHEAD_MIN =
		key("jobmanager.memory.jvm-overhead.min")
			.memoryType()
			.defaultValue(MemorySize.ofMebiBytes(192))
			.withDescription("Min JVM Overhead size for the JobManager. " + JVM_OVERHEAD_DESCRIPTION);

	/**
	 * Max JVM Overhead size for the TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> JVM_OVERHEAD_MAX =
		key("jobmanager.memory.jvm-overhead.max")
			.memoryType()
			.defaultValue(MemorySize.parse("1g"))
			.withDescription("Max JVM Overhead size for the JobManager. " + JVM_OVERHEAD_DESCRIPTION);

	/**
	 * Fraction of Total Process Memory to be reserved for JVM Overhead.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<Float> JVM_OVERHEAD_FRACTION =
		key("jobmanager.memory.jvm-overhead.fraction")
			.floatType()
			.defaultValue(0.1f)
			.withDescription("Fraction of Total Process Memory to be reserved for JVM Overhead. " + JVM_OVERHEAD_DESCRIPTION);

	/**
	 * The maximum number of prior execution attempts kept in history.
	 */
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<Integer> MAX_ATTEMPTS_HISTORY_SIZE =
		key("jobmanager.execution.attempts-history-size")
			.defaultValue(16)
			.withDeprecatedKeys("job-manager.max-attempts-history-size")
			.withDescription("The maximum number of prior execution attempts kept in history.");

	/**
	 * This option specifies the failover strategy, i.e. how the job computation recovers from task failures.
	 */
	@Documentation.Section({Documentation.Sections.ALL_JOB_MANAGER, Documentation.Sections.EXPERT_FAULT_TOLERANCE})
	public static final ConfigOption<String> EXECUTION_FAILOVER_STRATEGY =
		key("jobmanager.execution.failover-strategy")
			.stringType()
			.defaultValue("region")
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
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<String> ARCHIVE_DIR =
		key("jobmanager.archive.fs.dir")
			.noDefaultValue()
			.withDescription("Dictionary for JobManager to store the archives of completed jobs.");

	/**
	 * The job store cache size in bytes which is used to keep completed
	 * jobs in memory.
	 */
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<Long> JOB_STORE_CACHE_SIZE =
		key("jobstore.cache-size")
		.defaultValue(50L * 1024L * 1024L)
		.withDescription("The job store cache size in bytes which is used to keep completed jobs in memory.");

	/**
	 * The time in seconds after which a completed job expires and is purged from the job store.
	 */
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<Long> JOB_STORE_EXPIRATION_TIME =
		key("jobstore.expiration-time")
		.defaultValue(60L * 60L)
		.withDescription("The time in seconds after which a completed job expires and is purged from the job store.");

	/**
	 * The max number of completed jobs that can be kept in the job store.
	 */
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<Integer> JOB_STORE_MAX_CAPACITY =
		key("jobstore.max-capacity")
			.defaultValue(Integer.MAX_VALUE)
			.withDescription("The max number of completed jobs that can be kept in the job store.");

	/**
	 * Flag indicating whether JobManager would retrieve canonical host name of TaskManager during registration.
	 */
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<Boolean> RETRIEVE_TASK_MANAGER_HOSTNAME =
		key("jobmanager.retrieve-taskmanager-hostname")
			.defaultValue(true)
			.withDescription("Flag indicating whether JobManager would retrieve canonical "
							+ "host name of TaskManager during registration. "
							+ "If the option is set to \"false\", TaskManager registration with "
							+ "JobManager could be faster, since no reverse DNS lookup is performed. "
							+ "However, local input split assignment (such as for HDFS files) may be impacted.");

	/**
	 * The timeout in milliseconds for requesting a slot from Slot Pool.
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
	public static final ConfigOption<Long> SLOT_REQUEST_TIMEOUT =
		key("slot.request.timeout")
		.defaultValue(5L * 60L * 1000L)
		.withDescription("The timeout in milliseconds for requesting a slot from Slot Pool.");

	/**
	 * The timeout in milliseconds for a idle slot in Slot Pool.
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
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
