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
 * The set of configuration options relating to TaskManager and Task settings.
 */
@PublicEvolving
public class TaskManagerOptions {

	// ------------------------------------------------------------------------
	//  General TaskManager Options
	// ------------------------------------------------------------------------

	/**
	 * How many heap memory a task manager will supply for user.
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_HEAP_MEMORY =
			key("taskmanager.heap.mb")
			.defaultValue(1024)
			.withDescription("How many heap memory (in megabytes) a task manager will supply for user, not including managed memory.");

	/**
	 * How many cores a task manager will supply for user.
	 */
	public static final ConfigOption<Double> TASK_MANAGER_CORE =
			key("taskmanager.cpu.core")
			.defaultValue(1.0)
			.withDescription("How many physical cpu cores a task manager will supply for user");

	/**
	 * How many direct memory a task manager will supply for user.
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_DIRECT_MEMORY =
			key("taskmanager.direct.memory.mb")
			.defaultValue(0)
			.withDescription("How many direct memory (in megabytes) a task manager will supply for user.");

	/**
	 * How many native memory a task manager will supply for user.
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_NATIVE_MEMORY =
			key("taskmanager.native.memory.mb")
			.defaultValue(0)
			.withDescription("How many native memory (in megabytes) a task manager will supply for user.");

	/**
	 * Extended resources will supply for user.
	 * Specified as resource-type:value pairs separated by commas.
	 * such as GPU:1,FPGA:1.
	 */
	public static final ConfigOption<String> TASK_MANAGER_EXTENDED_RESOURCES =
			key("taskmanager.extended.resources")
			.noDefaultValue()
			.withDescription("Extended resources will supply for user. " +
				"Specified as resource-type:value pairs separated by commas. such as GPU:1,FPGA:1.");

	/**
	 * The heap memory used for task manager process.
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_PROCESS_HEAP_MEMORY =
			key("taskmanager.process.heap.memory.mb")
			.defaultValue(128)
			.withDescription("The heap memory (in megabytes) used for task manager process.");

	/**
	 * The native memory used for task manager process.
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_PROCESS_NATIVE_MEMORY =
			key("taskmanager.process.native.memory.mb")
			.defaultValue(0)
			.withDescription("The native memory (in megabytes) used for task manager process.");

	/**
	 * The direct memory used for netty framework in the task manager process.
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_PROCESS_NETTY_MEMORY =
			key("taskmanager.process.netty.memory.mb")
			.defaultValue(64)
			.withDescription("The direct memory (in megabytes) used for netty framework in the task manager process.");

	/**
	 * Ratio of young generation for dynamic memory in task manager.
	 */
	public static final ConfigOption<Double> TASK_MANAGER_MEMORY_DYNAMIC_YOUNG_RATIO =
			key("taskmanager.jvm.memory.dynamic.young.ratio")
			.defaultValue(0.25)
			.withDescription("Ratio of young generation for dynamic memory in task manager.");

	/**
	 * Ratio of young generation for persistent memory in task manager.
	 */
	public static final ConfigOption<Double> TASK_MANAGER_MEMORY_PERSISTENT_YOUNG_RATIO =
			key("taskmanager.jvm.memory.persistent.young.ratio")
			.defaultValue(0.1)
			.withDescription("Ratio of young generation for persistent memory in task manager.");

	/**
	 * Cpu core limitation for a task manager, used to decide how many slots can be placed on a task manager.
	 */
	public static final ConfigOption<Double> TASK_MANAGER_MULTI_SLOTS_MAX_CORE =
			key("taskmanager.multi-slots.max.cpu.core")
			.defaultValue(1.0)
			.withDescription("Cpu core limitation, used to decide how many slots can be placed on a taskmanager.");

	/**
	 * Memory limitation, used to decide how many slots can be placed on a task manager.
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_MULTI_SLOTS_MAX_MEMORY =
			key("taskmanager.multi-slots.max.memory.mb")
			.defaultValue(32768)
			.withDescription("Memory (in megabytes) limitation, used to decide how many slots can be placed " +
				"on a taskmanager.");

	/**
	 * Extended resources limitation, used to decide how many slots can be placed on a task manager.
	 */
	public static final ConfigOption<String> TASK_MANAGER_MULTI_SLOTS_MAX_EXTENDED_RESOURCES =
			key("taskmanager.multi-slots.max.extended-resources")
			.defaultValue("")
			.withDescription("Extended resources limitation, used to decide how many slots can be placed " +
				"on a taskmanger. String format is like \"GPU=10,FPGA=12\".");

	/**
	 * Min cpu cores for a task manager.
	 */
	public static final ConfigOption<Double> TASK_MANAGER_MULTI_SLOTS_MIN_CORE =
			key("taskmanager.multi-slots.min.cpu.core")
			.defaultValue(1.0)
			.withDescription("Min cpu core for a taskmanager.");

	/**
	 * Min memory for a task manager.
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_MULTI_SLOTS_MIN_MEMORY =
			key("taskmanager.multi-slots.min.memory.mb")
			.defaultValue(1024)
			.withDescription("Min memory (in megabytes) for taskmanager.");

	/**
	 * The resource profile for slots in a task executor.
	 */
	public static final ConfigOption<String> TASK_MANAGER_RESOURCE_PROFILE_KEY =
			key("taskmanager.resourceProfile")
			.defaultValue("")
			.withDescription("The resource profile of a slot in a task executor.");

	/**
	 * The resource profile for all the slots in a task executor.
	 */
	public static final ConfigOption<String> TASK_MANAGER_TOTAL_RESOURCE_PROFILE_KEY =
			key("taskmanager.total.resourceProfile")
			.defaultValue("")
			.withDescription("The total resource profile of all the slots in a task executor.");

	/**
	 * Whether to kill the TaskManager when the task thread throws an OutOfMemoryError.
	 */
	public static final ConfigOption<Boolean> KILL_ON_OUT_OF_MEMORY =
			key("taskmanager.jvm-exit-on-oom")
			.defaultValue(false)
			.withDescription("Whether to kill the TaskManager when the task thread throws an OutOfMemoryError.");

	/**
	 * Whether the quarantine monitor for task managers shall be started. The quarantine monitor
	 * shuts down the actor system if it detects that it has quarantined another actor system
	 * or if it has been quarantined by another actor system.
	 */
	public static final ConfigOption<Boolean> EXIT_ON_FATAL_AKKA_ERROR =
			key("taskmanager.exit-on-fatal-akka-error")
			.defaultValue(false)
			.withDescription("Whether the quarantine monitor for task managers shall be started. The quarantine monitor" +
				" shuts down the actor system if it detects that it has quarantined another actor system" +
				" or if it has been quarantined by another actor system.");

	/**
	 * The config parameter defining the task manager's hostname.
	 */
	public static final ConfigOption<String> HOST =
		key("taskmanager.host")
			.noDefaultValue()
			.withDescription("The hostname of the network interface that the TaskManager binds to. By default, the" +
				" TaskManager searches for network interfaces that can connect to the JobManager and other TaskManagers." +
				" This option can be used to define a hostname if that strategy fails for some reason. Because" +
				" different TaskManagers need different values for this option, it usually is specified in an" +
				" additional non-shared TaskManager-specific config file.");

	/**
	 * The default network port range the task manager expects incoming IPC connections. The {@code "0"} means that
	 * the TaskManager searches for a free port.
	 */
	public static final ConfigOption<String> RPC_PORT =
		key("taskmanager.rpc.port")
			.defaultValue("0")
			.withDescription("The task manager’s IPC port. Accepts a list of ports (“50100,50101”), ranges" +
				" (“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid" +
				" collisions when multiple TaskManagers are running on the same machine.");

	/**
	 * The default network port the task manager expects to receive transfer envelopes on. The {@code 0} means that
	 * the TaskManager searches for a free port.
	 */
	public static final ConfigOption<Integer> DATA_PORT =
		key("taskmanager.data.port")
			.defaultValue(0)
			.withDescription("The task manager’s port used for data exchange operations.");

	/**
	 * Config parameter to override SSL support for taskmanager's data transport.
	 */
	public static final ConfigOption<Boolean> DATA_SSL_ENABLED =
		key("taskmanager.data.ssl.enabled")
			.defaultValue(true)
			.withDescription("Enable SSL support for the taskmanager data transport. This is applicable only when the" +
				" global ssl flag " + SecurityOptions.SSL_ENABLED.key() + " is set to true");

	/**
	 * The initial registration backoff between two consecutive registration attempts. The backoff
	 * is doubled for each new registration attempt until it reaches the maximum registration backoff.
	 */
	public static final ConfigOption<String> INITIAL_REGISTRATION_BACKOFF =
		key("taskmanager.registration.initial-backoff")
			.defaultValue("500 ms")
			.withDeprecatedKeys("taskmanager.initial-registration-pause")
			.withDescription("The initial registration backoff between two consecutive registration attempts. The backoff" +
				" is doubled for each new registration attempt until it reaches the maximum registration backoff.");

	/**
	 * The maximum registration backoff between two consecutive registration attempts.
	 */
	public static final ConfigOption<String> REGISTRATION_MAX_BACKOFF =
		key("taskmanager.registration.max-backoff")
			.defaultValue("30 s")
			.withDeprecatedKeys("taskmanager.max-registration-pause")
			.withDescription("The maximum registration backoff between two consecutive registration attempts. The max" +
				" registration backoff requires a time unit specifier (ms/s/min/h/d).");

	/**
	 * The backoff after a registration has been refused by the job manager before retrying to connect.
	 */
	public static final ConfigOption<String> REFUSED_REGISTRATION_BACKOFF =
		key("taskmanager.registration.refused-backoff")
			.defaultValue("10 s")
			.withDeprecatedKeys("taskmanager.refused-registration-pause")
			.withDescription("The backoff after a registration has been refused by the job manager before retrying to connect.");

	/**
	 * Defines the timeout it can take for the TaskManager registration. If the duration is
	 * exceeded without a successful registration, then the TaskManager terminates.
	 */
	public static final ConfigOption<String> REGISTRATION_TIMEOUT =
		key("taskmanager.registration.timeout")
			.defaultValue("5 min")
			.withDeprecatedKeys("taskmanager.maxRegistrationDuration")
			.withDescription("Defines the timeout for the TaskManager registration. If the duration is" +
				" exceeded without a successful registration, then the TaskManager terminates.");

	/**
	 * Defines the maximum time it can take for the TaskManager reconnection. If the duration is
	 * exceeded without a successful reconnection, then disassociate from JM.
	 */
	public static final ConfigOption<String> RECONNECTION_TIMEOUT =
			key("taskmanager.reconnection.timeout")
					.defaultValue("1 min")
					.withDescription("Defines the maximum time it can take for the TaskManager reconnection. If the duration is" +
							" exceeded without a successful reconnection, then disassociate from JM.");

	/**
	 * The config parameter defining the number of task slots of a task manager.
	 */
	public static final ConfigOption<Integer> NUM_TASK_SLOTS =
		key("taskmanager.numberOfTaskSlots")
			.defaultValue(1)
			.withDescription("The number of parallel operator or user function instances that a single TaskManager can" +
				" run. If this value is larger than 1, a single TaskManager takes multiple instances of a function or" +
				" operator. That way, the TaskManager can utilize multiple CPU cores, but at the same time, the" +
				" available memory is divided between the different operator or function instances. This value" +
				" is typically proportional to the number of physical CPU cores that the TaskManager's machine has" +
				" (e.g., equal to the number of cores, or half the number of cores).");

	public static final ConfigOption<Boolean> DEBUG_MEMORY_LOG =
		key("taskmanager.debug.memory.log")
			.defaultValue(false)
			.withDeprecatedKeys("taskmanager.debug.memory.startLogThread")
			.withDescription("Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.");

	public static final ConfigOption<Long> DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS =
		key("taskmanager.debug.memory.log-interval")
			.defaultValue(5000L)
			.withDeprecatedKeys("taskmanager.debug.memory.logIntervalMs")
			.withDescription("The interval (in ms) for the log thread to log the current memory usage.");

	public static final ConfigOption<Integer> TASK_MANAGER_CAPACITY_MEMORY_MB =
		key("taskmanager.capacity.memory.mb")
			.defaultValue(-1)
			.withDescription("The overall memory in MB that allocated to the task manager.");

	public static final ConfigOption<Double> TASK_MANAGER_CAPACITY_CPU_CORE =
		key("taskmanager.capacity.cpu.core")
			.defaultValue(-1.0)
			.withDescription("The overall cpu cores allocated to the task manager.");

	// ------------------------------------------------------------------------
	//  External Shuffle Options
	// ------------------------------------------------------------------------
	/**
	 * Currently we support two shuffle services for blocking result partition. The default
	 * 'TM' refers to internal task manager shuffle service, and the value 'YARN' indicates
	 * the external shuffle service deployed on the node manager of yarn.
	 */
	public static final ConfigOption<String> TASK_BLOCKING_SHUFFLE_TYPE =
		key("task.blocking.shuffle.type")
			.defaultValue("TM")
			.withDescription("The type of shuffle service used for blocking edge. Currently it can be configured to TM or YARN.");

	/**
	 * The type of disks that can be used for external result partition on this task manager.
	 */
	public static final ConfigOption<String> TASK_MANAGER_OUTPUT_LOCAL_DISK_TYPE =
		key("taskmanager.output.local-disk.type")
			.defaultValue("")
			.withDescription("The disk type preferred to write the shuffle data. If not specified, all the root " +
				"directories are feasible. If specified, only directories with the configured type are feasible.");

	/**
	 * The list of dirs to be used for the external result partitions on this task manager.
	 * This configuration should be computed from the hadoop configuration and should not be
	 * configured by users manually.
	 */
	public static final ConfigOption<String> TASK_MANAGER_OUTPUT_LOCAL_OUTPUT_DIRS =
		key("taskmanager.output.local-output-dirs")
			.defaultValue("")
			.withDescription("The available directories for the external shuffle service. It will be configured " +
				"automatically and should not be configured manually.");

	/**
	 * The memory to be allocated from MemoryManager for each external result partition.
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_OUTPUT_MEMORY_MB =
		key("taskmanager.output.memory.mb")
			.defaultValue(200)
			.withDescription("The write buffer size for each output in a task.");

	/**
	 * The maximum number of subpartitions can hash file writer supported.
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_OUTPUT_HASH_MAX_SUBPARTITIONS =
		key("taskmanager.output.hash.max-subpartitions")
			.defaultValue(200)
			.withDescription("The maximum number of subpartitions supported by the hash writer.");

	/**
	 * The maximum of file handles that can be merged at one time. And if
	 * taskmanager.output.merge.enable-async-merge is set to false, the number of
	 * final merged files is less than this merge factor.
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_OUTPUT_MERGE_FACTOR =
		key("taskmanager.output.merge.factor")
			.defaultValue(64)
			.withDescription("The maximum number of files to merge at once when using the merge writer.");

	/**
	 * Whether merge to one file or not.
	 */
	public static final ConfigOption<Boolean> TASK_MANAGER_OUTPUT_MERGE_TO_ONE_FILE =
		key("taskmanager.output.merge.merge-to-one-file")
			.defaultValue(true)
			.withDescription("Whether to merge to one file finally when using the merge writer. If not, the " +
				"merge stops once the number of files are less than taskmanager.output.merge.factor.");

	/**
	 * Whether enable async merging or not, this option only takes effect when merge
	 * writer is used.
	 */
	public static final ConfigOption<Boolean> TASK_MANAGER_OUTPUT_ENABLE_ASYNC_MERGE =
		key("taskmanager.output.merge.enable-async-merge")
			.defaultValue(false)
			.withDescription("Whether to start merge while writing has not been finished.");

	/**
	 * The maximum number of external subpartitions that can be requested at the same time.
	 * To support the input selection, there must be at least one input channel in each gate
	 * that is requested, so this value should be larger than or equal to the number of
	 * SingleInputGate. If the configured value is smaller than the number of inputs, then
	 * the larger one will be used. This config option only takes effect when external shuffle
	 * service is used.
	 */
	public static final ConfigOption<Integer> TASK_EXTERNAL_SHUFFLE_MAX_CONCURRENT_REQUESTS =
		key("task.external.shuffle.max-concurrent-requests")
			.defaultValue(2000)
			.withDescription("The maximum number of concurrent requests in the reduce-side tasks.");

	/**
	 * Number of network buffers to use for each external input channel.
	 *
	 * <p>This value should be large for external channels to avoid random reads in the shuffle service
	 */
	public static final ConfigOption<Integer> NETWORK_BUFFERS_PER_EXTERNAL_BLOCKING_CHANNEL =
		key("taskmanager.network.memory.buffers-per-external-blocking-channel")
			.defaultValue(16)
			.withDescription("The number of buffers available for each external blocking channel.");

	/**
	 * Number of extra network buffers for to use for each ingoing external gate (input gate).
	 * The non-positive value will be replaced with 2 * number of active input channel in the runtime.
	 */
	public static final ConfigOption<Integer> NETWORK_EXTRA_BUFFERS_PER_EXTERNAL_BLOCKING_GATE =
		key("taskmanager.network.memory.floating-buffers-per-external-blocking-gate")
			.defaultValue(0)
			.withDescription("taskmanager.network.memory.floating-buffers-per-external-blocking-gate");

	public static final ConfigOption<Boolean> TASK_EXTERNAL_SHUFFLE_ENABLE_COMPRESSION =
		key("task.external.shuffle.compression.enable")
			.defaultValue(true)
			.withDescription("Whether to enable compress shuffle data when using external shuffle.");

	public static final ConfigOption<String> TASK_EXTERNAL_SHUFFLE_COMPRESSION_CODEC =
		key("task.external.shuffle.compression.codec")
			.defaultValue("lz4")
			.withDescription("The codec to use when compress or decompress external shuffle data. " +
				"Currently supported codecs are lz4, bzip2, gzip. User can also implement " +
				"interface BlockCompressionFactory and set its class to specify other codecs.");

	public static final ConfigOption<Integer> TASK_EXTERNAL_SHUFFLE_COMPRESSION_BUFFER_SIZE =
		key("task.external.shuffle.compression.buffer-size")
			.defaultValue(64 * 1024)
			.withDescription("The max buffer size to compress external shuffle data.");

	/**
	 * The duration to retain a partition's data after it has been fully consumed, in seconds.
	 */
	public static final ConfigOption<Integer> TASK_EXTERNAL_SHUFFLE_CONSUMED_PARTITION_TTL_IN_SECONDS =
		key("task.external.shuffle.consumed-partition-ttl-in-seconds")
			.defaultValue(60 * 60)
			.withDescription("The time interval to delete the fully consumed shuffle data directories " +
				"since they become inactive.");

	/**
	 * The duration to retain a partition's data after its last consumption if it hasn't been fully consumed,
	 * in seconds.
	 */
	public static final ConfigOption<Integer> TASK_EXTERNAL_SHUFFLE_PARTIAL_CONSUMED_PARTITION_TTL_IN_SECONDS =
		key("task.external.shuffle.partial-consumed-partition-ttl-in-seconds")
			.defaultValue(60 * 60 * 12)
			.withDescription("The time interval to delete the partially consumed shuffle data directories " +
				"since they become inactive.");

	/**
	 * The duration to retain a partition's data after its last modified time
	 * if this partition is ready for consumption but hasn't been consumed yet, in seconds.
	 */
	public static final ConfigOption<Integer> TASK_EXTERNAL_SHUFFLE_UNCONSUMED_PARTITION_TTL_IN_SECONDS =
		key("task.external.shuffle.unconsumed-partition-ttl-in-seconds")
			.defaultValue(60 * 60 * 12)
			.withDescription("TThe time interval to delete the unconsumed shuffle data directories " +
				"since they are ready to consume.");

	/**
	 * The duration to retain a partition's data after its last modified time
	 * if this partition is unfinished and cannot be consumed, probably due to upstream write failure,
	 * in seconds.
	 */
	public static final ConfigOption<Integer> TASK_EXTERNAL_SHUFFLE_UNFINISHED_PARTITION_TTL_IN_SECONDS =
		key("task.external.shuffle.unfinished-partition-ttl-in-seconds")
			.defaultValue(60 * 60)
			.withDescription("The time interval to delete the writing shuffle data directories " +
				"since the last writing.");

	// ------------------------------------------------------------------------
	//  Managed Memory Options
	// ------------------------------------------------------------------------

	/**
	 * Size of memory buffers used by the network stack and the memory manager (in bytes).
	 */
	public static final ConfigOption<Integer> MEMORY_SEGMENT_SIZE =
			key("taskmanager.memory.segment-size")
			.defaultValue(32768)
			.withDescription("Size of memory buffers used by the network stack and the memory manager (in bytes).");

	/**
	 * Amount of memory to be allocated by the task manager's memory manager (in megabytes). If not
	 * set, a relative fraction will be allocated, as defined by {@link #MANAGED_MEMORY_FRACTION}.
	 */
	public static final ConfigOption<Long> MANAGED_MEMORY_SIZE =
			key("taskmanager.managed.memory.size")
			.defaultValue(-1L)
			.withDeprecatedKeys("taskmanager.memory.size")
			.withDescription("Amount of memory to be allocated by the task manager's memory manager (in megabytes)." +
				" If not set, a relative fraction will be allocated.");

	/**
	 * Fraction of free memory allocated by the memory manager if {@link #MANAGED_MEMORY_SIZE} is
	 * not set.
	 */
	public static final ConfigOption<Float> MANAGED_MEMORY_FRACTION =
			key("taskmanager.memory.fraction")
			.defaultValue(0.7f)
			.withDescription("The relative amount of memory (after subtracting the amount of memory used by network" +
				" buffers) that the task manager reserves for sorting, hash tables, and caching of intermediate results." +
				" For example, a value of `0.8` means that a task manager reserves 80% of its memory" +
				" for internal data buffers, leaving 20% of free memory for the task manager's heap for objects" +
				" created by user-defined functions. This parameter is only evaluated, if " + MANAGED_MEMORY_SIZE.key() +
				" is not set.");

	public static final ConfigOption<Long> FLOATING_MANAGED_MEMORY_SIZE =
			key("taskmanager.floating.memory.size")
			.defaultValue(0L);

	/**
	 * Memory allocation method (JVM heap or off-heap), used for managed memory of the TaskManager
	 * as well as the network buffers.
	 **/
	public static final ConfigOption<Boolean> MEMORY_OFF_HEAP =
			key("taskmanager.memory.off-heap")
			.defaultValue(false)
			.withDescription("Memory allocation method (JVM heap or off-heap), used for managed memory of the" +
				" TaskManager as well as the network buffers.");

	/**
	 * Whether TaskManager managed memory should be pre-allocated when the TaskManager is starting.
	 */
	public static final ConfigOption<Boolean> MANAGED_MEMORY_PRE_ALLOCATE =
			key("taskmanager.memory.preallocate")
			.defaultValue(false)
			.withDescription("Whether TaskManager managed memory should be pre-allocated when the TaskManager is starting.");

	// ------------------------------------------------------------------------
	//  Network Options
	// ------------------------------------------------------------------------

	/**
	 * Number of buffers used in the network stack. This defines the number of possible tasks and
	 * shuffles.
	 *
	 * @deprecated use {@link #NETWORK_BUFFERS_MEMORY_FRACTION}, {@link #NETWORK_BUFFERS_MEMORY_MIN},
	 * and {@link #NETWORK_BUFFERS_MEMORY_MAX} instead
	 */
	@Deprecated
	public static final ConfigOption<Integer> NETWORK_NUM_BUFFERS =
			key("taskmanager.network.numberOfBuffers")
			.defaultValue(2048);

	/**
	 * Fraction of JVM memory to use for network buffers.
	 */
	public static final ConfigOption<Float> NETWORK_BUFFERS_MEMORY_FRACTION =
			key("taskmanager.network.memory.fraction")
			.defaultValue(0.1f)
			.withDescription("Fraction of JVM memory to use for network buffers. This determines how many streaming" +
				" data exchange channels a TaskManager can have at the same time and how well buffered the channels" +
				" are. If a job is rejected or you get a warning that the system has not enough buffers available," +
				" increase this value or the min/max values below. Also note, that \"taskmanager.network.memory.min\"" +
				"` and \"taskmanager.network.memory.max\" may override this fraction.");

	/**
	 * Minimum memory size for network buffers (in bytes).
	 */
	public static final ConfigOption<Long> NETWORK_BUFFERS_MEMORY_MIN =
			key("taskmanager.network.memory.min")
			.defaultValue(64L << 20) // 64 MB
			.withDescription("Minimum memory size for network buffers (in bytes).");

	/**
	 * Maximum memory size for network buffers (in bytes).
	 */
	public static final ConfigOption<Long> NETWORK_BUFFERS_MEMORY_MAX =
			key("taskmanager.network.memory.max")
			.defaultValue(1024L << 20) // 1 GB
			.withDescription("Maximum memory size for network buffers (in bytes).");

	/**
	 * Number of network buffers to use for each outgoing/incoming channel (subpartition/input channel).
	 *
	 * <p>Reasoning: 1 buffer for in-flight data in the subpartition + 1 buffer for parallel serialization.
	 */
	public static final ConfigOption<Integer> NETWORK_BUFFERS_PER_CHANNEL =
			key("taskmanager.network.memory.buffers-per-channel")
			.defaultValue(2)
			.withDescription("Maximum number of network buffers to use for each outgoing/incoming channel (subpartition/input channel)." +
				"In credit-based flow control mode, this indicates how many credits are exclusive in each input channel. It should be" +
				" configured at least 2 for good performance. 1 buffer is for receiving in-flight data in the subpartition and 1 buffer is" +
				" for parallel serialization.");

	/**
	 * Number of network buffers to use for each outgoing channel (subpartition).
	 *
	 * <p>Reasoning: 1 buffer for in-flight data in the subpartition + 1 buffer for parallel serialization
	 */
	public static final ConfigOption<Integer> NETWORK_BUFFERS_PER_SUBPARTITION =
			key("taskmanager.network.memory.buffers-per-subpartition")
			.defaultValue(2);

	/**
	 * Number of extra network buffers to use for each outgoing/incoming gate (result partition/input gate).
	 */
	public static final ConfigOption<Integer> NETWORK_EXTRA_BUFFERS_PER_GATE =
			key("taskmanager.network.memory.floating-buffers-per-gate")
			.defaultValue(8)
			.withDescription("Number of extra network buffers to use for each outgoing/incoming gate (result partition/input gate)." +
				" In credit-based flow control mode, this indicates how many floating credits are shared among all the input channels." +
				" The floating buffers are distributed based on backlog (real-time output buffers in the subpartition) feedback, and can" +
				" help relieve back-pressure caused by unbalanced data distribution among the subpartitions. This value should be" +
				" increased in case of higher round trip times between nodes and/or larger number of machines in the cluster.");

	/**
	 * Minimum backoff for partition requests of input channels.
	 */
	public static final ConfigOption<Integer> NETWORK_REQUEST_BACKOFF_INITIAL =
			key("taskmanager.network.request-backoff.initial")
			.defaultValue(100)
			.withDeprecatedKeys("taskmanager.net.request-backoff.initial")
			.withDescription("Minimum backoff for partition requests of input channels.");

	/**
	 * Maximum backoff for partition requests of input channels.
	 */
	public static final ConfigOption<Integer> NETWORK_REQUEST_BACKOFF_MAX =
			key("taskmanager.network.request-backoff.max")
			.defaultValue(10000)
			.withDeprecatedKeys("taskmanager.net.request-backoff.max")
			.withDescription("Maximum backoff for partition requests of input channels.");

	/**
	 * Boolean flag to enable/disable more detailed metrics about inbound/outbound network queue
	 * lengths.
	 */
	public static final ConfigOption<Boolean> NETWORK_DETAILED_METRICS =
			key("taskmanager.network.detailed-metrics")
			.defaultValue(false)
			.withDescription("Boolean flag to enable/disable more detailed metrics about inbound/outbound network queue lengths.");

	/**
	 * Boolean flag to enable/disable network credit-based flow control.
	 *
	 * @deprecated Will be removed for Flink 1.6 when the old code will be dropped in favour of
	 * credit-based flow control.
	 */
	@Deprecated
	public static final ConfigOption<Boolean> NETWORK_CREDIT_MODEL =
			key("taskmanager.network.credit-model")
			.defaultValue(true)
			.withDeprecatedKeys("taskmanager.network.credit-based-flow-control.enabled")
			.withDescription("Boolean flag to enable/disable network credit-based flow control.");

	/** Boolean flag indicates whether to check partition producer state if the task requests a partition failed
	 * and wants to re-trigger the partition request. The task will re-trigger the partition request
	 * if the producer is healthy or fail otherwise. */
	public static final ConfigOption<Boolean> CHECK_PARTITION_PRODUCER_STATE =
		key("taskmanager.network.check-partition-producer-state")
			.defaultValue(false)
			.withDescription("Boolean flag indicates whether to check partition producer state if the task requests" +
				" a partition failed and wants to re-trigger the partition request. The task will re-trigger the" +
				" partition request if the producer is healthy or fail otherwise.");

	// ------------------------------------------------------------------------
	//  Task Options
	// ------------------------------------------------------------------------

	/**
	 * Time interval in milliseconds between two successive task cancellation
	 * attempts.
	 */
	public static final ConfigOption<Long> TASK_CANCELLATION_INTERVAL =
			key("task.cancellation.interval")
			.defaultValue(30000L)
			.withDeprecatedKeys("task.cancellation-interval")
			.withDescription("Time interval between two successive task cancellation attempts in milliseconds.");

	/**
	 * Timeout in milliseconds after which a task cancellation times out and
	 * leads to a fatal TaskManager error. A value of <code>0</code> deactivates
	 * the watch dog.
	 */
	public static final ConfigOption<Long> TASK_CANCELLATION_TIMEOUT =
			key("task.cancellation.timeout")
			.defaultValue(180000L)
			.withDescription("Timeout in milliseconds after which a task cancellation times out and" +
				" leads to a fatal TaskManager error. A value of 0 deactivates" +
				" the watch dog.");
	/**
	 * This configures how long we wait for the timers to finish all pending timer threads
	 * when the stream task is cancelled .
	 */
	public static final ConfigOption<Long> TASK_CANCELLATION_TIMEOUT_TIMERS = ConfigOptions
			.key("task.cancellation.timers.timeout")
			.defaultValue(7500L)
			.withDeprecatedKeys("timerservice.exceptional.shutdown.timeout");

	/**
	 * The maximum number of bytes that a checkpoint alignment may buffer.
	 * If the checkpoint alignment buffers more than the configured amount of
	 * data, the checkpoint is aborted (skipped).
	 *
	 * <p>The default value of {@code -1} indicates that there is no limit.
	 */
	public static final ConfigOption<Long> TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT =
			key("task.checkpoint.alignment.max-size")
			.defaultValue(-1L)
			.withDescription("The maximum number of bytes that a checkpoint alignment may buffer. If the checkpoint" +
				" alignment buffers more than the configured amount of data, the checkpoint is aborted (skipped)." +
				" A value of -1 indicates that there is no limit.");

	public static final ConfigOption<Integer> IO_MANAGER_BUFFERED_READ_SIZE =
			key("io.manager.buffered.read.size")
					.defaultValue(-1)
					.withDescription("The buffer size of io manager buffered read, -1 mean" +
							" not use buffered read, this will reduce random IO, but will" +
							" result in more than one copy.");

	public static final ConfigOption<Integer> IO_MANAGER_BUFFERED_WRITE_SIZE =
			key("io.manager.buffered.write.size")
					.defaultValue(-1)
					.withDescription("The buffer size of io manager buffered write, -1 mean" +
							" not use buffered write, this will reduce random IO, but will" +
							" result in more than one copy.");

	public static final ConfigOption<Integer> IO_MANAGER_ASYNC_NUM_READ_WRITE_THREAD =
		key("io.manager.async.num-read-write-thread")
			.defaultValue(-1)
			.withDescription("The number of async read write thread. " +
				"If not positive, it will be adjusted to max(1, number of temp dirs) for TM shuffle and max(2, 2 * number of disks) for YARN shuffle.");

	// ------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private TaskManagerOptions() {}
}
