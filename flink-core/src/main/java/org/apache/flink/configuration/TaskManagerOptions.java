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

	// @TODO Migrate 'taskmanager.*' config options from ConfigConstants

	/**
	 * JVM heap size (in megabytes) for the TaskManagers
	 */
	public static final ConfigOption<Integer> TASK_MANAGER_HEAP_MEMORY =
			key("taskmanager.heap.mb")
			.defaultValue(1024);

	/**
	 * Whether to kill the TaskManager when the task thread throws an OutOfMemoryError
	 */
	public static final ConfigOption<Boolean> KILL_ON_OUT_OF_MEMORY =
			key("taskmanager.jvm-exit-on-oom")
			.defaultValue(false);

	/**
	 * Whether the quarantine monitor for task managers shall be started. The quarantine monitor
	 * shuts down the actor system if it detects that it has quarantined another actor system
	 * or if it has been quarantined by another actor system.
	 */
	public static final ConfigOption<Boolean> EXIT_ON_FATAL_AKKA_ERROR =
			key("taskmanager.exit-on-fatal-akka-error")
			.defaultValue(false);

	/**
	 * The default network port range the task manager expects incoming IPC connections. The {@code "0"} means that
	 * the TaskManager searches for a free port.
	 */
	public static final ConfigOption<String> RPC_PORT = 
		key("taskmanager.rpc.port")
			.defaultValue("0");

	// ------------------------------------------------------------------------
	//  Managed Memory Options
	// ------------------------------------------------------------------------

	/**
	 * Size of memory buffers used by the network stack and the memory manager (in bytes).
	 */
	public static final ConfigOption<Integer> MEMORY_SEGMENT_SIZE =
			key("taskmanager.memory.segment-size")
			.defaultValue(32768);

	/**
	 * Amount of memory to be allocated by the task manager's memory manager (in megabytes). If not
	 * set, a relative fraction will be allocated, as defined by {@link #MANAGED_MEMORY_FRACTION}.
	 */
	public static final ConfigOption<Long> MANAGED_MEMORY_SIZE =
			key("taskmanager.memory.size")
			.defaultValue(-1L);

	/**
	 * Fraction of free memory allocated by the memory manager if {@link #MANAGED_MEMORY_SIZE} is
	 * not set.
	 */
	public static final ConfigOption<Float> MANAGED_MEMORY_FRACTION =
			key("taskmanager.memory.fraction")
			.defaultValue(0.7f);

	/**
	 * Memory allocation method (JVM heap or off-heap), used for managed memory of the TaskManager
	 * as well as the network buffers.
	 **/
	public static final ConfigOption<Boolean> MEMORY_OFF_HEAP =
			key("taskmanager.memory.off-heap")
			.defaultValue(false);

	/**
	 * Whether TaskManager managed memory should be pre-allocated when the TaskManager is starting.
	 */
	public static final ConfigOption<Boolean> MANAGED_MEMORY_PRE_ALLOCATE =
			key("taskmanager.memory.preallocate")
			.defaultValue(false);

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
			.defaultValue(0.1f);

	/**
	 * Minimum memory size for network buffers (in bytes)
	 */
	public static final ConfigOption<Long> NETWORK_BUFFERS_MEMORY_MIN =
			key("taskmanager.network.memory.min")
			.defaultValue(64L << 20); // 64 MB

	/**
	 * Maximum memory size for network buffers (in bytes)
	 */
	public static final ConfigOption<Long> NETWORK_BUFFERS_MEMORY_MAX =
			key("taskmanager.network.memory.max")
			.defaultValue(1024L << 20); // 1 GB

	/**
	 * Number of network buffers to use for each outgoing/incoming channel (subpartition/input channel).
	 *
	 * Reasoning: 1 buffer for in-flight data in the subpartition + 1 buffer for parallel serialization
	 */
	public static final ConfigOption<Integer> NETWORK_BUFFERS_PER_CHANNEL =
			key("taskmanager.network.memory.buffers-per-channel")
			.defaultValue(2);

	/**
	 * Number of extra network buffers to use for each outgoing/incoming gate (result partition/input gate).
	 */
	public static final ConfigOption<Integer> NETWORK_EXTRA_BUFFERS_PER_GATE =
			key("taskmanager.network.memory.floating-buffers-per-gate")
			.defaultValue(8);

	/**
	 * Minimum backoff for partition requests of input channels.
	 */
	public static final ConfigOption<Integer> NETWORK_REQUEST_BACKOFF_INITIAL =
			key("taskmanager.network.request-backoff.initial")
			.defaultValue(100)
			.withDeprecatedKeys("taskmanager.net.request-backoff.initial");

	/**
	 * Maximum backoff for partition requests of input channels.
	 */
	public static final ConfigOption<Integer> NETWORK_REQUEST_BACKOFF_MAX =
			key("taskmanager.network.request-backoff.max")
			.defaultValue(10000)
			.withDeprecatedKeys("taskmanager.net.request-backoff.max");

	/**
	 * Boolean flag to enable/disable more detailed metrics about inbound/outbound network queue
	 * lengths.
	 */
	public static final ConfigOption<Boolean> NETWORK_DETAILED_METRICS =
			key("taskmanager.network.detailed-metrics")
			.defaultValue(false);

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
			.withDeprecatedKeys("task.cancellation-interval");

	/**
	 * Timeout in milliseconds after which a task cancellation times out and
	 * leads to a fatal TaskManager error. A value of <code>0</code> deactivates
	 * the watch dog.
	 */
	public static final ConfigOption<Long> TASK_CANCELLATION_TIMEOUT =
			key("task.cancellation.timeout")
			.defaultValue(180000L);

	/**
	 * The maximum number of bytes that a checkpoint alignment may buffer.
	 * If the checkpoint alignment buffers more than the configured amount of
	 * data, the checkpoint is aborted (skipped).
	 *
	 * <p>The default value of {@code -1} indicates that there is no limit.
	 */
	public static final ConfigOption<Long> TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT =
			key("task.checkpoint.alignment.max-size")
			.defaultValue(-1L);

	// ------------------------------------------------------------------------

	/** Not intended to be instantiated */
	private TaskManagerOptions() {}
}
