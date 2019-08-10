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
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.annotation.docs.Documentation;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to network stack.
 */
@PublicEvolving
@ConfigGroups(groups = @ConfigGroup(name = "NetworkNetty", keyPrefix = "taskmanager.network.netty"))
public class NettyShuffleEnvironmentOptions {

	// ------------------------------------------------------------------------
	//  Network General Options
	// ------------------------------------------------------------------------

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
				" global flag for internal SSL (" + SecurityOptions.SSL_INTERNAL_ENABLED.key() + ") is set to true");

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
	 * Minimum memory size for network buffers.
	 */
	public static final ConfigOption<String> NETWORK_BUFFERS_MEMORY_MIN =
		key("taskmanager.network.memory.min")
			.defaultValue("64mb")
			.withDescription("Minimum memory size for network buffers.");

	/**
	 * Maximum memory size for network buffers.
	 */
	public static final ConfigOption<String> NETWORK_BUFFERS_MEMORY_MAX =
		key("taskmanager.network.memory.max")
			.defaultValue("1gb")
			.withDescription("Maximum memory size for network buffers.");

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
	 * The timeout for requesting exclusive buffers for each channel.
	 */
	@Documentation.ExcludeFromDocumentation("This option is purely implementation related, and may be removed as the implementation changes.")
	public static final ConfigOption<Long> NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS =
		key("taskmanager.network.memory.exclusive-buffers-request-timeout-ms")
			.defaultValue(30000L)
			.withDescription("The timeout for requesting exclusive buffers for each channel. Since the number of maximum buffers and " +
					"the number of required buffers is not the same for local buffer pools, there may be deadlock cases that the upstream" +
					"tasks have occupied all the buffers and the downstream tasks are waiting for the exclusive buffers. The timeout breaks" +
					"the tie by failing the request of exclusive buffers and ask users to increase the number of total buffers.");

	@Documentation.ExcludeFromDocumentation("This option is only used for testing at the moment.")
	public static final ConfigOption<String> NETWORK_BOUNDED_BLOCKING_SUBPARTITION_TYPE =
		key("taskmanager.network.bounded-blocking-subpartition-type")
			.defaultValue("auto")
			.withDescription("The bounded blocking subpartition type, either \"mmap\" or \"file\". The default \"auto\" means selecting the" +
					"property type automatically based on system memory architecture.");

	// ------------------------------------------------------------------------
	//  Netty Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> NUM_ARENAS =
		key("taskmanager.network.netty.num-arenas")
			.defaultValue(-1)
			.withDeprecatedKeys("taskmanager.net.num-arenas")
			.withDescription("The number of Netty arenas.");

	public static final ConfigOption<Integer> NUM_THREADS_SERVER =
		key("taskmanager.network.netty.server.numThreads")
			.defaultValue(-1)
			.withDeprecatedKeys("taskmanager.net.server.numThreads")
			.withDescription("The number of Netty server threads.");

	public static final ConfigOption<Integer> NUM_THREADS_CLIENT =
		key("taskmanager.network.netty.client.numThreads")
			.defaultValue(-1)
			.withDeprecatedKeys("taskmanager.net.client.numThreads")
			.withDescription("The number of Netty client threads.");

	public static final ConfigOption<Integer> CONNECT_BACKLOG =
		key("taskmanager.network.netty.server.backlog")
			.defaultValue(0) // default: 0 => Netty's default
			.withDeprecatedKeys("taskmanager.net.server.backlog")
			.withDescription("The netty server connection backlog.");

	public static final ConfigOption<Integer> CLIENT_CONNECT_TIMEOUT_SECONDS =
		key("taskmanager.network.netty.client.connectTimeoutSec")
			.defaultValue(120) // default: 120s = 2min
			.withDeprecatedKeys("taskmanager.net.client.connectTimeoutSec")
			.withDescription("The Netty client connection timeout.");

	public static final ConfigOption<Integer> SEND_RECEIVE_BUFFER_SIZE =
		key("taskmanager.network.netty.sendReceiveBufferSize")
			.defaultValue(0) // default: 0 => Netty's default
			.withDeprecatedKeys("taskmanager.net.sendReceiveBufferSize")
			.withDescription("The Netty send and receive buffer size. This defaults to the system buffer size" +
				" (cat /proc/sys/net/ipv4/tcp_[rw]mem) and is 4 MiB in modern Linux.");

	public static final ConfigOption<String> TRANSPORT_TYPE =
		key("taskmanager.network.netty.transport")
			.defaultValue("nio")
			.withDeprecatedKeys("taskmanager.net.transport")
			.withDescription("The Netty transport type, either \"nio\" or \"epoll\"");

	// ------------------------------------------------------------------------
	//  Partition Request Options
	// ------------------------------------------------------------------------

	/**
	 * Minimum backoff for partition requests of input channels.
	 */
	public static final ConfigOption<Integer> NETWORK_REQUEST_BACKOFF_INITIAL =
		key("taskmanager.network.request-backoff.initial")
			.defaultValue(100)
			.withDeprecatedKeys("taskmanager.net.request-backoff.initial")
			.withDescription("Minimum backoff in milliseconds for partition requests of input channels.");

	/**
	 * Maximum backoff for partition requests of input channels.
	 */
	public static final ConfigOption<Integer> NETWORK_REQUEST_BACKOFF_MAX =
		key("taskmanager.network.request-backoff.max")
			.defaultValue(10000)
			.withDeprecatedKeys("taskmanager.net.request-backoff.max")
			.withDescription("Maximum backoff in milliseconds for partition requests of input channels.");

	// ------------------------------------------------------------------------

	@Documentation.ExcludeFromDocumentation("dev use only; likely temporary")
	public static final ConfigOption<Boolean> FORCE_PARTITION_RELEASE_ON_CONSUMPTION =
		key("taskmanager.network.partition.force-release-on-consumption")
			.defaultValue(false);

	/** Not intended to be instantiated. */
	private NettyShuffleEnvironmentOptions() {}
}
