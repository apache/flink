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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration for partition request client/server.
 */
public class PartitionRequestNettyConfig extends NettyConfig {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestNettyConfig.class);

	// - Config keys ----------------------------------------------------------

	public static final String NUM_ARENAS = "taskmanager.net.num-arenas";

	public static final String NUM_THREADS_SERVER = "taskmanager.net.server.numThreads";

	public static final String NUM_THREADS_CLIENT = "taskmanager.net.client.numThreads";

	public static final String CONNECT_BACKLOG = "taskmanager.net.server.backlog";

	public static final String CLIENT_CONNECT_TIMEOUT_SECONDS = "taskmanager.net.client.connectTimeoutSec";

	public static final String SEND_RECEIVE_BUFFER_SIZE = "taskmanager.net.sendReceiveBufferSize";

	public static final String TRANSPORT_TYPE = "taskmanager.net.transport";

	// ------------------------------------------------------------------------

	final static String SERVER_THREAD_GROUP_NAME = "Flink Netty Server";

	final static String CLIENT_THREAD_GROUP_NAME = "Flink Netty Client";

	private final int memorySegmentSize;

	private final int numberOfSlots;

	private final Configuration config; // optional configuration

	public PartitionRequestNettyConfig(
		InetAddress serverAddress,
		int serverPort,
		int memorySegmentSize,
		int numberOfSlots,
		Configuration config) {

		super(serverAddress, serverPort, config);

		checkArgument(memorySegmentSize > 0, "Invalid memory segment size.");
		this.memorySegmentSize = memorySegmentSize;

		checkArgument(numberOfSlots > 0, "Number of slots");
		this.numberOfSlots = numberOfSlots;

		this.config = checkNotNull(config);

		LOG.info(this.toString());
	}

	public int getNumberOfSlots() {
		return numberOfSlots;
	}

	public int getMemorySegmentSize() {
		return memorySegmentSize;
	}

	// ------------------------------------------------------------------------
	// Setters
	// ------------------------------------------------------------------------

	public NettyConfig setServerConnectBacklog(int connectBacklog) {
		checkArgument(connectBacklog >= 0);
		config.setInteger(CONNECT_BACKLOG, connectBacklog);

		return this;
	}

	public NettyConfig setServerNumThreads(int numThreads) {
		checkArgument(numThreads >= 0);
		config.setInteger(NUM_THREADS_SERVER, numThreads);

		return this;
	}

	public NettyConfig setClientNumThreads(int numThreads) {
		checkArgument(numThreads >= 0);
		config.setInteger(NUM_THREADS_CLIENT, numThreads);

		return this;
	}

	public NettyConfig setClientConnectTimeoutSeconds(int connectTimeoutSeconds) {
		checkArgument(connectTimeoutSeconds >= 0);
		config.setInteger(CLIENT_CONNECT_TIMEOUT_SECONDS, connectTimeoutSeconds);

		return this;
	}

	public NettyConfig setSendAndReceiveBufferSize(int bufferSize) {
		checkArgument(bufferSize >= 0);
		config.setInteger(SEND_RECEIVE_BUFFER_SIZE, bufferSize);

		return this;
	}

	public NettyConfig setTransportType(String transport) {
		if (transport.equals("nio") || transport.equals("epoll") || transport.equals("auto")) {
			config.setString(TRANSPORT_TYPE, transport);
		}
		else {
			throw new IllegalArgumentException("Unknown transport type.");
		}

		return this;
	}

	public NettyConfig setSSLEnabled(boolean enabled) {
		config.setBoolean(ConfigConstants.TASK_MANAGER_DATA_SSL_ENABLED, enabled);

		return this;
	}

	// ------------------------------------------------------------------------
	// Getters
	// ------------------------------------------------------------------------

	@Override
	public Tuple2<Integer,Integer> getServerWriteBufferWatermark() {
		return new Tuple2<>(getMemorySegmentSize() + 1, 2 * getMemorySegmentSize());
	}

	@Override
	public int getServerConnectBacklog() {
		return config.getInteger(CONNECT_BACKLOG, super.getServerConnectBacklog());
	}

	@Override
	public int getNumberOfArenas() {
		// default: number of slots
		return config.getInteger(NUM_ARENAS, numberOfSlots);
	}

	@Override
	public int getServerNumThreads() {
		// default: number of task slots
		return config.getInteger(NUM_THREADS_SERVER, numberOfSlots);
	}

	@Override
	public String getServerThreadGroupName() {
		// Add the server port number to the name in order to distinguish
		// multiple servers running on the same host.
		return PartitionRequestNettyConfig.SERVER_THREAD_GROUP_NAME + " (" + getServerPort() + ")";
	}

	@Override
	public int getClientNumThreads() {
		// default: number of task slots
		return config.getInteger(NUM_THREADS_CLIENT, numberOfSlots);
	}

	@Override
	public int getClientConnectTimeoutSeconds() {
		return config.getInteger(CLIENT_CONNECT_TIMEOUT_SECONDS, super.getClientConnectTimeoutSeconds());
	}

	@Override
	public String getClientThreadGroupName() {
		// Add the server port number to the name in order to distinguish
		// multiple clients running on the same host.
		return PartitionRequestNettyConfig.CLIENT_THREAD_GROUP_NAME + " (" + getServerPort() + ")";
	}

	@Override
	public int getSendAndReceiveBufferSize() {
		return config.getInteger(SEND_RECEIVE_BUFFER_SIZE, super.getSendAndReceiveBufferSize());
	}

	@Override
	public TransportType getTransportType() {
		String transport = config.getString(TRANSPORT_TYPE, "nio");
		return parseTransportType(transport);
	}

	@Override
	public boolean getSSLEnabled() {
		return config.getBoolean(ConfigConstants.TASK_MANAGER_DATA_SSL_ENABLED,
			ConfigConstants.DEFAULT_TASK_MANAGER_DATA_SSL_ENABLED)
			&& super.getSSLEnabled();
	}

	@Override
	public String toString() {
		String format = "NettyConfig [" +
			"server address: %s, " +
			"server port: %d, " +
			"memory segment size (bytes): %d, " +
			"transport type: %s, " +
			"number of server threads: %d (%s), " +
			"number of client threads: %d (%s), " +
			"server connect backlog: %d (%s), " +
			"client connect timeout (sec): %d, " +
			"send/receive buffer size (bytes): %d (%s)]";

		String def = "use Netty's default";
		String man = "manual";

		return String.format(format, getServerAddress(), getServerPort(), getMemorySegmentSize(),
			getTransportType(), getServerNumThreads(), getServerNumThreads() == 0 ? def : man,
			getClientNumThreads(), getClientNumThreads() == 0 ? def : man,
			getServerConnectBacklog(), getServerConnectBacklog() == 0 ? def : man,
			getClientConnectTimeoutSeconds(), getSendAndReceiveBufferSize(),
			getSendAndReceiveBufferSize() == 0 ? def : man);
	}
}
