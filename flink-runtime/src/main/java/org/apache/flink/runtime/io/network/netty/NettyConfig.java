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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.net.SSLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.net.InetAddress;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class NettyConfig {

	private static final Logger LOG = LoggerFactory.getLogger(NettyConfig.class);

	// - Config keys ----------------------------------------------------------

	public static final ConfigOption<Integer> NUM_ARENAS = ConfigOptions
			.key("taskmanager.network.netty.num-arenas")
			.defaultValue(-1)
			.withDeprecatedKeys("taskmanager.net.num-arenas");

	public static final ConfigOption<Integer> NUM_THREADS_SERVER = ConfigOptions
			.key("taskmanager.network.netty.server.numThreads")
			.defaultValue(-1)
			.withDeprecatedKeys("taskmanager.net.server.numThreads");

	public static final ConfigOption<Integer> NUM_THREADS_CLIENT = ConfigOptions
			.key("taskmanager.network.netty.client.numThreads")
			.defaultValue(-1)
			.withDeprecatedKeys("taskmanager.net.client.numThreads");

	public static final ConfigOption<Integer> CONNECT_BACKLOG = ConfigOptions
			.key("taskmanager.network.netty.server.backlog")
			.defaultValue(0) // default: 0 => Netty's default
			.withDeprecatedKeys("taskmanager.net.server.backlog");

	public static final ConfigOption<Integer> CLIENT_CONNECT_TIMEOUT_SECONDS = ConfigOptions
			.key("taskmanager.network.netty.client.connectTimeoutSec")
			.defaultValue(120) // default: 120s = 2min
			.withDeprecatedKeys("taskmanager.net.client.connectTimeoutSec");

	public static final ConfigOption<Integer> SEND_RECEIVE_BUFFER_SIZE = ConfigOptions
			.key("taskmanager.network.netty.sendReceiveBufferSize")
			.defaultValue(0) // default: 0 => Netty's default
			.withDeprecatedKeys("taskmanager.net.sendReceiveBufferSize");

	public static final ConfigOption<String> TRANSPORT_TYPE = ConfigOptions
			.key("taskmanager.network.netty.transport")
			.defaultValue("nio")
			.withDeprecatedKeys("taskmanager.net.transport");

	// ------------------------------------------------------------------------

	enum TransportType {
		NIO, EPOLL, AUTO
	}

	final static String SERVER_THREAD_GROUP_NAME = "Flink Netty Server";

	final static String CLIENT_THREAD_GROUP_NAME = "Flink Netty Client";

	private final InetAddress serverAddress;

	private final int serverPort;

	private final int memorySegmentSize;

	private final int numberOfSlots;

	private final Configuration config; // optional configuration

	public NettyConfig(
			InetAddress serverAddress,
			int serverPort,
			int memorySegmentSize,
			int numberOfSlots,
			Configuration config) {

		this.serverAddress = checkNotNull(serverAddress);

		checkArgument(serverPort >= 0 && serverPort <= 65536, "Invalid port number.");
		this.serverPort = serverPort;

		checkArgument(memorySegmentSize > 0, "Invalid memory segment size.");
		this.memorySegmentSize = memorySegmentSize;

		checkArgument(numberOfSlots > 0, "Number of slots");
		this.numberOfSlots = numberOfSlots;

		this.config = checkNotNull(config);

		LOG.info(this.toString());
	}

	InetAddress getServerAddress() {
		return serverAddress;
	}

	int getServerPort() {
		return serverPort;
	}

	int getMemorySegmentSize() {
		return memorySegmentSize;
	}

	public int getNumberOfSlots() {
		return numberOfSlots;
	}

	// ------------------------------------------------------------------------
	// Getters
	// ------------------------------------------------------------------------

	public int getServerConnectBacklog() {
		return config.getInteger(CONNECT_BACKLOG);
	}

	public int getNumberOfArenas() {
		// default: number of slots
		final int configValue = config.getInteger(NUM_ARENAS);
		return configValue == -1 ? numberOfSlots : configValue;
	}

	public int getServerNumThreads() {
		// default: number of task slots
		final int configValue = config.getInteger(NUM_THREADS_SERVER);
		return configValue == -1 ? numberOfSlots : configValue;
	}

	public int getClientNumThreads() {
		// default: number of task slots
		final int configValue = config.getInteger(NUM_THREADS_CLIENT);
		return configValue == -1 ? numberOfSlots : configValue;
	}

	public int getClientConnectTimeoutSeconds() {
		return config.getInteger(CLIENT_CONNECT_TIMEOUT_SECONDS);
	}

	public int getSendAndReceiveBufferSize() {
		return config.getInteger(SEND_RECEIVE_BUFFER_SIZE);
	}

	public TransportType getTransportType() {
		String transport = config.getString(TRANSPORT_TYPE);

		switch (transport) {
			case "nio":
				return TransportType.NIO;
			case "epoll":
				return TransportType.EPOLL;
			default:
				return TransportType.AUTO;
		}
	}

	public SSLContext createClientSSLContext() throws Exception {

		// Create SSL Context from config
		SSLContext clientSSLContext = null;
		if (getSSLEnabled()) {
			clientSSLContext = SSLUtils.createSSLClientContext(config);
		}

		return clientSSLContext;
	}

	public SSLContext createServerSSLContext() throws Exception {

		// Create SSL Context from config
		SSLContext serverSSLContext = null;
		if (getSSLEnabled()) {
			serverSSLContext = SSLUtils.createSSLServerContext(config);
		}

		return serverSSLContext;
	}

	public boolean getSSLEnabled() {
		return config.getBoolean(ConfigConstants.TASK_MANAGER_DATA_SSL_ENABLED,
				ConfigConstants.DEFAULT_TASK_MANAGER_DATA_SSL_ENABLED)
			&& SSLUtils.getSSLEnabled(config);
	}

	public void setSSLVerAndCipherSuites(SSLEngine engine) {
		SSLUtils.setSSLVerAndCipherSuites(engine, config);
	}

	public void setSSLVerifyHostname(SSLParameters sslParams) {
		SSLUtils.setSSLVerifyHostname(config, sslParams);
	}

	@Override
	public String toString() {
		String format = "NettyConfig [" +
				"server address: %s, " +
				"server port: %d, " +
				"ssl enabled: %s, " +
				"memory segment size (bytes): %d, " +
				"transport type: %s, " +
				"number of server threads: %d (%s), " +
				"number of client threads: %d (%s), " +
				"server connect backlog: %d (%s), " +
				"client connect timeout (sec): %d, " +
				"send/receive buffer size (bytes): %d (%s)]";

		String def = "use Netty's default";
		String man = "manual";

		return String.format(format, serverAddress, serverPort, getSSLEnabled() ? "true":"false",
				memorySegmentSize, getTransportType(), getServerNumThreads(),
				getServerNumThreads() == 0 ? def : man,
				getClientNumThreads(), getClientNumThreads() == 0 ? def : man,
				getServerConnectBacklog(), getServerConnectBacklog() == 0 ? def : man,
				getClientConnectTimeoutSeconds(), getSendAndReceiveBufferSize(),
				getSendAndReceiveBufferSize() == 0 ? def : man);
	}
}
