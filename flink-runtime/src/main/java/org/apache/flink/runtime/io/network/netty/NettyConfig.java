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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.net.SSLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.net.InetAddress;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class NettyConfig {

	private static final Logger LOG = LoggerFactory.getLogger(NettyConfig.class);

	// ------------------------------------------------------------------------

	enum TransportType {
		NIO, EPOLL, AUTO
	}

	private final InetAddress serverAddress;

	private final int serverPort;

	private final Configuration config;

	public NettyConfig(
			InetAddress serverAddress,
			int serverPort,
			Configuration config) {

		this.serverAddress = checkNotNull(serverAddress);

		checkArgument(serverPort >= 0 && serverPort <= 65536, "Invalid port number.");
		this.serverPort = serverPort;

		this.config = checkNotNull(config);
	}

	public InetAddress getServerAddress() {
		return serverAddress;
	}

	public int getServerPort() {
		return serverPort;
	}

	// ------------------------------------------------------------------------
	// Getters
	// ------------------------------------------------------------------------

	public Tuple2<Integer,Integer> getServerWriteBufferWatermark() {
		// default: N/A
		return null;
	}

	public int getServerConnectBacklog() {
		// default: 0 => Netty's default
		return 0;
	}

	public int getNumberOfArenas() {
		return getServerNumThreads();
	}

	public abstract int getServerNumThreads();

	public abstract String getServerThreadGroupName();

	public abstract int getClientNumThreads();

	public int getClientConnectTimeoutSeconds() {
		// default: 120s = 2min
		return 120;
	}

	public abstract String getClientThreadGroupName();

	public int getSendAndReceiveBufferSize() {
		// default: 0 => Netty's default
		return 0;
	}

	public TransportType getTransportType() {
		return TransportType.AUTO;
	}

	protected static TransportType parseTransportType(String transport) {
		if (transport.equals("nio")) {
			return TransportType.NIO;
		}
		else if (transport.equals("epoll")) {
			return TransportType.EPOLL;
		}
		else {
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
		return SSLUtils.getSSLEnabled(config);
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
				"transport type: %s, " +
				"number of server threads: %d (%s), " +
				"number of client threads: %d (%s), " +
				"server connect backlog: %d (%s), " +
				"client connect timeout (sec): %d, " +
				"send/receive buffer size (bytes): %d (%s)]";

		String def = "use Netty's default";
		String man = "manual";

		return String.format(format, serverAddress, serverPort, getSSLEnabled() ? "true" : "false",
				getTransportType(), getServerNumThreads(), getServerNumThreads() == 0 ? def : man,
				getClientNumThreads(), getClientNumThreads() == 0 ? def : man,
				getServerConnectBacklog(), getServerConnectBacklog() == 0 ? def : man,
				getClientConnectTimeoutSeconds(), getSendAndReceiveBufferSize(),
				getSendAndReceiveBufferSize() == 0 ? def : man);
	}
}
