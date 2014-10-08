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

import io.netty.channel.Channel;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.netty.protocols.envelope.NettyEnvelopeProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.io.network.ChannelManager;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.NetworkConnectionManager;
import org.apache.flink.runtime.io.network.RemoteReceiver;

import java.io.IOException;
import java.net.InetAddress;

public class NettyConnectionManager implements NetworkConnectionManager {

	private static final Logger LOG = LoggerFactory.getLogger(NettyConnectionManager.class);

	private final NettyServer server;

	private final NettyClient client;

	private NettyConnectionRaceTrack connections;

	public NettyConnectionManager(InetAddress serverAddress, int serverPort, int bufferSize) {
		this(serverAddress, serverPort, bufferSize, new Configuration());
	}

	public NettyConnectionManager(InetAddress serverAddress, int serverPort, int bufferSize, Configuration config) {
		NettyConfig nettyConfig = new NettyConfig(serverAddress, serverPort, bufferSize, config);

		if (LOG.isDebugEnabled()) {
			LOG.debug(nettyConfig.toString());
		}

		this.server = new NettyServer(nettyConfig);
		this.client = new NettyClient(nettyConfig);
	}

	@Override
	public void start(ChannelManager channelManager) throws IOException {
		NettyProtocol protocol = new NettyEnvelopeProtocol(channelManager);

		this.client.init(protocol);

		this.server.init(protocol);

		this.connections = new NettyConnectionRaceTrack(client);
	}

	@Override
	public void enqueue(Envelope envelope, RemoteReceiver receiver) throws IOException {
		Channel channel = connections.connectOrGet(receiver);

		channel.pipeline().fireUserEventTriggered(envelope);
	}

	@Override
	public void shutdown() throws IOException {
		server.shutdown();
		client.shutdown();
	}
}

