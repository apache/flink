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

import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.RemoteAddress;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionProvider;

import java.io.IOException;

public class NettyConnectionManager implements ConnectionManager {

	private final NettyServer server;

	private final NettyClient client;

	private final PartitionRequestClientFactory partitionRequestClientFactory;

	public NettyConnectionManager(NettyConfig nettyConfig) {
		this.server = new NettyServer(nettyConfig);
		this.client = new NettyClient(nettyConfig);

		this.partitionRequestClientFactory = new PartitionRequestClientFactory(client);
	}

	@Override
	public void start(IntermediateResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher) throws IOException {
		PartitionRequestProtocol partitionRequestProtocol = new PartitionRequestProtocol(partitionProvider, taskEventDispatcher);

		client.init(partitionRequestProtocol);
		server.init(partitionRequestProtocol);
	}

	@Override
	public PartitionRequestClient createPartitionRequestClient(RemoteAddress remoteAddress) throws IOException {
		return partitionRequestClientFactory.createPartitionRequestClient(remoteAddress);
	}

	@Override
	public int getNumberOfActiveConnections() {
		return partitionRequestClientFactory.getNumberOfActiveClients();
	}

	@Override
	public void shutdown() {
		client.shutdown();
		server.shutdown();
	}
}

