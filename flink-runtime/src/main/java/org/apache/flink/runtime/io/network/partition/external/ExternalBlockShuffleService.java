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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyProtocol;
import org.apache.flink.runtime.io.network.netty.NettyServer;

import java.io.IOException;

public class ExternalBlockShuffleService {

	private final ExternalBlockShuffleServiceConfiguration shuffleServiceConfiguration;

	private final ExternalBlockResultPartitionManager partitionProvider;

	private final NettyServer server;

	private final NettyBufferPool bufferPool;

	public ExternalBlockShuffleService(Configuration configuration) throws Exception {
		this.shuffleServiceConfiguration = ExternalBlockShuffleServiceConfiguration.fromConfiguration(configuration);
		this.partitionProvider = new ExternalBlockResultPartitionManager(shuffleServiceConfiguration);

		NettyConfig nettyConfig = shuffleServiceConfiguration.getNettyConfig();
		bufferPool = new NettyBufferPool(nettyConfig.getNumberOfArenas(), nettyConfig.getMaxOrder(), nettyConfig.getChunkSize());
		server = new NettyServer(nettyConfig);
	}

	public void start() throws IOException {
		TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();
		NettyProtocol protocol = new NettyProtocol(
			partitionProvider,
			taskEventDispatcher,
			true);

		server.init(protocol, bufferPool);
	}

	public void stop() {
		server.shutdown();
		partitionProvider.stop();
	}

	public void initializeApplication(String user, String appId) {
		partitionProvider.initializeApplication(user, appId);
	}

	public void stopApplication(String appId) {
		partitionProvider.stopApplication(appId);
	}
}
