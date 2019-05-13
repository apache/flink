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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;

/**
 * Builder for the {@link NetworkEnvironment}.
 */
public class NetworkEnvironmentBuilder {

	private int numNetworkBuffers = 1024;

	private int networkBufferSize = 32 * 1024;

	private int partitionRequestInitialBackoff = 0;

	private int partitionRequestMaxBackoff = 0;

	private int networkBuffersPerChannel = 2;

	private int floatingNetworkBuffersPerGate = 8;

	private boolean isCreditBased = true;

	private NettyConfig nettyConfig;

	private TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();

	public NetworkEnvironmentBuilder setNumNetworkBuffers(int numNetworkBuffers) {
		this.numNetworkBuffers = numNetworkBuffers;
		return this;
	}

	public NetworkEnvironmentBuilder setNetworkBufferSize(int networkBufferSize) {
		this.networkBufferSize = networkBufferSize;
		return this;
	}

	public NetworkEnvironmentBuilder setPartitionRequestInitialBackoff(int partitionRequestInitialBackoff) {
		this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
		return this;
	}

	public NetworkEnvironmentBuilder setPartitionRequestMaxBackoff(int partitionRequestMaxBackoff) {
		this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;
		return this;
	}

	public NetworkEnvironmentBuilder setNetworkBuffersPerChannel(int networkBuffersPerChannel) {
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		return this;
	}

	public NetworkEnvironmentBuilder setFloatingNetworkBuffersPerGate(int floatingNetworkBuffersPerGate) {
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		return this;
	}

	public NetworkEnvironmentBuilder setIsCreditBased(boolean isCreditBased) {
		this.isCreditBased = isCreditBased;
		return this;
	}

	public NetworkEnvironmentBuilder setNettyConfig(NettyConfig nettyConfig) {
		this.nettyConfig = nettyConfig;
		return this;
	}

	public NetworkEnvironmentBuilder setTaskEventDispatcher(TaskEventDispatcher taskEventDispatcher) {
		this.taskEventDispatcher = taskEventDispatcher;
		return this;
	}

	public NetworkEnvironment build() {
		return new NetworkEnvironment(
			new NetworkEnvironmentConfiguration(
				numNetworkBuffers,
				networkBufferSize,
				partitionRequestInitialBackoff,
				partitionRequestMaxBackoff,
				networkBuffersPerChannel,
				floatingNetworkBuffersPerGate,
				isCreditBased,
				nettyConfig),
			taskEventDispatcher);
	}
}
