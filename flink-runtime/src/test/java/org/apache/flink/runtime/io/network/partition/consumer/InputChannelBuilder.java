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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;

import java.net.InetSocketAddress;

/**
 * Builder for various {@link InputChannel} types.
 */
public class InputChannelBuilder {
	public static final ConnectionID STUB_CONNECTION_ID =
		new ConnectionID(new InetSocketAddress("localhost", 5000), 0);

	private int channelIndex = 0;
	private ResultPartitionID partitionId = new ResultPartitionID();
	private ConnectionID connectionID = STUB_CONNECTION_ID;
	private ResultPartitionManager partitionManager = new ResultPartitionManager();
	private TaskEventPublisher taskEventPublisher = new TaskEventDispatcher();
	private ConnectionManager connectionManager = new LocalConnectionManager();
	private int initialBackoff = 0;
	private int maxBackoff = 0;
	private InputChannelMetrics metrics = InputChannelTestUtils.newUnregisteredInputChannelMetrics();
	private MemorySegmentProvider memorySegmentProvider = InputChannelTestUtils.StubMemorySegmentProvider.getInstance();

	public static InputChannelBuilder newBuilder() {
		return new InputChannelBuilder();
	}

	public InputChannelBuilder setChannelIndex(int channelIndex) {
		this.channelIndex = channelIndex;
		return this;
	}

	public InputChannelBuilder setPartitionId(ResultPartitionID partitionId) {
		this.partitionId = partitionId;
		return this;
	}

	public InputChannelBuilder setPartitionManager(ResultPartitionManager partitionManager) {
		this.partitionManager = partitionManager;
		return this;
	}

	InputChannelBuilder setTaskEventPublisher(TaskEventPublisher taskEventPublisher) {
		this.taskEventPublisher = taskEventPublisher;
		return this;
	}

	public InputChannelBuilder setConnectionManager(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
		return this;
	}

	public InputChannelBuilder setInitialBackoff(int initialBackoff) {
		this.initialBackoff = initialBackoff;
		return this;
	}

	public InputChannelBuilder setMaxBackoff(int maxBackoff) {
		this.maxBackoff = maxBackoff;
		return this;
	}

	public InputChannelBuilder setMetrics(InputChannelMetrics metrics) {
		this.metrics = metrics;
		return this;
	}

	public InputChannelBuilder setMemorySegmentProvider(MemorySegmentProvider memorySegmentProvider) {
		this.memorySegmentProvider = memorySegmentProvider;
		return this;
	}

	InputChannelBuilder setupFromNettyShuffleEnvironment(NettyShuffleEnvironment network) {
		this.partitionManager = network.getResultPartitionManager();
		this.connectionManager = network.getConnectionManager();
		this.initialBackoff = network.getConfiguration().partitionRequestInitialBackoff();
		this.maxBackoff = network.getConfiguration().partitionRequestMaxBackoff();
		this.memorySegmentProvider = network.getNetworkBufferPool();
		return this;
	}

	UnknownInputChannel buildUnknownAndSetToGate(SingleInputGate inputGate) {
		UnknownInputChannel channel = new UnknownInputChannel(
			inputGate,
			channelIndex,
			partitionId,
			partitionManager,
			taskEventPublisher,
			connectionManager,
			initialBackoff,
			maxBackoff,
			metrics,
			memorySegmentProvider);
		inputGate.setInputChannel(channel);
		return channel;
	}

	public LocalInputChannel buildLocalAndSetToGate(SingleInputGate inputGate) {
		LocalInputChannel channel = new LocalInputChannel(
			inputGate,
			channelIndex,
			partitionId,
			partitionManager,
			taskEventPublisher,
			initialBackoff,
			maxBackoff,
			metrics);
		inputGate.setInputChannel(channel);
		return channel;
	}

	public RemoteInputChannel buildRemoteAndSetToGate(SingleInputGate inputGate) {
		RemoteInputChannel channel = new RemoteInputChannel(
			inputGate,
			channelIndex,
			partitionId,
			connectionID,
			connectionManager,
			initialBackoff,
			maxBackoff,
			metrics,
			memorySegmentProvider);
		inputGate.setInputChannel(channel);
		return channel;
	}
}
