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

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.NoOpResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;

import java.net.InetSocketAddress;

import static org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateTest.TestingResultPartitionManager;

/**
 * Builder for various {@link InputChannel} types.
 */
public class InputChannelBuilder {
	public static final ConnectionID STUB_CONNECTION_ID =
		new ConnectionID(new InetSocketAddress("localhost", 5000), 0);

	private int channelIndex = 0;
	private ResultPartitionID partitionId = new ResultPartitionID();
	private ConnectionID connectionID = STUB_CONNECTION_ID;
	private ResultPartitionManager partitionManager = new TestingResultPartitionManager(new NoOpResultSubpartitionView());
	private TaskEventPublisher taskEventPublisher = new TaskEventDispatcher();
	private ChannelStateWriter stateWriter = ChannelStateWriter.NO_OP;
	private ConnectionManager connectionManager = new TestingConnectionManager();
	private int initialBackoff = 0;
	private int maxBackoff = 0;
	private int networkBuffersPerChannel = 2;
	private InputChannelMetrics metrics = InputChannelTestUtils.newUnregisteredInputChannelMetrics();

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

	public InputChannelBuilder setNetworkBuffersPerChannel(int networkBuffersPerChannel) {
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		return this;
	}

	public InputChannelBuilder setMetrics(InputChannelMetrics metrics) {
		this.metrics = metrics;
		return this;
	}

	public InputChannelBuilder setStateWriter(ChannelStateWriter stateWriter) {
		this.stateWriter = stateWriter;
		return this;
	}

	public InputChannelBuilder setupFromNettyShuffleEnvironment(NettyShuffleEnvironment network) {
		this.partitionManager = network.getResultPartitionManager();
		this.connectionManager = network.getConnectionManager();
		this.initialBackoff = network.getConfiguration().partitionRequestInitialBackoff();
		this.maxBackoff = network.getConfiguration().partitionRequestMaxBackoff();
		this.networkBuffersPerChannel = network.getConfiguration().networkBuffersPerChannel();
		return this;
	}

	UnknownInputChannel buildUnknownChannel(SingleInputGate inputGate) {
		UnknownInputChannel channel = new UnknownInputChannel(
			inputGate,
			channelIndex,
			partitionId,
			partitionManager,
			taskEventPublisher,
			connectionManager,
			initialBackoff,
			maxBackoff,
			networkBuffersPerChannel,
			metrics);
		channel.setChannelStateWriter(stateWriter);
		return channel;
	}

	public LocalInputChannel buildLocalChannel(SingleInputGate inputGate) {
		return new LocalInputChannel(
			inputGate,
			channelIndex,
			partitionId,
			partitionManager,
			taskEventPublisher,
			initialBackoff,
			maxBackoff,
			metrics.getNumBytesInLocalCounter(),
			metrics.getNumBuffersInLocalCounter(),
			stateWriter);
	}

	public RemoteInputChannel buildRemoteChannel(SingleInputGate inputGate) {
		return new RemoteInputChannel(
			inputGate,
			channelIndex,
			partitionId,
			connectionID,
			connectionManager,
			initialBackoff,
			maxBackoff,
			networkBuffersPerChannel,
			metrics.getNumBytesInRemoteCounter(),
			metrics.getNumBuffersInRemoteCounter(),
			stateWriter);
	}

	public LocalRecoveredInputChannel buildLocalRecoveredChannel(SingleInputGate inputGate) {
		LocalRecoveredInputChannel channel = new LocalRecoveredInputChannel(
			inputGate,
			channelIndex,
			partitionId,
			partitionManager,
			taskEventPublisher,
			initialBackoff,
			maxBackoff,
			networkBuffersPerChannel,
			metrics);
		channel.setChannelStateWriter(stateWriter);
		return channel;
	}

	public RemoteRecoveredInputChannel buildRemoteRecoveredChannel(SingleInputGate inputGate) {
		RemoteRecoveredInputChannel channel = new RemoteRecoveredInputChannel(
			inputGate,
			channelIndex,
			partitionId,
			connectionID,
			connectionManager,
			initialBackoff,
			maxBackoff,
			networkBuffersPerChannel,
			metrics);
		channel.setChannelStateWriter(stateWriter);
		return channel;
	}
}
