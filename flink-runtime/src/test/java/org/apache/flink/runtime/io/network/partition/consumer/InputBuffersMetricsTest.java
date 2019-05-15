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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.metrics.CreditBasedInputBuffersUsageGauge;
import org.apache.flink.runtime.io.network.metrics.ExclusiveBuffersUsageGauge;
import org.apache.flink.runtime.io.network.metrics.FloatingBuffersUsageGauge;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests the metrics for input buffers usage.
 */
public class InputBuffersMetricsTest extends TestLogger {

	@Test
	public void testCalculateTotalBuffersSize() throws IOException {
		int numberOfRemoteChannels = 2;
		int numberOfLocalChannels = 0;

		int numberOfBufferPerChannel = 2;
		int numberOfBuffersPerGate = 8;

		NettyShuffleEnvironment network = new NettyShuffleEnvironmentBuilder()
			.setNetworkBuffersPerChannel(numberOfBufferPerChannel)
			.setFloatingNetworkBuffersPerGate(numberOfBuffersPerGate)
			.build();

		SingleInputGate inputGate1 = buildInputGate(
			network,
			numberOfRemoteChannels,
			numberOfLocalChannels).f0;

		SingleInputGate[] inputGates = new SingleInputGate[]{inputGate1};
		FloatingBuffersUsageGauge floatingBuffersUsageGauge = new FloatingBuffersUsageGauge(inputGates);
		ExclusiveBuffersUsageGauge exclusiveBuffersUsageGauge = new ExclusiveBuffersUsageGauge(inputGates);
		CreditBasedInputBuffersUsageGauge inputBufferPoolUsageGauge = new CreditBasedInputBuffersUsageGauge(
			floatingBuffersUsageGauge,
			exclusiveBuffersUsageGauge,
			inputGates);

		try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {

			closeableRegistry.registerCloseable(network::close);
			closeableRegistry.registerCloseable(inputGate1::close);

			assertEquals(numberOfBuffersPerGate, floatingBuffersUsageGauge.calculateTotalBuffers(inputGate1));
			assertEquals(numberOfRemoteChannels * numberOfBufferPerChannel, exclusiveBuffersUsageGauge.calculateTotalBuffers(inputGate1));
			assertEquals(numberOfRemoteChannels * numberOfBufferPerChannel + numberOfBuffersPerGate, inputBufferPoolUsageGauge.calculateTotalBuffers(inputGate1));
		}
	}

	@Test
	public void testExclusiveBuffersUsage() throws IOException {
		int numberOfRemoteChannelsGate1 = 2;
		int numberOfLocalChannelsGate1 = 0;
		int numberOfRemoteChannelsGate2 = 1;
		int numberOfLocalChannelsGate2 = 1;

		int totalNumberOfRemoteChannels = numberOfRemoteChannelsGate1 + numberOfRemoteChannelsGate2;

		int buffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;

		NettyShuffleEnvironment network = new NettyShuffleEnvironmentBuilder()
			.setNetworkBuffersPerChannel(buffersPerChannel)
			.setFloatingNetworkBuffersPerGate(extraNetworkBuffersPerGate)
			.build();

		Tuple2<SingleInputGate, List<RemoteInputChannel>> tuple1 = buildInputGate(
			network,
			numberOfRemoteChannelsGate1,
			numberOfLocalChannelsGate1);
		Tuple2<SingleInputGate, List<RemoteInputChannel>> tuple2 = buildInputGate(
			network,
			numberOfRemoteChannelsGate2,
			numberOfLocalChannelsGate2);

		SingleInputGate inputGate1 = tuple1.f0;
		SingleInputGate inputGate2 = tuple2.f0;

		List<RemoteInputChannel> remoteInputChannels = tuple1.f1;

		SingleInputGate[] inputGates = new SingleInputGate[]{tuple1.f0, tuple2.f0};
		FloatingBuffersUsageGauge floatingBuffersUsageGauge = new FloatingBuffersUsageGauge(inputGates);
		ExclusiveBuffersUsageGauge exclusiveBuffersUsageGauge = new ExclusiveBuffersUsageGauge(inputGates);
		CreditBasedInputBuffersUsageGauge inputBuffersUsageGauge = new CreditBasedInputBuffersUsageGauge(
			floatingBuffersUsageGauge,
			exclusiveBuffersUsageGauge,
			inputGates);

		try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
			assertEquals(0.0, exclusiveBuffersUsageGauge.getValue(), 0.0);
			assertEquals(0.0, inputBuffersUsageGauge.getValue(), 0.0);

			int totalBuffers = extraNetworkBuffersPerGate * inputGates.length + buffersPerChannel * totalNumberOfRemoteChannels;

			int channelIndex = 1;
			for (RemoteInputChannel channel : remoteInputChannels) {
				drainAndValidate(
					buffersPerChannel,
					buffersPerChannel * channelIndex++,
					channel,
					closeableRegistry,
					totalBuffers,
					buffersPerChannel * totalNumberOfRemoteChannels,
					exclusiveBuffersUsageGauge,
					inputBuffersUsageGauge,
					inputGate1);
			}
		} finally {
			inputGate1.close();
			inputGate2.close();
			network.close();
		}
	}

	@Test
	public void testFloatingBuffersUsage() throws IOException, InterruptedException {

		int numberOfRemoteChannelsGate1 = 2;
		int numberOfLocalChannelsGate1 = 0;
		int numberOfRemoteChannelsGate2 = 1;
		int numberOfLocalChannelsGate2 = 1;

		int totalNumberOfRemoteChannels = numberOfRemoteChannelsGate1 + numberOfRemoteChannelsGate2;

		int buffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;

		NettyShuffleEnvironment network = new NettyShuffleEnvironmentBuilder()
			.setNetworkBuffersPerChannel(buffersPerChannel)
			.setFloatingNetworkBuffersPerGate(extraNetworkBuffersPerGate)
			.build();

		Tuple2<SingleInputGate, List<RemoteInputChannel>> tuple1 = buildInputGate(
			network,
			numberOfRemoteChannelsGate1,
			numberOfLocalChannelsGate1);
		SingleInputGate inputGate2 = buildInputGate(
			network,
			numberOfRemoteChannelsGate2,
			numberOfLocalChannelsGate2).f0;

		SingleInputGate inputGate1 = tuple1.f0;

		RemoteInputChannel remoteInputChannel1 = tuple1.f1.get(0);

		SingleInputGate[] inputGates = new SingleInputGate[]{tuple1.f0, inputGate2};
		FloatingBuffersUsageGauge floatingBuffersUsageGauge = new FloatingBuffersUsageGauge(inputGates);
		ExclusiveBuffersUsageGauge exclusiveBuffersUsageGauge = new ExclusiveBuffersUsageGauge(inputGates);
		CreditBasedInputBuffersUsageGauge inputBuffersUsageGauge = new CreditBasedInputBuffersUsageGauge(
			floatingBuffersUsageGauge,
			exclusiveBuffersUsageGauge,
			inputGates);

		try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
			assertEquals(0.0, floatingBuffersUsageGauge.getValue(), 0.0);
			assertEquals(0.0, inputBuffersUsageGauge.getValue(), 0.0);

			// drain gate1's exclusive buffers
			drainBuffer(buffersPerChannel, remoteInputChannel1, closeableRegistry);

			int totalBuffers = extraNetworkBuffersPerGate * inputGates.length + buffersPerChannel * totalNumberOfRemoteChannels;

			remoteInputChannel1.requestSubpartition(0);

			int backlog = 3;
			int totalRequestedBuffers = buffersPerChannel + backlog;

			remoteInputChannel1.onSenderBacklog(backlog);

			assertEquals(totalRequestedBuffers, remoteInputChannel1.unsynchronizedGetFloatingBuffersAvailable());

			drainBuffer(totalRequestedBuffers, remoteInputChannel1, closeableRegistry);

			assertEquals(0, remoteInputChannel1.unsynchronizedGetFloatingBuffersAvailable());
			assertEquals((double) (buffersPerChannel + totalRequestedBuffers) / totalBuffers,
				inputBuffersUsageGauge.getValue(), 0.0001);
		} finally {
			inputGate1.close();
			inputGate2.close();
			network.close();
		}
	}

	private void drainAndValidate(
		int numBuffersToRequest,
		int totalRequestedBuffers,
		RemoteInputChannel channel,
		CloseableRegistry closeableRegistry,
		int totalBuffers,
		int totalExclusiveBuffers,
		ExclusiveBuffersUsageGauge exclusiveBuffersUsageGauge,
		CreditBasedInputBuffersUsageGauge inputBuffersUsageGauge,
		SingleInputGate inputGate) throws IOException {

		drainBuffer(numBuffersToRequest, channel, closeableRegistry);
		assertEquals(totalRequestedBuffers, exclusiveBuffersUsageGauge.calculateUsedBuffers(inputGate));
		assertEquals((double) totalRequestedBuffers / totalExclusiveBuffers, exclusiveBuffersUsageGauge.getValue(), 0.0001);
		assertEquals((double) totalRequestedBuffers / totalBuffers, inputBuffersUsageGauge.getValue(), 0.0001);
	}

	private void drainBuffer(int boundary, RemoteInputChannel channel, CloseableRegistry closeableRegistry) throws IOException {
		for (int i = 0; i < boundary; i++) {
			Buffer buffer = channel.requestBuffer();
			if (buffer != null) {
				closeableRegistry.registerCloseable(buffer::recycleBuffer);
			} else {
				break;
			}
		}
	}

	private Tuple2<SingleInputGate, List<RemoteInputChannel>> buildInputGate(
		NettyShuffleEnvironment network,
		int numberOfRemoteChannels,
		int numberOfLocalChannels) throws IOException {

		SingleInputGate inputGate = new SingleInputGateBuilder()
			.setNumberOfChannels(numberOfRemoteChannels + numberOfLocalChannels)
			.setResultPartitionType(ResultPartitionType.PIPELINED_BOUNDED)
			.setupBufferPoolFactory(network)
			.build();

		Tuple2<SingleInputGate, List<RemoteInputChannel>> res = Tuple2.of(inputGate, new ArrayList<>());

		int channelIdx = 0;
		for (int i = 0; i < numberOfRemoteChannels; i++) {
			res.f1.add(buildRemoteChannel(channelIdx, inputGate, network));
			channelIdx++;
		}

		for (int i = 0; i < numberOfLocalChannels; i++) {
			buildLocalChannel(channelIdx, inputGate, network);
		}
		inputGate.setup();
		return res;
	}

	private RemoteInputChannel buildRemoteChannel(
		int channelIndex,
		SingleInputGate inputGate,
		NettyShuffleEnvironment network) {
		return new InputChannelBuilder()
			.setChannelIndex(channelIndex)
			.setupFromNettyShuffleEnvironment(network)
			.setConnectionManager(new TestingConnectionManager())
			.buildRemoteAndSetToGate(inputGate);
	}

	private void buildLocalChannel(
		int channelIndex,
		SingleInputGate inputGate,
		NettyShuffleEnvironment network) {
		new InputChannelBuilder()
			.setChannelIndex(channelIndex)
			.setupFromNettyShuffleEnvironment(network)
			.setConnectionManager(new TestingConnectionManager())
			.buildLocalAndSetToGate(inputGate);
	}
}
