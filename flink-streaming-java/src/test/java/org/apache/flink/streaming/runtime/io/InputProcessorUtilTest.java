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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.MockChannelStateWriter;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.util.MockStreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the behaviors of the {@link InputProcessorUtil}.
 */
public class InputProcessorUtilTest {

	@Test
	public void testGenerateChannelIndexToInputGateMap() {
		SingleInputGate ig1 = new SingleInputGateBuilder().setNumberOfChannels(2).build();
		SingleInputGate ig2 = new SingleInputGateBuilder().setNumberOfChannels(3).build();

		InputGate[] channelIndexToInputGateMap = InputProcessorUtil.generateChannelIndexToInputGateMap(ig1, ig2);
		assertEquals(5, channelIndexToInputGateMap.length);
		assertEquals(ig1, channelIndexToInputGateMap[0]);
		assertEquals(ig1, channelIndexToInputGateMap[1]);
		assertEquals(ig2, channelIndexToInputGateMap[2]);
		assertEquals(ig2, channelIndexToInputGateMap[3]);
		assertEquals(ig2, channelIndexToInputGateMap[4]);
	}

	@Test
	public void testGenerateInputGateToChannelIndexOffsetMap() {
		SingleInputGate ig1 = new SingleInputGateBuilder().setNumberOfChannels(3).build();
		SingleInputGate ig2 = new SingleInputGateBuilder().setNumberOfChannels(2).build();

		Map<InputGate, Integer> inputGateToChannelIndexOffsetMap =
			InputProcessorUtil.generateInputGateToChannelIndexOffsetMap(ig1, ig2);
		assertEquals(2, inputGateToChannelIndexOffsetMap.size());
		assertEquals(0, inputGateToChannelIndexOffsetMap.get(ig1).intValue());
		assertEquals(3, inputGateToChannelIndexOffsetMap.get(ig2).intValue());
	}

	@Test
	public void testCreateCheckpointedMultipleInputGate() throws Exception {
		try (CloseableRegistry registry = new CloseableRegistry()) {
			MockEnvironment environment = new MockEnvironmentBuilder().build();
			MockStreamTask streamTask = new MockStreamTaskBuilder(environment).build();
			StreamConfig streamConfig = new StreamConfig(environment.getJobConfiguration());
			streamConfig.setCheckpointMode(CheckpointingMode.EXACTLY_ONCE);
			streamConfig.setUnalignedCheckpointsEnabled(true);

			// First input gate has index larger than the second
			Collection<IndexedInputGate>[] inputGates = new Collection[] {
				Collections.singletonList(new MockIndexedInputGate(1, 4)),
				Collections.singletonList(new MockIndexedInputGate(0, 2)),
			};

			CheckpointedInputGate[] checkpointedMultipleInputGate = InputProcessorUtil.createCheckpointedMultipleInputGate(
				streamTask,
				streamConfig,
				new MockChannelStateWriter(),
				environment.getMetricGroup().getIOMetricGroup(),
				streamTask.getName(),
				inputGates);
			for (CheckpointedInputGate checkpointedInputGate : checkpointedMultipleInputGate) {
				registry.registerCloseable(checkpointedInputGate);
			}

			CheckpointBarrierHandler barrierHandler = checkpointedMultipleInputGate[0].getCheckpointBarrierHandler();
			assertTrue(barrierHandler.getBufferReceivedListener().isPresent());
			BufferReceivedListener bufferReceivedListener = barrierHandler.getBufferReceivedListener().get();

			List<IndexedInputGate> allInputGates = Arrays.stream(inputGates).flatMap(gates -> gates.stream()).collect(Collectors.toList());
			for (IndexedInputGate inputGate : allInputGates) {
				for (int channelId = 0; channelId < inputGate.getNumberOfInputChannels(); channelId++) {
					bufferReceivedListener.notifyBarrierReceived(
						new CheckpointBarrier(1, 42, CheckpointOptions.forCheckpointWithDefaultLocation(true, true)),
						new InputChannelInfo(inputGate.getGateIndex(), channelId));
				}
			}
			assertTrue(barrierHandler.getAllBarriersReceivedFuture(1).isDone());
		}
	}
}
