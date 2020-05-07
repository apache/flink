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

import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

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
}
