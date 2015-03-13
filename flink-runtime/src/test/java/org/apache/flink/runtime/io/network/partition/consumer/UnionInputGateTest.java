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

import org.apache.flink.runtime.io.network.util.MockInputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UnionInputGateTest {

	@Test
	public void testChannelMapping() throws Exception {

		final SingleInputGate ig1 = new SingleInputGate(new IntermediateDataSetID(), 0, 3);
		final SingleInputGate ig2 = new SingleInputGate(new IntermediateDataSetID(), 0, 5);

		final UnionInputGate union = new UnionInputGate(new SingleInputGate[]{ig1, ig2});

		assertEquals(ig1.getNumberOfInputChannels() + ig2.getNumberOfInputChannels(), union.getNumberOfInputChannels());

		final MockInputChannel[][] inputChannels = new MockInputChannel[][]{
				MockInputChannel.createInputChannels(ig1, 3),
				MockInputChannel.createInputChannels(ig2, 5)
		};

		inputChannels[0][0].readBuffer(); // 0 => 0
		inputChannels[1][2].readBuffer(); // 2 => 5
		inputChannels[1][0].readBuffer(); // 0 => 3
		inputChannels[1][1].readBuffer(); // 1 => 4
		inputChannels[0][1].readBuffer(); // 1 => 1
		inputChannels[1][3].readBuffer(); // 3 => 6
		inputChannels[0][2].readBuffer(); // 1 => 2
		inputChannels[1][4].readBuffer(); // 4 => 7

		assertEquals(0, union.getNextBufferOrEvent().getChannelIndex());
		assertEquals(5, union.getNextBufferOrEvent().getChannelIndex());
		assertEquals(3, union.getNextBufferOrEvent().getChannelIndex());
		assertEquals(4, union.getNextBufferOrEvent().getChannelIndex());
		assertEquals(1, union.getNextBufferOrEvent().getChannelIndex());
		assertEquals(6, union.getNextBufferOrEvent().getChannelIndex());
		assertEquals(2, union.getNextBufferOrEvent().getChannelIndex());
		assertEquals(7, union.getNextBufferOrEvent().getChannelIndex());
	}
}
