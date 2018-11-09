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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.junit.Test;

import static org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateTest.verifyBufferOrEvent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link UnionInputGate}.
 */
public class UnionInputGateTest {

	/**
	 * Tests basic correctness of buffer-or-event interleaving and correct <code>null</code> return
	 * value after receiving all end-of-partition events.
	 *
	 * <p>For buffer-or-event instances, it is important to verify that they have been set off to
	 * the correct logical index.
	 */
	@Test(timeout = 120 * 1000)
	public void testBasicGetNextLogic() throws Exception {
		// Setup
		final String testTaskName = "Test Task";
		final SingleInputGate ig1 = new SingleInputGate(
			testTaskName, new JobID(),
			new IntermediateDataSetID(), ResultPartitionType.PIPELINED,
			0, 3,
			mock(TaskActions.class),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup(),
			true);
		final SingleInputGate ig2 = new SingleInputGate(
			testTaskName, new JobID(),
			new IntermediateDataSetID(), ResultPartitionType.PIPELINED,
			0, 5,
			mock(TaskActions.class),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup(),
			true);

		final UnionInputGate union = new UnionInputGate(new SingleInputGate[]{ig1, ig2});

		assertEquals(ig1.getNumberOfInputChannels() + ig2.getNumberOfInputChannels(), union.getNumberOfInputChannels());

		final TestInputChannel[][] inputChannels = new TestInputChannel[][]{
				TestInputChannel.createInputChannels(ig1, 3),
				TestInputChannel.createInputChannels(ig2, 5)
		};

		inputChannels[0][0].readBuffer(); // 0 => 0
		inputChannels[0][0].readEndOfPartitionEvent(); // 0 => 0
		inputChannels[1][2].readBuffer(); // 2 => 5
		inputChannels[1][2].readEndOfPartitionEvent(); // 2 => 5
		inputChannels[1][0].readBuffer(); // 0 => 3
		inputChannels[1][1].readBuffer(); // 1 => 4
		inputChannels[0][1].readBuffer(); // 1 => 1
		inputChannels[1][3].readBuffer(); // 3 => 6
		inputChannels[0][1].readEndOfPartitionEvent(); // 1 => 1
		inputChannels[1][3].readEndOfPartitionEvent(); // 3 => 6
		inputChannels[0][2].readBuffer(); // 1 => 2
		inputChannels[0][2].readEndOfPartitionEvent(); // 1 => 2
		inputChannels[1][4].readBuffer(); // 4 => 7
		inputChannels[1][4].readEndOfPartitionEvent(); // 4 => 7
		inputChannels[1][1].readEndOfPartitionEvent(); // 0 => 3
		inputChannels[1][0].readEndOfPartitionEvent(); // 0 => 3

		ig1.notifyChannelNonEmpty(inputChannels[0][0]);
		ig1.notifyChannelNonEmpty(inputChannels[0][1]);
		ig1.notifyChannelNonEmpty(inputChannels[0][2]);

		ig2.notifyChannelNonEmpty(inputChannels[1][0]);
		ig2.notifyChannelNonEmpty(inputChannels[1][1]);
		ig2.notifyChannelNonEmpty(inputChannels[1][2]);
		ig2.notifyChannelNonEmpty(inputChannels[1][3]);
		ig2.notifyChannelNonEmpty(inputChannels[1][4]);

		verifyBufferOrEvent(union, true, 0, true); // gate 1, channel 0
		verifyBufferOrEvent(union, true, 3, true); // gate 2, channel 0
		verifyBufferOrEvent(union, true, 1, true); // gate 1, channel 1
		verifyBufferOrEvent(union, true, 4, true); // gate 2, channel 1
		verifyBufferOrEvent(union, true, 2, true); // gate 1, channel 2
		verifyBufferOrEvent(union, true, 5, true); // gate 2, channel 1
		verifyBufferOrEvent(union, false, 0, true); // gate 1, channel 0
		verifyBufferOrEvent(union, true, 6, true); // gate 2, channel 1
		verifyBufferOrEvent(union, false, 1, true); // gate 1, channel 1
		verifyBufferOrEvent(union, true, 7, true); // gate 2, channel 1
		verifyBufferOrEvent(union, false, 2, true); // gate 1, channel 2
		verifyBufferOrEvent(union, false, 3, true); // gate 2, channel 0
		verifyBufferOrEvent(union, false, 4, true); // gate 2, channel 1
		verifyBufferOrEvent(union, false, 5, true); // gate 2, channel 2
		verifyBufferOrEvent(union, false, 6, true); // gate 2, channel 3
		verifyBufferOrEvent(union, false, 7, false); // gate 2, channel 4

		// Return null when the input gate has received all end-of-partition events
		assertTrue(union.isFinished());
		assertFalse(union.getNextBufferOrEvent().isPresent());
	}
}
