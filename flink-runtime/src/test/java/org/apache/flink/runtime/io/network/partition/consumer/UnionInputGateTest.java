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

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateTest.verifyBufferOrEvent;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link UnionInputGate}.
 */
public class UnionInputGateTest {

	private UnionInputGate unionInputGate;
	private InputGate[] unionSubInputGates;
	private SingleInputGate[] allMemeberInputGates;
	private TestInputChannel[][] allInputChannels;

	/**
	 * Tests basic correctness of buffer-or-event interleaving and correct <code>null</code> return
	 * value after receiving all end-of-partition events.
	 *
	 * <p>For buffer-or-event instances, it is important to verify that they have been set off to
	 * the correct logical index.
	 */
	@Test(timeout = 120 * 1000)
	public void testBasicGetNextLogic() throws Exception {
		setup(new int[][]{{3}, {5}});

		allInputChannels[0][0].readBuffer(); // 0 => 0
		allInputChannels[0][0].readEndOfPartitionEvent(); // 0 => 0
		allInputChannels[1][2].readBuffer(); // 2 => 5
		allInputChannels[1][2].readEndOfPartitionEvent(); // 2 => 5
		allInputChannels[1][0].readBuffer(); // 0 => 3
		allInputChannels[1][1].readBuffer(); // 1 => 4
		allInputChannels[0][1].readBuffer(); // 1 => 1
		allInputChannels[1][3].readBuffer(); // 3 => 6
		allInputChannels[0][1].readEndOfPartitionEvent(); // 1 => 1
		allInputChannels[1][3].readEndOfPartitionEvent(); // 3 => 6
		allInputChannels[0][2].readBuffer(); // 1 => 2
		allInputChannels[0][2].readEndOfPartitionEvent(); // 1 => 2
		allInputChannels[1][4].readBuffer(); // 4 => 7
		allInputChannels[1][4].readEndOfPartitionEvent(); // 4 => 7
		allInputChannels[1][1].readEndOfPartitionEvent(); // 0 => 3
		allInputChannels[1][0].readEndOfPartitionEvent(); // 0 => 3

		allMemeberInputGates[0].notifyChannelNonEmpty(allInputChannels[0][0].getInputChannel());
		allMemeberInputGates[0].notifyChannelNonEmpty(allInputChannels[0][1].getInputChannel());
		allMemeberInputGates[0].notifyChannelNonEmpty(allInputChannels[0][2].getInputChannel());

		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][0].getInputChannel());
		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][1].getInputChannel());
		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][2].getInputChannel());
		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][3].getInputChannel());
		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][4].getInputChannel());

		verifyBufferOrEvent(unionInputGate, true, 0, true); // gate 1, channel 0
		verifyBufferOrEvent(unionInputGate, true, 3, true); // gate 2, channel 0
		verifyBufferOrEvent(unionInputGate, true, 1, true); // gate 1, channel 1
		verifyBufferOrEvent(unionInputGate, true, 4, true); // gate 2, channel 1
		verifyBufferOrEvent(unionInputGate, true, 2, true); // gate 1, channel 2
		verifyBufferOrEvent(unionInputGate, true, 5, true); // gate 2, channel 1
		verifyBufferOrEvent(unionInputGate, false, 0, true); // gate 1, channel 0
		verifyBufferOrEvent(unionInputGate, true, 6, true); // gate 2, channel 1
		verifyBufferOrEvent(unionInputGate, false, 1, true); // gate 1, channel 1
		verifyBufferOrEvent(unionInputGate, true, 7, true); // gate 2, channel 1
		verifyBufferOrEvent(unionInputGate, false, 2, true); // gate 1, channel 2
		verifyBufferOrEvent(unionInputGate, false, 3, true); // gate 2, channel 0
		verifyBufferOrEvent(unionInputGate, false, 4, true); // gate 2, channel 1
		verifyBufferOrEvent(unionInputGate, false, 5, true); // gate 2, channel 2
		verifyBufferOrEvent(unionInputGate, false, 6, true); // gate 2, channel 3
		verifyBufferOrEvent(unionInputGate, false, 7, false); // gate 2, channel 4

		// Return null when the input gate has received all end-of-partition events
		assertTrue(unionInputGate.isFinished());
		assertFalse(unionInputGate.getNextBufferOrEvent().isPresent());
	}

	/**
	 * Tests basic correctness for selected reading.
	 */
	@Test(timeout = 120 * 1000)
	public void testBasicGetNextLogicForSelectedReading() throws Exception {
		// setup
		final int[][] numChannelsOfMemberGates = new int[][] {{2}, {3}};
		setup(numChannelsOfMemberGates);

		allInputChannels[0][0].readBuffer();
		allInputChannels[0][0].readEndOfPartitionEvent();
		allInputChannels[0][1].readBuffer();
		allInputChannels[0][1].readEndOfPartitionEvent();

		allInputChannels[1][0].readBuffer();
		allInputChannels[1][0].readEndOfPartitionEvent();
		allInputChannels[1][1].readBuffer();
		allInputChannels[1][1].readEndOfPartitionEvent();
		allInputChannels[1][2].readBuffer();
		allInputChannels[1][2].readEndOfPartitionEvent();

		allMemeberInputGates[0].notifyChannelNonEmpty(allInputChannels[0][0].getInputChannel());
		allMemeberInputGates[0].notifyChannelNonEmpty(allInputChannels[0][1].getInputChannel());

		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][0].getInputChannel());
		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][1].getInputChannel());
		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][2].getInputChannel());

		// read the second input
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[1]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 0), true, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[1]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 1), true, true);

		// read the first input
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[0]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 0), true, true);

		// unspecified reading
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 1), true, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 2), true, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 0), false, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 0), false, true);

		// read the first input
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[0]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 1), false, true);
		assertFalse(unionInputGate.getNextBufferOrEvent(unionSubInputGates[0]).isPresent());

		// unspecified reading
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 1), false, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 2), false, false);

		// Return null when the input gate has received all end-of-partition events
		assertTrue(unionInputGate.isFinished());
		assertFalse(unionInputGate.getNextBufferOrEvent().isPresent());
	}

	/**
	 * Tests correctness of selected reading in the case that one or more inputs have no data.
	 */
	@Test(timeout = 120 * 1000)
	public void testGetNextNodataLogicForSelectedReading() throws Exception {
		// setup
		final int[][] numChannelsOfMemberGates = new int[][] {{1}, {2}};
		setup(numChannelsOfMemberGates);

		allInputChannels[0][0].readBuffer();
		allInputChannels[0][0].readEndOfPartitionEvent();

		allInputChannels[1][0].readBuffer();
		allInputChannels[1][0].readEndOfPartitionEvent();
		allInputChannels[1][1].readBuffer();
		allInputChannels[1][1].readEndOfPartitionEvent();

		allMemeberInputGates[0].notifyChannelNonEmpty(allInputChannels[0][0].getInputChannel());

		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][0].getInputChannel());
		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][1].getInputChannel());

		// unspecified reading
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 0), true, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 0), true, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 0), false, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 1), true, true);

		// read the second input
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[1]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 0), false, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[1]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 1), false, false);

		// read the first input
		assertFalse(unionInputGate.getNextBufferOrEvent(unionSubInputGates[0]).isPresent());

		// read the second input
		assertFalse(unionInputGate.getNextBufferOrEvent(unionSubInputGates[1]).isPresent());

		// Return null when the input gate has received all end-of-partition events
		assertTrue(unionInputGate.isFinished());
		assertFalse(unionInputGate.getNextBufferOrEvent().isPresent());
	}

	/**
	 * Tests correctness of reading in the case that only selected reading input has one piece of data in a moment.
	 */
	@Test(timeout = 120 * 1000)
	public void testGetNextLogicOnlySelectedInputHavingOneData() throws Exception {
		// setup
		final int[][] numChannelsOfMemberGates = new int[][] {{1}, {2}};
		setup(numChannelsOfMemberGates);

		// read the first input
		allInputChannels[0][0].readBuffer(false);
		allMemeberInputGates[0].notifyChannelNonEmpty(allInputChannels[0][0].getInputChannel());

		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[0]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 0), true, false);

		// unspecified reading
		allInputChannels[0][0].readBuffer();
		allMemeberInputGates[0].notifyChannelNonEmpty(allInputChannels[0][0].getInputChannel());

		allInputChannels[1][0].readBuffer();
		allInputChannels[1][0].readEndOfPartitionEvent();
		allInputChannels[1][1].readBuffer();
		allInputChannels[1][1].readEndOfPartitionEvent();

		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][0].getInputChannel());
		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][1].getInputChannel());

		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 0), true, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 0), true, true);

		// read the first input again
		allInputChannels[0][0].readEndOfPartitionEvent();
		allMemeberInputGates[0].notifyChannelNonEmpty(allInputChannels[0][0].getInputChannel());

		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[0]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 0), false, true);

		// unspecified reading again
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 1), true, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 0), false, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 1), false, false);

		// Return null when the input gate has received all end-of-partition events
		assertTrue(unionInputGate.isFinished());
		assertFalse(unionInputGate.getNextBufferOrEvent().isPresent());
	}

	/**
	 * Tests correctness for selected group reading.
	 */
	@Test(timeout = 120 * 1000)
	public void testGetNextLogicWithSelectedGroupReading() throws Exception {
		// setup
		final int[][] numChannelsOfMemberGates = new int[][] {{2}, {3, 1}};
		setup(numChannelsOfMemberGates);

		allInputChannels[0][0].readBuffer();
		allInputChannels[0][0].readEndOfPartitionEvent();
		allInputChannels[0][1].readBuffer();
		allInputChannels[0][1].readEndOfPartitionEvent();

		allInputChannels[1][0].readBuffer();
		allInputChannels[1][0].readEndOfPartitionEvent();
		allInputChannels[1][1].readBuffer();
		allInputChannels[1][1].readEndOfPartitionEvent();
		allInputChannels[1][2].readBuffer();
		allInputChannels[1][2].readEndOfPartitionEvent();

		allInputChannels[2][0].readBuffer();
		allInputChannels[2][0].readEndOfPartitionEvent();

		allMemeberInputGates[0].notifyChannelNonEmpty(allInputChannels[0][0].getInputChannel());
		allMemeberInputGates[0].notifyChannelNonEmpty(allInputChannels[0][1].getInputChannel());

		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][0].getInputChannel());
		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][1].getInputChannel());
		allMemeberInputGates[1].notifyChannelNonEmpty(allInputChannels[1][2].getInputChannel());

		allMemeberInputGates[2].notifyChannelNonEmpty(allInputChannels[2][0].getInputChannel());

		// read the second input
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[1]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 0), true, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[1]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 2, 0), true, true);

		// read the first input
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[0]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 0), true, true);

		// unspecified reading
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 1), true, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 1), true, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 0), false, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 2, 0), false, true);

		// read the first input
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.of(unionSubInputGates[0]),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 0, 1), false, true);
		assertFalse(unionInputGate.getNextBufferOrEvent(unionSubInputGates[0]).isPresent());

		// unspecified reading
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 2), true, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 0), false, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 1), false, true);
		verifyBufferOrEventWithPriorityReading(unionInputGate, Optional.empty(),
			channelIndexOfUnionInputGate(numChannelsOfMemberGates, 1, 2), false, false);

		// Return null when the input gate has received all end-of-partition events
		assertTrue(unionInputGate.isFinished());
		assertFalse(unionInputGate.getNextBufferOrEvent().isPresent());
	}

	// ------------------------------------------------------------------------

	private void setup(int[][] numChannelsOfMemberGates) {
		int numAllMemberInputGates = 0;
		for (int[] numChannelsOfInputGate : numChannelsOfMemberGates) {
			numAllMemberInputGates += numChannelsOfInputGate.length;
		}

		this.allMemeberInputGates = new SingleInputGate[numAllMemberInputGates];
		this.allInputChannels = new TestInputChannel[numAllMemberInputGates][];

		PartitionRequestManager partitionRequestManager = new PartitionRequestManager(Integer.MAX_VALUE, numAllMemberInputGates);

		int numInputGatesL1 = numChannelsOfMemberGates.length;
		this.unionSubInputGates = new InputGate[numInputGatesL1];

		int currentMemberGateIndex = 0;
		for (int i = 0; i < numInputGatesL1; i++) {
			int numInputGatesL2 = numChannelsOfMemberGates[i].length;
			final InputGate[] inputGatesL2 = new InputGate[numInputGatesL2];
			checkState(numInputGatesL2 > 0);

			for (int  j = 0; j < numInputGatesL2; j++) {
				int numChannels = numChannelsOfMemberGates[i][j];
				final String testTaskName = "Test Task " + currentMemberGateIndex;

				allMemeberInputGates[currentMemberGateIndex] = new SingleInputGate(
					testTaskName, new JobID(),
					new IntermediateDataSetID(), ResultPartitionType.PIPELINED,
					0, numChannels,
					mock(TaskActions.class),
					UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup(),
					partitionRequestManager,
					Executors.newSingleThreadExecutor(),
					true,
					false);

				allInputChannels[currentMemberGateIndex] = TestInputChannel.createInputChannels(
					(SingleInputGate) allMemeberInputGates[currentMemberGateIndex], numChannels);
				inputGatesL2[j] = allMemeberInputGates[currentMemberGateIndex];

				currentMemberGateIndex++;
			}

			if (inputGatesL2.length == 1) {
				unionSubInputGates[i] = inputGatesL2[0];
			} else {
				unionSubInputGates[i] = new UnionInputGate(inputGatesL2);

				int numChannelsOfUnionGate = 0;
				for (InputGate inputGate : inputGatesL2) {
					numChannelsOfUnionGate += inputGate.getNumberOfInputChannels();
				}
				assertEquals(numChannelsOfUnionGate, unionSubInputGates[i].getNumberOfInputChannels());
			}
		}

		this.unionInputGate = new UnionInputGate(unionSubInputGates);
		int numChannelsOfUnionGate = 0;
		for (InputGate inputGate : unionSubInputGates) {
			numChannelsOfUnionGate += inputGate.getNumberOfInputChannels();
		}
		assertEquals(numChannelsOfUnionGate, unionInputGate.getNumberOfInputChannels());
	}

	private int channelIndexOfUnionInputGate(int[][] numChannelsOfMemberGates, int memberGateIndex, int channelIndexInMemberGate) {
		int channelOffset = 0;

		int currentMemberGateIndex = 0;
		for (int i = 0; i < numChannelsOfMemberGates.length && currentMemberGateIndex < memberGateIndex; i++) {
			for (int j = 0; j < numChannelsOfMemberGates[i].length && currentMemberGateIndex < memberGateIndex; j++) {
				channelOffset += numChannelsOfMemberGates[i][j];

				currentMemberGateIndex++;
			}
		}

		return channelOffset + channelIndexInMemberGate;
	}

	private static void verifyBufferOrEventWithPriorityReading(
		UnionInputGate unionInputGate,
		Optional<InputGate> readingMemberGate,
		int channelIndex,
		boolean isBuffer,
		boolean moreAvailable) throws IOException, InterruptedException {

		final Optional<BufferOrEvent> bufferOrEvent = readingMemberGate.isPresent() ?
			unionInputGate.getNextBufferOrEvent(readingMemberGate.get()) : unionInputGate.getNextBufferOrEvent();
		assertTrue(bufferOrEvent.isPresent());
		assertEquals(channelIndex, bufferOrEvent.get().getChannelIndex());
		assertEquals(isBuffer, bufferOrEvent.get().isBuffer());
		assertEquals(moreAvailable, bufferOrEvent.get().moreAvailable());
	}
}
