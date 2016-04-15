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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SingleInputGateTest {

	/**
	 * Tests basic correctness of buffer-or-event interleaving and correct <code>null</code> return
	 * value after receiving all end-of-partition events.
	 */
	@Test(timeout = 120 * 1000)
	public void testBasicGetNextLogic() throws Exception {
		// Setup
		final SingleInputGate inputGate = new SingleInputGate(
				"Test Task Name", new JobID(), new ExecutionAttemptID(), new IntermediateDataSetID(), 0, 2, mock(PartitionStateChecker.class));

		final TestInputChannel[] inputChannels = new TestInputChannel[]{
				new TestInputChannel(inputGate, 0),
				new TestInputChannel(inputGate, 1)
		};

		inputGate.setInputChannel(
				new IntermediateResultPartitionID(), inputChannels[0].getInputChannel());

		inputGate.setInputChannel(
				new IntermediateResultPartitionID(), inputChannels[1].getInputChannel());

		// Test
		inputChannels[0].readBuffer();
		inputChannels[0].readBuffer();
		inputChannels[1].readBuffer();
		inputChannels[1].readEndOfPartitionEvent();
		inputChannels[0].readEndOfPartitionEvent();

		verifyBufferOrEvent(inputGate, true, 0);
		verifyBufferOrEvent(inputGate, true, 0);
		verifyBufferOrEvent(inputGate, true, 1);
		verifyBufferOrEvent(inputGate, false, 1);
		verifyBufferOrEvent(inputGate, false, 0);

		// Return null when the input gate has received all end-of-partition events
		assertTrue(inputGate.isFinished());
		assertNull(inputGate.getNextBufferOrEvent());
	}

	@Test
	public void testBackwardsEventWithUninitializedChannel() throws Exception {
		// Setup environment
		final TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
		when(taskEventDispatcher.publish(any(ResultPartitionID.class), any(TaskEvent.class))).thenReturn(true);

		final ResultSubpartitionView iterator = mock(ResultSubpartitionView.class);
		when(iterator.getNextBuffer()).thenReturn(
				new Buffer(MemorySegmentFactory.allocateUnpooledSegment(1024), mock(BufferRecycler.class)));

		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager.createSubpartitionView(any(ResultPartitionID.class), anyInt(), any(BufferProvider.class))).thenReturn(iterator);

		// Setup reader with one local and one unknown input channel
		final IntermediateDataSetID resultId = new IntermediateDataSetID();

		final SingleInputGate inputGate = new SingleInputGate("Test Task Name", new JobID(), new ExecutionAttemptID(), resultId, 0, 2, mock(PartitionStateChecker.class));
		final BufferPool bufferPool = mock(BufferPool.class);
		when(bufferPool.getNumberOfRequiredMemorySegments()).thenReturn(2);

		inputGate.setBufferPool(bufferPool);

		// Local
		ResultPartitionID localPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());

		InputChannel local = new LocalInputChannel(inputGate, 0, localPartitionId, partitionManager, taskEventDispatcher);

		// Unknown
		ResultPartitionID unknownPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());

		InputChannel unknown = new UnknownInputChannel(inputGate, 1, unknownPartitionId, partitionManager, taskEventDispatcher, mock(ConnectionManager.class), new Tuple2<Integer, Integer>(0, 0));

		// Set channels
		inputGate.setInputChannel(localPartitionId.getPartitionId(), local);
		inputGate.setInputChannel(unknownPartitionId.getPartitionId(), unknown);

		// Request partitions
		inputGate.requestPartitions();

		// Only the local channel can request
		verify(partitionManager, times(1)).createSubpartitionView(any(ResultPartitionID.class), anyInt(), any(BufferProvider.class));

		// Send event backwards and initialize unknown channel afterwards
		final TaskEvent event = new TestTaskEvent();
		inputGate.sendTaskEvent(event);

		// Only the local channel can send out the event
		verify(taskEventDispatcher, times(1)).publish(any(ResultPartitionID.class), any(TaskEvent.class));

		// After the update, the pending event should be send to local channel
		inputGate.updateInputChannel(new InputChannelDeploymentDescriptor(new ResultPartitionID(unknownPartitionId.getPartitionId(), unknownPartitionId.getProducerId()), ResultPartitionLocation.createLocal()));

		verify(partitionManager, times(2)).createSubpartitionView(any(ResultPartitionID.class), anyInt(), any(BufferProvider.class));
		verify(taskEventDispatcher, times(2)).publish(any(ResultPartitionID.class), any(TaskEvent.class));
	}

	/**
	 * Tests that an update channel does not trigger a partition request before the UDF has
	 * requested any partitions. Otherwise, this can lead to races when registering a listener at
	 * the gate (e.g. in UnionInputGate), which can result in missed buffer notifications at the
	 * listener.
	 */
	@Test
	public void testUpdateChannelBeforeRequest() throws Exception {
		SingleInputGate inputGate = new SingleInputGate(
				"t1",
				new JobID(),
				new ExecutionAttemptID(),
				new IntermediateDataSetID(),
				0,
				1,
				mock(PartitionStateChecker.class));

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);

		InputChannel unknown = new UnknownInputChannel(
				inputGate,
				0,
				new ResultPartitionID(),
				partitionManager,
				new TaskEventDispatcher(),
				new LocalConnectionManager(),
				new Tuple2<>(0, 0));

		inputGate.setInputChannel(unknown.partitionId.getPartitionId(), unknown);

		// Update to a local channel and verify that no request is triggered
		inputGate.updateInputChannel(new InputChannelDeploymentDescriptor(
				unknown.partitionId,
				ResultPartitionLocation.createLocal()));

		verify(partitionManager, never()).createSubpartitionView(
				any(ResultPartitionID.class), anyInt(), any(BufferProvider.class));
	}

	/**
	 * Tests that the release of the input gate is noticed while polling the
	 * channels for available data.
	 */
	@Test
	public void testReleaseWhilePollingChannel() throws Exception {
		final AtomicReference<Exception> asyncException = new AtomicReference<>();

		// Setup the input gate with a single channel that does nothing
		final SingleInputGate inputGate = new SingleInputGate(
				"InputGate",
				new JobID(),
				new ExecutionAttemptID(),
				new IntermediateDataSetID(),
				0,
				1,
				mock(PartitionStateChecker.class));

		InputChannel unknown = new UnknownInputChannel(
				inputGate,
				0,
				new ResultPartitionID(),
				new ResultPartitionManager(),
				new TaskEventDispatcher(),
				new LocalConnectionManager(),
				new Tuple2<>(0, 0));

		inputGate.setInputChannel(unknown.partitionId.getPartitionId(), unknown);

		// Start the consumer in a separate Thread
		Thread asyncConsumer = new Thread() {
			@Override
			public void run() {
				try {
					inputGate.getNextBufferOrEvent();
				} catch (Exception e) {
					asyncException.set(e);
				}
			}
		};
		asyncConsumer.start();

		// Wait for blocking queue poll call and release input gate
		boolean success = false;
		for (int i = 0; i < 50; i++) {
			if (asyncConsumer != null && asyncConsumer.isAlive()) {
				StackTraceElement[] stackTrace = asyncConsumer.getStackTrace();
				success = isInBlockingQueuePoll(stackTrace);
			}

			if (success) {
				break;
			} else {
				// Retry
				Thread.sleep(500);
			}
		}

		// Verify that async consumer is in blocking request
		assertTrue("Did not trigger blocking buffer request.", success);

		// Release the input gate
		inputGate.releaseAllResources();

		// Wait for Thread to finish and verify expected Exceptions. If the
		// input gate status is not properly checked during requests, this
		// call will never return.
		asyncConsumer.join();

		assertNotNull(asyncException.get());
		assertEquals(IllegalStateException.class, asyncException.get().getClass());
	}

	/**
	 * Returns whether the stack trace represents a Thread in a blocking queue
	 * poll call.
	 *
	 * @param stackTrace Stack trace of the Thread to check
	 *
	 * @return Flag indicating whether the Thread is in a blocking queue poll
	 * call.
	 */
	private boolean isInBlockingQueuePoll(StackTraceElement[] stackTrace) {
		for (StackTraceElement elem : stackTrace) {
			if (elem.getMethodName().equals("poll") &&
					elem.getClassName().equals("java.util.concurrent.LinkedBlockingQueue")) {

				return true;
			}
		}

		return false;
	}

	// ---------------------------------------------------------------------------------------------

	static void verifyBufferOrEvent(
			InputGate inputGate,
			boolean isBuffer,
			int channelIndex) throws IOException, InterruptedException {

		final BufferOrEvent boe = inputGate.getNextBufferOrEvent();
		assertEquals(isBuffer, boe.isBuffer());
		assertEquals(channelIndex, boe.getChannelIndex());
	}
}
