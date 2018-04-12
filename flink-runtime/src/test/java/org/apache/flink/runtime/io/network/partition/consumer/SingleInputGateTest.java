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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
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
		final SingleInputGate inputGate = createInputGate();

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

		inputGate.notifyChannelNonEmpty(inputChannels[0].getInputChannel());
		inputGate.notifyChannelNonEmpty(inputChannels[1].getInputChannel());

		verifyBufferOrEvent(inputGate, true, 0, true);
		verifyBufferOrEvent(inputGate, true, 1, true);
		verifyBufferOrEvent(inputGate, true, 0, true);
		verifyBufferOrEvent(inputGate, false, 1, true);
		verifyBufferOrEvent(inputGate, false, 0, false);

		// Return null when the input gate has received all end-of-partition events
		assertTrue(inputGate.isFinished());
	}

	@Test(timeout = 120 * 1000)
	public void testIsMoreAvailableReadingFromSingleInputChannel() throws Exception {
		// Setup
		final SingleInputGate inputGate = createInputGate();

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
		inputChannels[0].readBuffer(false);

		inputGate.notifyChannelNonEmpty(inputChannels[0].getInputChannel());

		verifyBufferOrEvent(inputGate, true, 0, true);
		verifyBufferOrEvent(inputGate, true, 0, false);
	}

	@Test
	public void testBackwardsEventWithUninitializedChannel() throws Exception {
		// Setup environment
		final TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
		when(taskEventDispatcher.publish(any(ResultPartitionID.class), any(TaskEvent.class))).thenReturn(true);

		final ResultSubpartitionView iterator = mock(ResultSubpartitionView.class);
		when(iterator.getNextBuffer()).thenReturn(
			new BufferAndBacklog(new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(1024), FreeingBufferRecycler.INSTANCE), false,0, false));

		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager.createSubpartitionView(
			any(ResultPartitionID.class),
			anyInt(),
			any(BufferAvailabilityListener.class))).thenReturn(iterator);

		// Setup reader with one local and one unknown input channel

		final SingleInputGate inputGate = createInputGate();
		final BufferPool bufferPool = mock(BufferPool.class);
		when(bufferPool.getNumberOfRequiredMemorySegments()).thenReturn(2);

		inputGate.setBufferPool(bufferPool);

		// Local
		ResultPartitionID localPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());

		InputChannel local = new LocalInputChannel(inputGate, 0, localPartitionId, partitionManager, taskEventDispatcher, UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());

		// Unknown
		ResultPartitionID unknownPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());

		InputChannel unknown = new UnknownInputChannel(inputGate, 1, unknownPartitionId, partitionManager, taskEventDispatcher, mock(ConnectionManager.class), 0, 0, UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());

		// Set channels
		inputGate.setInputChannel(localPartitionId.getPartitionId(), local);
		inputGate.setInputChannel(unknownPartitionId.getPartitionId(), unknown);

		// Request partitions
		inputGate.requestPartitions();

		// Only the local channel can request
		verify(partitionManager, times(1)).createSubpartitionView(any(ResultPartitionID.class), anyInt(), any(BufferAvailabilityListener.class));

		// Send event backwards and initialize unknown channel afterwards
		final TaskEvent event = new TestTaskEvent();
		inputGate.sendTaskEvent(event);

		// Only the local channel can send out the event
		verify(taskEventDispatcher, times(1)).publish(any(ResultPartitionID.class), any(TaskEvent.class));

		// After the update, the pending event should be send to local channel
		inputGate.updateInputChannel(new InputChannelDeploymentDescriptor(new ResultPartitionID(unknownPartitionId.getPartitionId(), unknownPartitionId.getProducerId()), ResultPartitionLocation.createLocal()));

		verify(partitionManager, times(2)).createSubpartitionView(any(ResultPartitionID.class), anyInt(), any(BufferAvailabilityListener.class));
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
		SingleInputGate inputGate = createInputGate(1);

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);

		InputChannel unknown = new UnknownInputChannel(
			inputGate,
			0,
			new ResultPartitionID(),
			partitionManager,
			new TaskEventDispatcher(),
			new LocalConnectionManager(),
			0, 0, UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());

		inputGate.setInputChannel(unknown.partitionId.getPartitionId(), unknown);

		// Update to a local channel and verify that no request is triggered
		inputGate.updateInputChannel(new InputChannelDeploymentDescriptor(
			unknown.partitionId,
			ResultPartitionLocation.createLocal()));

		verify(partitionManager, never()).createSubpartitionView(
			any(ResultPartitionID.class), anyInt(), any(BufferAvailabilityListener.class));
	}

	/**
	 * Tests that the release of the input gate is noticed while polling the
	 * channels for available data.
	 */
	@Test
	public void testReleaseWhilePollingChannel() throws Exception {
		final AtomicReference<Exception> asyncException = new AtomicReference<>();

		// Setup the input gate with a single channel that does nothing
		final SingleInputGate inputGate = createInputGate(1);

		InputChannel unknown = new UnknownInputChannel(
			inputGate,
			0,
			new ResultPartitionID(),
			new ResultPartitionManager(),
			new TaskEventDispatcher(),
			new LocalConnectionManager(),
			0, 0,
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());

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
			if (asyncConsumer.isAlive()) {
				success = asyncConsumer.getState() == Thread.State.WAITING;
			}

			if (success) {
				break;
			} else {
				// Retry
				Thread.sleep(100);
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
	 * Tests request back off configuration is correctly forwarded to the channels.
	 */
	@Test
	public void testRequestBackoffConfiguration() throws Exception {
		ResultPartitionID[] partitionIds = new ResultPartitionID[] {
			new ResultPartitionID(),
			new ResultPartitionID(),
			new ResultPartitionID()
		};

		InputChannelDeploymentDescriptor[] channelDescs = new InputChannelDeploymentDescriptor[]{
			// Local
			new InputChannelDeploymentDescriptor(
				partitionIds[0],
				ResultPartitionLocation.createLocal()),
			// Remote
			new InputChannelDeploymentDescriptor(
				partitionIds[1],
				ResultPartitionLocation.createRemote(new ConnectionID(new InetSocketAddress("localhost", 5000), 0))),
			// Unknown
			new InputChannelDeploymentDescriptor(
				partitionIds[2],
				ResultPartitionLocation.createUnknown())};

		InputGateDeploymentDescriptor gateDesc =
			new InputGateDeploymentDescriptor(new IntermediateDataSetID(),
				ResultPartitionType.PIPELINED, 0, channelDescs);

		int initialBackoff = 137;
		int maxBackoff = 1001;

		NetworkEnvironment netEnv = mock(NetworkEnvironment.class);
		when(netEnv.getResultPartitionManager()).thenReturn(new ResultPartitionManager());
		when(netEnv.getTaskEventDispatcher()).thenReturn(new TaskEventDispatcher());
		when(netEnv.getPartitionRequestInitialBackoff()).thenReturn(initialBackoff);
		when(netEnv.getPartitionRequestMaxBackoff()).thenReturn(maxBackoff);
		when(netEnv.getConnectionManager()).thenReturn(new LocalConnectionManager());

		SingleInputGate gate = SingleInputGate.create(
			"TestTask",
			new JobID(),
			new ExecutionAttemptID(),
			gateDesc,
			netEnv,
			mock(TaskActions.class),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());

		assertEquals(gateDesc.getConsumedPartitionType(), gate.getConsumedPartitionType());

		Map<IntermediateResultPartitionID, InputChannel> channelMap = gate.getInputChannels();

		assertEquals(3, channelMap.size());
		InputChannel localChannel = channelMap.get(partitionIds[0].getPartitionId());
		assertEquals(LocalInputChannel.class, localChannel.getClass());

		InputChannel remoteChannel = channelMap.get(partitionIds[1].getPartitionId());
		assertEquals(RemoteInputChannel.class, remoteChannel.getClass());

		InputChannel unknownChannel = channelMap.get(partitionIds[2].getPartitionId());
		assertEquals(UnknownInputChannel.class, unknownChannel.getClass());

		InputChannel[] channels = new InputChannel[]{localChannel, remoteChannel, unknownChannel};
		for (InputChannel ch : channels) {
			assertEquals(0, ch.getCurrentBackoff());

			assertTrue(ch.increaseBackoff());
			assertEquals(initialBackoff, ch.getCurrentBackoff());

			assertTrue(ch.increaseBackoff());
			assertEquals(initialBackoff * 2, ch.getCurrentBackoff());

			assertTrue(ch.increaseBackoff());
			assertEquals(initialBackoff * 2 * 2, ch.getCurrentBackoff());

			assertTrue(ch.increaseBackoff());
			assertEquals(maxBackoff, ch.getCurrentBackoff());

			assertFalse(ch.increaseBackoff());
		}
	}

	/**
	 * Tests that input gate requests and assigns network buffers for remote input channel.
	 */
	@Test
	public void testRequestBuffersWithRemoteInputChannel() throws Exception {
		final SingleInputGate inputGate = new SingleInputGate(
			"t1",
			new JobID(),
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED_BOUNDED,
			0,
			1,
			mock(TaskActions.class),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());

		RemoteInputChannel remote = mock(RemoteInputChannel.class);
		inputGate.setInputChannel(new IntermediateResultPartitionID(), remote);

		final int buffersPerChannel = 2;
		NetworkBufferPool network = mock(NetworkBufferPool.class);
		// Trigger requests of segments from global pool and assign buffers to remote input channel
		inputGate.assignExclusiveSegments(network, buffersPerChannel);

		verify(network, times(1)).requestMemorySegments(buffersPerChannel);
		verify(remote, times(1)).assignExclusiveSegments(anyListOf(MemorySegment.class));
	}

	/**
	 * Tests that input gate requests and assigns network buffers when unknown input channel
	 * updates to remote input channel.
	 */
	@Test
	public void testRequestBuffersWithUnknownInputChannel() throws Exception {
		final SingleInputGate inputGate = createInputGate(1);

		UnknownInputChannel unknown = mock(UnknownInputChannel.class);
		final ResultPartitionID resultPartitionId = new ResultPartitionID();
		inputGate.setInputChannel(resultPartitionId.getPartitionId(), unknown);

		RemoteInputChannel remote = mock(RemoteInputChannel.class);
		final ConnectionID connectionId = new ConnectionID(new InetSocketAddress("localhost", 5000), 0);
		when(unknown.toRemoteInputChannel(connectionId)).thenReturn(remote);

		final int buffersPerChannel = 2;
		NetworkBufferPool network = mock(NetworkBufferPool.class);
		inputGate.assignExclusiveSegments(network, buffersPerChannel);

		// Trigger updates to remote input channel from unknown input channel
		inputGate.updateInputChannel(new InputChannelDeploymentDescriptor(
			resultPartitionId,
			ResultPartitionLocation.createRemote(connectionId)));

		verify(network, times(1)).requestMemorySegments(buffersPerChannel);
		verify(remote, times(1)).assignExclusiveSegments(anyListOf(MemorySegment.class));
	}

	// ---------------------------------------------------------------------------------------------

	private static SingleInputGate createInputGate() {
		return createInputGate(2);
	}

	private static SingleInputGate createInputGate(int numberOfInputChannels) {
		SingleInputGate inputGate = new SingleInputGate(
			"Test Task Name",
			new JobID(),
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED,
			0,
			numberOfInputChannels,
			mock(TaskActions.class),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());

		assertEquals(ResultPartitionType.PIPELINED, inputGate.getConsumedPartitionType());

		return inputGate;
	}

	static void verifyBufferOrEvent(
			InputGate inputGate,
			boolean expectedIsBuffer,
			int expectedChannelIndex,
			boolean expectedMoreAvailable) throws IOException, InterruptedException {

		final Optional<BufferOrEvent> bufferOrEvent = inputGate.getNextBufferOrEvent();
		assertTrue(bufferOrEvent.isPresent());
		assertEquals(expectedIsBuffer, bufferOrEvent.get().isBuffer());
		assertEquals(expectedChannelIndex, bufferOrEvent.get().getChannelIndex());
		assertEquals(expectedMoreAvailable, bufferOrEvent.get().moreAvailable());
		if (!expectedMoreAvailable) {
			try {
				assertFalse(inputGate.pollNextBufferOrEvent().isPresent());
			}
			catch (UnsupportedOperationException ex) {
				/**
				 * {@link UnionInputGate#pollNextBufferOrEvent()} is unsupported at the moment.
				 */
				if (!(inputGate instanceof UnionInputGate)) {
					throw ex;
				}
			}
		}
	}
}
