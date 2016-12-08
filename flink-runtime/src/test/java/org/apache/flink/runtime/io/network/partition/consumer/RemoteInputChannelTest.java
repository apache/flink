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

import com.google.common.collect.Lists;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel.CapacityAvailabilityListener;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteInputChannelTest {

	@Test
	public void testExceptionOnReordering() throws Exception {
		// Setup
		final SingleInputGate inputGate = mock(SingleInputGate.class);
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		final CapacityAvailabilityListener capacityListener = mock(CapacityAvailabilityListener.class);

		// The test
		assertTrue(inputChannel.onBuffer(TestBufferFactory.getMockBuffer(), 0, capacityListener));

		// This does not yet throw the exception, but sets the error at the channel.
		assertTrue(inputChannel.onBuffer(TestBufferFactory.getMockBuffer(), 29, capacityListener));

		try {
			inputChannel.getNextBuffer();

			fail("Did not throw expected exception after enqueuing an out-of-order buffer.");
		}
		catch (Exception expected) {
			// expected
		}

		// Need to notify the input gate for the out-of-order buffer as well. Otherwise the
		// receiving task will not notice the error.
		verify(inputGate, times(2)).notifyChannelNonEmpty(eq(inputChannel));
	}

	@Test
	public void testConcurrentOnBufferAndRelease() throws Exception {
		// Config
		// Repeatedly spawn two tasks: one to queue buffers and the other to release the channel
		// concurrently. We do this repeatedly to provoke races.
		final int numberOfRepetitions = 8192;

		// Setup
		final ExecutorService executor = Executors.newFixedThreadPool(2);
		final CapacityAvailabilityListener capacityListener = mock(CapacityAvailabilityListener.class);

		try {
			// Test
			final SingleInputGate inputGate = mock(SingleInputGate.class);

			for (int i = 0; i < numberOfRepetitions; i++) {
				final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);

				final Callable<Void> enqueueTask = new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						while (true) {
							for (int j = 0; j < 128; j++) {
								inputChannel.onBuffer(TestBufferFactory.getMockBuffer(), j, capacityListener);
							}

							if (inputChannel.isReleased()) {
								return null;
							}
						}
					}
				};

				final Callable<Void> releaseTask = new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						inputChannel.releaseAllResources();

						return null;
					}
				};

				// Submit tasks and wait to finish
				List<Future<Void>> results = Lists.newArrayListWithCapacity(2);

				results.add(executor.submit(enqueueTask));
				results.add(executor.submit(releaseTask));

				for (Future<Void> result : results) {
					result.get();
				}

				assertEquals("Resource leak during concurrent release and enqueue.",
						0, inputChannel.getNumberOfQueuedBuffers());
			}
		}
		finally {
			executor.shutdown();
		}
	}

	@Test(expected = IllegalStateException.class)
	public void testRetriggerWithoutPartitionRequest() throws Exception {
		Tuple2<Integer, Integer> backoff = new Tuple2<Integer, Integer>(500, 3000);
		PartitionRequestClient connClient = mock(PartitionRequestClient.class);
		SingleInputGate inputGate = mock(SingleInputGate.class);

		RemoteInputChannel ch = createRemoteInputChannel(inputGate, connClient, backoff);

		ch.retriggerSubpartitionRequest(0);
	}

	@Test
	public void testPartitionRequestExponentialBackoff() throws Exception {
		// Config
		Tuple2<Integer, Integer> backoff = new Tuple2<Integer, Integer>(500, 3000);

		// Start with initial backoff, then keep doubling, and cap at max.
		int[] expectedDelays = {backoff._1(), 1000, 2000, backoff._2()};

		// Setup
		PartitionRequestClient connClient = mock(PartitionRequestClient.class);
		SingleInputGate inputGate = mock(SingleInputGate.class);

		RemoteInputChannel ch = createRemoteInputChannel(inputGate, connClient, backoff);

		// Initial request
		ch.requestSubpartition(0);
		verify(connClient).requestSubpartition(eq(ch.partitionId), eq(0), eq(ch), eq(0));

		// Request subpartition and verify that the actual requests are delayed.
		for (int expected : expectedDelays) {
			ch.retriggerSubpartitionRequest(0);

			verify(connClient).requestSubpartition(eq(ch.partitionId), eq(0), eq(ch), eq(expected));
		}

		// Exception after backoff is greater than the maximum backoff.
		try {
			ch.retriggerSubpartitionRequest(0);
			ch.getNextBuffer();
			fail("Did not throw expected exception.");
		}
		catch (Exception expected) {
		}
	}

	@Test
	public void testPartitionRequestSingleBackoff() throws Exception {
		// Config
		Tuple2<Integer, Integer> backoff = new Tuple2<Integer, Integer>(500, 500);

		// Setup
		PartitionRequestClient connClient = mock(PartitionRequestClient.class);
		SingleInputGate inputGate = mock(SingleInputGate.class);

		RemoteInputChannel ch = createRemoteInputChannel(inputGate, connClient, backoff);

		// No delay for first request
		ch.requestSubpartition(0);
		verify(connClient).requestSubpartition(eq(ch.partitionId), eq(0), eq(ch), eq(0));

		// Initial delay for second request
		ch.retriggerSubpartitionRequest(0);
		verify(connClient).requestSubpartition(eq(ch.partitionId), eq(0), eq(ch), eq(backoff._1()));

		// Exception after backoff is greater than the maximum backoff.
		try {
			ch.retriggerSubpartitionRequest(0);
			ch.getNextBuffer();
			fail("Did not throw expected exception.");
		}
		catch (Exception expected) {
		}
	}

	@Test
	public void testPartitionRequestNoBackoff() throws Exception {
		// Config
		Tuple2<Integer, Integer> backoff = new Tuple2<Integer, Integer>(0, 0);

		// Setup
		PartitionRequestClient connClient = mock(PartitionRequestClient.class);
		SingleInputGate inputGate = mock(SingleInputGate.class);

		RemoteInputChannel ch = createRemoteInputChannel(inputGate, connClient, backoff);

		// No delay for first request
		ch.requestSubpartition(0);
		verify(connClient).requestSubpartition(eq(ch.partitionId), eq(0), eq(ch), eq(0));

		// Exception, because backoff is disabled.
		try {
			ch.retriggerSubpartitionRequest(0);
			ch.getNextBuffer();
			fail("Did not throw expected exception.");
		}
		catch (Exception expected) {
		}
	}

	@Test
	public void testOnFailedPartitionRequest() throws Exception {
		final ConnectionManager connectionManager = mock(ConnectionManager.class);
		when(connectionManager.createPartitionRequestClient(any(ConnectionID.class)))
				.thenReturn(mock(PartitionRequestClient.class));

		final ResultPartitionID partitionId = new ResultPartitionID();

		final SingleInputGate inputGate = mock(SingleInputGate.class);

		final RemoteInputChannel ch = new RemoteInputChannel(
				inputGate,
				0,
				partitionId,
				mock(ConnectionID.class),
				connectionManager,
				new Tuple2<>(0, 0),
				new UnregisteredTaskMetricsGroup.DummyIOMetricGroup(),
				0);

		ch.onFailedPartitionRequest();

		verify(inputGate).triggerPartitionStateCheck(eq(partitionId));
	}

	@Test(expected = CancelTaskException.class)
	public void testProducerFailedException() throws Exception {

		ConnectionManager connManager = mock(ConnectionManager.class);
		when(connManager.createPartitionRequestClient(any(ConnectionID.class)))
				.thenReturn(mock(PartitionRequestClient.class));

		final RemoteInputChannel ch = new RemoteInputChannel(
				mock(SingleInputGate.class),
				0,
				new ResultPartitionID(),
				mock(ConnectionID.class),
				connManager,
				new Tuple2<>(0, 0),
				new UnregisteredTaskMetricsGroup.DummyIOMetricGroup(),
				0);

		ch.onError(new ProducerFailedException(new RuntimeException("Expected test exception.")));

		ch.requestSubpartition(0);

		// Should throw an instance of CancelTaskException.
		ch.getNextBuffer();
	}

	/**
	 * Tests the capacity limit when queuing and consuming buffers.
	 *
	 * Queueing should respect the capacity limit for regular buffers, but
	 * be ignored for event buffers. The callback should happen when we
	 * fall back under the capacity limit.
	 */
	@Test
	public void testCapacityLimit() throws Exception {
		int capacityLimit = 3;

		ConnectionManager connManager = mock(ConnectionManager.class);
		when(connManager.createPartitionRequestClient(any(ConnectionID.class)))
			.thenReturn(mock(PartitionRequestClient.class));

		RemoteInputChannel channel = new RemoteInputChannel(
			mock(SingleInputGate.class),
			0,
			new ResultPartitionID(),
			mock(ConnectionID.class),
			connManager,
			new Tuple2<>(0, 0),
			new UnregisteredTaskMetricsGroup.DummyIOMetricGroup(),
			capacityLimit);

		// Fake request to satisfy internal channel logic
		channel.requestSubpartition(0);

		Buffer buffer = TestBufferFactory.getMockBuffer();
		CapacityAvailabilityListener callback = mock(CapacityAvailabilityListener.class);

		int seqNo = 0;

		// Queue as many regular bufffers as allowed
		for (int i = 0; i < capacityLimit; i++) {
			assertTrue(channel.onBuffer(buffer, seqNo++, callback));
		}

		// This buffer is at the limit. Don't increment the sequence number.
		assertFalse(channel.onBuffer(buffer, seqNo, callback));

		// Events should be allowed though
		Buffer eventBuffer = EventSerializer.toBuffer(new CheckpointBarrier(19292, 29292929));
		assertTrue(channel.onBuffer(eventBuffer, seqNo++, callback));
		assertTrue(channel.onBuffer(eventBuffer, seqNo++, callback));
		assertTrue(channel.onBuffer(eventBuffer, seqNo++, callback));

		// But really, no regular buffers, please
		assertFalse(channel.onBuffer(buffer, seqNo, callback));

		// The listener should only be notified when we are past
		// the capacity and fall below it, which didn't happen so far
		verify(callback, times(0)).capacityAvailable();

		// Start consuming
		// We have queued 6 buffers in total (3 regular buffers and 3 event buffers)
		InputChannel.BufferAndAvailability baa;

		// Consume buffer (5 remaining afterwards)
		baa = channel.getNextBuffer();
		assertTrue(baa.moreAvailable());
		assertEquals(buffer, baa.buffer());

		// Callback should not happen => 5 buffers remaining
		verify(callback, times(0)).capacityAvailable();

		// Consume buffer (4 remaining afterwards)
		baa = channel.getNextBuffer();
		assertTrue(baa.moreAvailable());
		assertEquals(buffer, baa.buffer());

		// Callback should not happen => 4 buffers remaining
		verify(callback, times(0)).capacityAvailable();

		// Consume buffer (3 remaining afterwards)
		baa = channel.getNextBuffer();
		assertTrue(baa.moreAvailable());
		assertEquals(buffer, baa.buffer());

		// Callback should not happen => 3 buffers remaining (still at limit)
		verify(callback, times(0)).capacityAvailable();

		// Consume buffer (2 remaining afterwards)
		baa = channel.getNextBuffer();
		assertTrue(baa.moreAvailable());
		assertEquals(eventBuffer, baa.buffer());

		// Callback should have happened now => we went below the limit
		verify(callback, times(1)).capacityAvailable();

		// Now we have space for another buffer => 3 buffers again in total
		assertTrue(channel.onBuffer(buffer, seqNo++, callback));

		// Consume buffer (2 remaining afterwards)
		baa = channel.getNextBuffer();
		assertTrue(baa.moreAvailable());
		assertEquals(eventBuffer, baa.buffer());

		// Consume buffer (1 remaining afterwards)
		baa = channel.getNextBuffer();
		assertTrue(baa.moreAvailable());
		assertEquals(eventBuffer, baa.buffer());

		// Consume buffer (0 remaining afterwards)
		baa = channel.getNextBuffer();
		assertFalse(baa.moreAvailable());
		assertEquals(buffer, baa.buffer());

		// We went over the limit _once_. Hence, only a single callback should have happened.
		verify(callback, times(1)).capacityAvailable();
	}

	/**
	 * Tests that the callback is cleared after falling under the limit.
	 */
	@Test
	public void testCallbackClearedAfterCall() throws Exception {
		int capacityLimit = 3;

		ConnectionManager connManager = mock(ConnectionManager.class);
		when(connManager.createPartitionRequestClient(any(ConnectionID.class)))
			.thenReturn(mock(PartitionRequestClient.class));

		RemoteInputChannel channel = new RemoteInputChannel(
			mock(SingleInputGate.class),
			0,
			new ResultPartitionID(),
			mock(ConnectionID.class),
			connManager,
			new Tuple2<>(0, 0),
			new UnregisteredTaskMetricsGroup.DummyIOMetricGroup(),
			capacityLimit);

		// Fake request to satisfy internal channel logic
		channel.requestSubpartition(0);

		Buffer buffer = TestBufferFactory.getMockBuffer();
		CapacityAvailabilityListener callback = mock(CapacityAvailabilityListener.class);

		int seqNo = 0;

		// Queue high as limit
		for (int i = 0; i < capacityLimit; i++) {
			assertTrue(channel.onBuffer(buffer, seqNo++, callback));
		}
		assertFalse(channel.onBuffer(buffer, seqNo, callback));

		// Consume
		InputChannel.BufferAndAvailability baa;
		baa = channel.getNextBuffer();
		assertTrue(baa.moreAvailable());
		assertEquals(buffer, baa.buffer());

		// Callback should have happened now => we went below the limit
		verify(callback, times(1)).capacityAvailable();

		// Queue again (hit the limit)
		assertTrue(channel.onBuffer(buffer, seqNo++, null));
		assertFalse(channel.onBuffer(buffer, seqNo, null));

		// Consume again
		baa = channel.getNextBuffer();
		assertTrue(baa.moreAvailable());
		assertEquals(buffer, baa.buffer());

		// Callback should not have happened again since we queued
		// with null listener.
		verify(callback, times(1)).capacityAvailable();
	}

	// ---------------------------------------------------------------------------------------------

	private RemoteInputChannel createRemoteInputChannel(SingleInputGate inputGate)
			throws IOException, InterruptedException {

		return createRemoteInputChannel(
				inputGate, mock(PartitionRequestClient.class), new Tuple2<Integer, Integer>(0, 0));
	}

	private RemoteInputChannel createRemoteInputChannel(
			SingleInputGate inputGate,
			PartitionRequestClient partitionRequestClient,
			Tuple2<Integer, Integer> initialAndMaxRequestBackoff)
			throws IOException, InterruptedException {

		final ConnectionManager connectionManager = mock(ConnectionManager.class);
		when(connectionManager.createPartitionRequestClient(any(ConnectionID.class)))
				.thenReturn(partitionRequestClient);

		return new RemoteInputChannel(
				inputGate,
				0,
				new ResultPartitionID(),
				mock(ConnectionID.class),
				connectionManager,
				initialAndMaxRequestBackoff,
				new UnregisteredTaskMetricsGroup.DummyIOMetricGroup(),
				0);
	}
}
