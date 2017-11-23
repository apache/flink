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
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;

import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteInputChannelTest {

	@Test
	public void testExceptionOnReordering() throws Exception {
		// Setup
		final SingleInputGate inputGate = mock(SingleInputGate.class);
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		final Buffer buffer = TestBufferFactory.createBuffer();

		// The test
		inputChannel.onBuffer(buffer.retain(), 0, -1);

		// This does not yet throw the exception, but sets the error at the channel.
		inputChannel.onBuffer(buffer, 29, -1);

		try {
			inputChannel.getNextBuffer();

			fail("Did not throw expected exception after enqueuing an out-of-order buffer.");
		}
		catch (Exception expected) {
			assertFalse(buffer.isRecycled());
			// free remaining buffer instances
			inputChannel.releaseAllResources();
			assertTrue(buffer.isRecycled());
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
		final Buffer buffer = TestBufferFactory.createBuffer();

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
								// this is the same buffer over and over again which will be
								// recycled by the RemoteInputChannel
								inputChannel.onBuffer(buffer.retain(), j, -1);
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
			assertFalse(buffer.isRecycled());
			buffer.recycle();
			assertTrue(buffer.isRecycled());
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
				new UnregisteredTaskMetricsGroup.DummyTaskIOMetricGroup());

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
				new UnregisteredTaskMetricsGroup.DummyTaskIOMetricGroup());

		ch.onError(new ProducerFailedException(new RuntimeException("Expected test exception.")));

		ch.requestSubpartition(0);

		// Should throw an instance of CancelTaskException.
		ch.getNextBuffer();
	}

	/**
	 * Tests to verify that the input channel requests floating buffers from buffer pool for
	 * maintaining (backlog + initialCredit) available buffers once receiving the sender's backlog.
	 *
	 * <p>Verifies the logic of recycling floating buffer back into the input channel and the logic
	 * of returning extra floating buffer into the buffer pool during recycling exclusive buffer.
	 */
	@Test
	public void testRequestAndReturnFloatingBuffer() throws Exception {
		// Setup
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(14, 32, MemoryType.HEAP);
		final int numExclusiveBuffers = 2;
		final int numFloatingBuffers = 12;

		final SingleInputGate inputGate = createSingleInputGate();
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		inputGate.setInputChannel(inputChannel.partitionId.getPartitionId(), inputChannel);
		try {
			final BufferPool bufferPool = spy(networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers));
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments(networkBufferPool, numExclusiveBuffers);

			// Prepare the exclusive and floating buffers to verify recycle logic later
			Buffer exclusiveBuffer = inputChannel.requestBuffer();
			assertNotNull(exclusiveBuffer);
			Buffer floatingBuffer1 = bufferPool.requestBuffer();
			assertNotNull(floatingBuffer1);
			Buffer floatingBuffer2 = bufferPool.requestBuffer();
			assertNotNull(floatingBuffer2);

			// Receive the producer's backlog less than the number of available floating buffers
			inputChannel.onSenderBacklog(8);

			// Request the floating buffers to maintain (backlog + initialCredit) available buffers
			verify(bufferPool, times(11)).requestBuffer();
			verify(bufferPool, times(0)).addBufferListener(inputChannel);
			assertEquals("There should be 10 buffers available in the channel", 10, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 10 buffers required in the channel", 10, inputChannel.getNumberOfRequiredBuffers());

			// Increase the backlog to exceed the number of available floating buffers
			inputChannel.onSenderBacklog(10);

			// The channel does not get enough floating buffer and register as buffer listener
			verify(bufferPool, times(13)).requestBuffer();
			verify(bufferPool, times(1)).addBufferListener(inputChannel);
			assertEquals("There should be 11 buffers available in the channel", 11, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 12 buffers required in the channel", 12, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffer available in local pool", 0, bufferPool.getNumberOfAvailableMemorySegments());

			// Continue increasing the backlog
			inputChannel.onSenderBacklog(11);

			// The channel is already in the status of waiting for buffers and will not request any more
			verify(bufferPool, times(13)).requestBuffer();
			verify(bufferPool, times(1)).addBufferListener(inputChannel);
			assertEquals("There should be 11 buffers available in the channel", 11, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 13 buffers required in the channel", 13, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffer available in local pool", 0, bufferPool.getNumberOfAvailableMemorySegments());

			// Recycle the floating buffer and assign it to the buffer listener
			floatingBuffer1.recycle();

			// The channel is still waiting for one more floating buffer
			assertEquals("There should be 12 buffers available in the channel", 12, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 13 buffers required in the channel", 13, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffer available in local pool", 0, bufferPool.getNumberOfAvailableMemorySegments());

			// Recycle one more floating buffer again
			floatingBuffer2.recycle();

			// The channel already gets all the required buffers
			assertEquals("There should be 13 buffers available in the channel", 13, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 13 buffers required in the channel", 13, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffer available in local pool", 0, bufferPool.getNumberOfAvailableMemorySegments());

			// Decrease the backlog and recycle one exclusive buffer
			inputChannel.onSenderBacklog(10);
			exclusiveBuffer.recycle();

			// Return one floating buffer if the number of available buffers is more than required buffers
			assertEquals("There should be 13 buffers available in the channel", 13, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 12 buffers required in the channel", 12, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 1 buffer available in local pool", 1, bufferPool.getNumberOfAvailableMemorySegments());

		} finally {
			// Release all the buffer resources
			inputChannel.releaseAllResources();

			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();
		}
	}

	/**
	 * Tests to verify that the buffer pool will distribute available floating buffers among
	 * all the channel listeners in a fair way.
	 */
	@Test
	public void testFairDistributionFloatingBuffers() throws Exception {
		// Setup
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(12, 32, MemoryType.HEAP);
		final int numExclusiveBuffers = 2;
		final int numFloatingBuffers = 3;

		final SingleInputGate inputGate = createSingleInputGate();
		final RemoteInputChannel channel1 = spy(createRemoteInputChannel(inputGate));
		final RemoteInputChannel channel2 = spy(createRemoteInputChannel(inputGate));
		final RemoteInputChannel channel3 = spy(createRemoteInputChannel(inputGate));
		inputGate.setInputChannel(channel1.partitionId.getPartitionId(), channel1);
		inputGate.setInputChannel(channel2.partitionId.getPartitionId(), channel2);
		inputGate.setInputChannel(channel3.partitionId.getPartitionId(), channel3);
		try {
			final BufferPool bufferPool = spy(networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers));
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments(networkBufferPool, numExclusiveBuffers);

			// Exhaust all the floating buffers
			final List<Buffer> floatingBuffers = new ArrayList<>(numFloatingBuffers);
			for (int i = 0; i < numFloatingBuffers; i++) {
				Buffer buffer = bufferPool.requestBuffer();
				assertNotNull(buffer);
				floatingBuffers.add(buffer);
			}

			// Receive the producer's backlog to trigger request floating buffers from pool
			// and register as listeners as a result
			channel1.onSenderBacklog(8);
			channel2.onSenderBacklog(8);
			channel3.onSenderBacklog(8);

			verify(bufferPool, times(1)).addBufferListener(channel1);
			verify(bufferPool, times(1)).addBufferListener(channel2);
			verify(bufferPool, times(1)).addBufferListener(channel3);
			assertEquals("There should be " + numExclusiveBuffers + " buffers available in the channel",
				numExclusiveBuffers, channel1.getNumberOfAvailableBuffers());
			assertEquals("There should be " + numExclusiveBuffers + " buffers available in the channel",
				numExclusiveBuffers, channel2.getNumberOfAvailableBuffers());
			assertEquals("There should be " + numExclusiveBuffers + " buffers available in the channel",
				numExclusiveBuffers, channel3.getNumberOfAvailableBuffers());

			// Recycle three floating buffers to trigger notify buffer available
			for (Buffer buffer : floatingBuffers) {
				buffer.recycle();
			}

			verify(channel1, times(1)).notifyBufferAvailable(any(Buffer.class));
			verify(channel2, times(1)).notifyBufferAvailable(any(Buffer.class));
			verify(channel3, times(1)).notifyBufferAvailable(any(Buffer.class));
			assertEquals("There should be 3 buffers available in the channel", 3, channel1.getNumberOfAvailableBuffers());
			assertEquals("There should be 3 buffers available in the channel", 3, channel2.getNumberOfAvailableBuffers());
			assertEquals("There should be 3 buffers available in the channel", 3, channel3.getNumberOfAvailableBuffers());

		} finally {
			// Release all the buffer resources
			channel1.releaseAllResources();
			channel2.releaseAllResources();
			channel3.releaseAllResources();

			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();
		}
	}

	/**
	 * Tests to verify that there is no race condition with two things running in parallel:
	 * requesting floating buffers on sender backlog and some other thread releasing
	 * the input channel.
	 */
	@Test
	public void testConcurrentOnSenderBacklogAndRelease() throws Exception {
		// Setup
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(130, 32, MemoryType.HEAP);
		final int numExclusiveBuffers = 2;
		final int numFloatingBuffers = 128;

		final ExecutorService executor = Executors.newFixedThreadPool(2);

		final SingleInputGate inputGate = createSingleInputGate();
		final RemoteInputChannel inputChannel  = createRemoteInputChannel(inputGate);
		inputGate.setInputChannel(inputChannel.partitionId.getPartitionId(), inputChannel);
		try {
			final BufferPool bufferPool = networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments(networkBufferPool, numExclusiveBuffers);

			final Callable requestBufferTask = new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					while (true) {
						for (int j = 1; j <= numFloatingBuffers; j++) {
							inputChannel.onSenderBacklog(j);
						}

						if (inputChannel.isReleased()) {
							return null;
						}
					}
				}
			};

			final Callable releaseTask = new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					inputChannel.releaseAllResources();

					return null;
				}
			};

			// Submit tasks and wait to finish
			submitTasksAndWaitResults(executor, new Callable[]{requestBufferTask, releaseTask});

			assertEquals("There should be no buffers available in the channel.",
				0, inputChannel.getNumberOfAvailableBuffers());
		} finally {
			// Release all the buffer resources once exception
			if (!inputChannel.isReleased()) {
				inputChannel.releaseAllResources();
			}

			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();

			executor.shutdown();
		}
	}

	/**
	 * Tests to verify that there is no race condition with two things running in parallel:
	 * requesting floating buffers on sender backlog and some other thread recycling
	 * floating or exclusive buffers.
	 */
	@Test
	public void testConcurrentOnSenderBacklogAndRecycle() throws Exception {
		// Setup
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(248, 32, MemoryType.HEAP);
		final int numExclusiveSegments = 120;
		final int numFloatingBuffers = 128;
		final int backlog = 128;

		final ExecutorService executor = Executors.newFixedThreadPool(3);

		final SingleInputGate inputGate = createSingleInputGate();
		final RemoteInputChannel inputChannel  = createRemoteInputChannel(inputGate);
		inputGate.setInputChannel(inputChannel.partitionId.getPartitionId(), inputChannel);
		try {
			final BufferPool bufferPool = networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments(networkBufferPool, numExclusiveSegments);

			final Callable requestBufferTask = new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					for (int j = 1; j <= backlog; j++) {
						inputChannel.onSenderBacklog(j);
					}

					return null;
				}
			};

			// Submit tasks and wait to finish
			submitTasksAndWaitResults(executor, new Callable[]{
				recycleExclusiveBufferTask(inputChannel, numExclusiveSegments),
				recycleFloatingBufferTask(bufferPool, numFloatingBuffers),
				requestBufferTask});

			assertEquals("There should be " + inputChannel.getNumberOfRequiredBuffers() +" buffers available in channel.",
				inputChannel.getNumberOfRequiredBuffers(), inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be no buffers available in local pool.",
				0, bufferPool.getNumberOfAvailableMemorySegments());

		} finally {
			// Release all the buffer resources
			inputChannel.releaseAllResources();

			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();

			executor.shutdown();
		}
	}

	/**
	 * Tests to verify that there is no race condition with two things running in parallel:
	 * recycling the exclusive or floating buffers and some other thread releasing the
	 * input channel.
	 */
	@Test
	public void testConcurrentRecycleAndRelease() throws Exception {
		// Setup
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(248, 32, MemoryType.HEAP);
		final int numExclusiveSegments = 120;
		final int numFloatingBuffers = 128;

		final ExecutorService executor = Executors.newFixedThreadPool(3);

		final SingleInputGate inputGate = createSingleInputGate();
		final RemoteInputChannel inputChannel  = createRemoteInputChannel(inputGate);
		inputGate.setInputChannel(inputChannel.partitionId.getPartitionId(), inputChannel);
		try {
			final BufferPool bufferPool = networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments(networkBufferPool, numExclusiveSegments);

			final Callable releaseTask = new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					inputChannel.releaseAllResources();

					return null;
				}
			};

			// Submit tasks and wait to finish
			submitTasksAndWaitResults(executor, new Callable[]{
				recycleExclusiveBufferTask(inputChannel, numExclusiveSegments),
				recycleFloatingBufferTask(bufferPool, numFloatingBuffers),
				releaseTask});

			assertEquals("There should be no buffers available in the channel.",
				0, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be " + numFloatingBuffers + " buffers available in local pool.",
				numFloatingBuffers, bufferPool.getNumberOfAvailableMemorySegments());
			assertEquals("There should be " + numExclusiveSegments + " buffers available in global pool.",
				numExclusiveSegments, networkBufferPool.getNumberOfAvailableMemorySegments());

		} finally {
			// Release all the buffer resources once exception
			if (!inputChannel.isReleased()) {
				inputChannel.releaseAllResources();
			}

			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();

			executor.shutdown();
		}
	}

	// ---------------------------------------------------------------------------------------------

	private SingleInputGate createSingleInputGate() {
		return new SingleInputGate(
			"InputGate",
			new JobID(),
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED_CREDIT_BASED,
			0,
			1,
			mock(TaskActions.class),
			new UnregisteredTaskMetricsGroup.DummyTaskIOMetricGroup());
	}

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
			initialAndMaxRequestBackoff._1(),
			initialAndMaxRequestBackoff._2(),
			new UnregisteredTaskMetricsGroup.DummyTaskIOMetricGroup());
	}

	private Callable recycleExclusiveBufferTask(RemoteInputChannel inputChannel, int numExclusiveSegments) {
		final List<Buffer> exclusiveBuffers = new ArrayList<>(numExclusiveSegments);
		// Exhaust all the exclusive buffers
		for (int i = 0; i < numExclusiveSegments; i++) {
			Buffer buffer = inputChannel.requestBuffer();
			assertNotNull(buffer);
			exclusiveBuffers.add(buffer);
		}

		return new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				for (Buffer buffer : exclusiveBuffers) {
					buffer.recycle();
				}

				return null;
			}
		};
	}

	private Callable recycleFloatingBufferTask(BufferPool bufferPool, int numFloatingBuffers) throws Exception {
		final List<Buffer> floatingBuffers = new ArrayList<>(numFloatingBuffers);
		// Exhaust all the floating buffers
		for (int i = 0; i < numFloatingBuffers; i++) {
			Buffer buffer = bufferPool.requestBuffer();
			assertNotNull(buffer);
			floatingBuffers.add(buffer);
		}

		return new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				for (Buffer buffer : floatingBuffers) {
					buffer.recycle();
				}

				return null;
			}
		};
	}

	private void submitTasksAndWaitResults(ExecutorService executor, Callable[] tasks) throws Exception {
		final List<Future> results = Lists.newArrayListWithCapacity(tasks.length);

		for(Callable task : tasks) {
			results.add(executor.submit(task));
		}

		for (Future result : results) {
			result.get();
		}
	}
}
