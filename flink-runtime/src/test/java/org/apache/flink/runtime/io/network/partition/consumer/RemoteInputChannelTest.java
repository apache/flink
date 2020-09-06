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

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.TestingPartitionRequestClient;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferListener.NotificationResult;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TestTaskBuilder;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT;
import static org.apache.flink.runtime.io.network.api.serialization.EventSerializer.toBuffer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSingleBuffer;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RemoteInputChannel}.
 */
public class RemoteInputChannelTest {

	@Test
	public void testExceptionOnReordering() throws Exception {
		// Setup
		final SingleInputGate inputGate = createSingleInputGate(1);
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		final Buffer buffer = TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE);

		// The test
		inputChannel.onBuffer(buffer.retainBuffer(), 0, -1);

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
	}

	@Test
	public void testConcurrentOnBufferAndRelease() throws Exception {
		testConcurrentReleaseAndSomething(8192, (inputChannel, buffer, j) -> {
			inputChannel.onBuffer(buffer, j, -1);
			return null;
		});
	}

	@Test
	public void testConcurrentNotifyBufferAvailableAndRelease() throws Exception {
		testConcurrentReleaseAndSomething(1024, (inputChannel, buffer, j) ->
			inputChannel.getBufferManager().notifyBufferAvailable(buffer)
		);
	}

	private interface TriFunction<T, U, V, R> {
		R apply(T t, U u, V v) throws Exception;
	}

	/**
	 * Repeatedly spawns two tasks: one to call <tt>function</tt> and the other to release the
	 * channel concurrently. We do this repeatedly to provoke races.
	 *
	 * @param numberOfRepetitions how often to repeat the test
	 * @param function function to call concurrently to {@link RemoteInputChannel#releaseAllResources()}
	 */
	private void testConcurrentReleaseAndSomething(
			final int numberOfRepetitions,
			TriFunction<RemoteInputChannel, Buffer, Integer, Object> function) throws Exception {

		// Setup
		final ExecutorService executor = Executors.newFixedThreadPool(2);
		final Buffer buffer = TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE);

		try {
			// Test
			final SingleInputGate inputGate = createSingleInputGate(1);

			for (int i = 0; i < numberOfRepetitions; i++) {
				final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);

				final Callable<Void> enqueueTask = () -> {
					while (true) {
						for (int j = 0; j < 128; j++) {
							// this is the same buffer over and over again which will be
							// recycled by the RemoteInputChannel
							Object obj = function.apply(inputChannel, buffer.retainBuffer(), j);
							if (obj instanceof NotificationResult && obj == NotificationResult.BUFFER_NOT_USED) {
								buffer.recycleBuffer();
							}
						}

						if (inputChannel.isReleased()) {
							return null;
						}
					}
				};

				final Callable<Void> releaseTask = () -> {
					inputChannel.releaseAllResources();
					return null;
				};

				// Submit tasks and wait to finish
				List<Future<Void>> results = Lists.newArrayListWithCapacity(2);

				results.add(executor.submit(enqueueTask));
				results.add(executor.submit(releaseTask));

				for (Future<Void> result : results) {
					result.get();
				}

				assertEquals("Resource leak during concurrent release and notifyBufferAvailable.",
					0, inputChannel.getNumberOfQueuedBuffers());
			}
		}
		finally {
			executor.shutdown();
			assertFalse(buffer.isRecycled());
			buffer.recycleBuffer();
			assertTrue(buffer.isRecycled());
		}
	}

	@Test(expected = IllegalStateException.class)
	public void testRetriggerWithoutPartitionRequest() throws Exception {
		SingleInputGate inputGate = createSingleInputGate(1);

		RemoteInputChannel ch = createRemoteInputChannel(inputGate, 500, 3000);

		ch.retriggerSubpartitionRequest(0);
	}

	@Test
	public void testPartitionRequestExponentialBackoff() throws Exception {
		// Start with initial backoff, then keep doubling, and cap at max.
		int[] expectedDelays = {500, 1000, 2000, 3000};

		// Setup
		SingleInputGate inputGate = createSingleInputGate(1);
		ResultPartitionID partitionId = new ResultPartitionID();
		TestVerifyPartitionRequestClient client = new TestVerifyPartitionRequestClient();
		ConnectionManager connectionManager = new TestVerifyConnectionManager(client);
		RemoteInputChannel ch = createRemoteInputChannel(inputGate, connectionManager, partitionId, 500, 3000);

		// Initial request
		ch.requestSubpartition(0);
		client.verifyResult(partitionId, 0, 0);

		// Request subpartition and verify that the actual requests are delayed.
		for (int expected : expectedDelays) {
			ch.retriggerSubpartitionRequest(0);

			client.verifyResult(partitionId, 0, expected);
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
		// Setup
		SingleInputGate inputGate = createSingleInputGate(1);
		ResultPartitionID partitionId = new ResultPartitionID();
		TestVerifyPartitionRequestClient client = new TestVerifyPartitionRequestClient();
		ConnectionManager connectionManager = new TestVerifyConnectionManager(client);
		RemoteInputChannel ch = createRemoteInputChannel(inputGate, connectionManager, partitionId, 500, 500);

		// No delay for first request
		ch.requestSubpartition(0);
		client.verifyResult(partitionId, 0, 0);

		// Initial delay for second request
		ch.retriggerSubpartitionRequest(0);
		client.verifyResult(partitionId, 0, 500);

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
		// Setup
		SingleInputGate inputGate = createSingleInputGate(1);
		ResultPartitionID partitionId = new ResultPartitionID();
		TestVerifyPartitionRequestClient client = new TestVerifyPartitionRequestClient();
		ConnectionManager connectionManager = new TestVerifyConnectionManager(client);
		RemoteInputChannel ch = createRemoteInputChannel(inputGate, connectionManager, partitionId, 0, 0);

		// No delay for first request
		ch.requestSubpartition(0);
		client.verifyResult(partitionId, 0, 0);

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
		final ResultPartitionID partitionId = new ResultPartitionID();
		final TestPartitionProducerStateProvider provider = new TestPartitionProducerStateProvider(partitionId);
		final SingleInputGate inputGate = new SingleInputGateBuilder()
			.setPartitionProducerStateProvider(provider)
			.build();
		final RemoteInputChannel ch = InputChannelBuilder.newBuilder()
			.setPartitionId(partitionId)
			.buildRemoteChannel(inputGate);

		ch.onFailedPartitionRequest();

		assertTrue(provider.isInvoked());
	}

	@Test(expected = CancelTaskException.class)
	public void testProducerFailedException() throws Exception {

		ConnectionManager connManager = mock(ConnectionManager.class);
		when(connManager.createPartitionRequestClient(any(ConnectionID.class)))
				.thenReturn(mock(PartitionRequestClient.class));

		final SingleInputGate gate = createSingleInputGate(1);
		final RemoteInputChannel ch = InputChannelTestUtils.createRemoteInputChannel(gate, 0, connManager);

		ch.onError(new ProducerFailedException(new RuntimeException("Expected test exception.")));

		ch.requestSubpartition(0);

		// Should throw an instance of CancelTaskException.
		ch.getNextBuffer();
	}

	@Test(expected = PartitionConnectionException.class)
	public void testPartitionConnectionException() throws IOException {
		final ConnectionManager connManager = new TestingExceptionConnectionManager();
		final SingleInputGate gate = createSingleInputGate(1);
		final RemoteInputChannel ch = InputChannelTestUtils.createRemoteInputChannel(gate, 0, connManager);
		gate.setInputChannels(ch);

		gate.requestPartitions();

		ch.getNextBuffer();
	}

	/**
	 * Tests to verify the behaviours of three different processes if the number of available
	 * buffers is less than required buffers.
	 *
	 * <ol>
	 * <li>Recycle the floating buffer</li>
	 * <li>Recycle the exclusive buffer</li>
	 * <li>Decrease the sender's backlog</li>
	 * </ol>
	 */
	@Test
	public void testAvailableBuffersLessThanRequiredBuffers() throws Exception {
		// Setup
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(16, 32);
		final int numFloatingBuffers = 14;

		final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		inputGate.setInputChannels(inputChannel);
		Throwable thrown = null;
		try {
			final BufferPool bufferPool = spy(networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers));
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments();
			inputChannel.requestSubpartition(0);

			// Prepare the exclusive and floating buffers to verify recycle logic later
			final Buffer exclusiveBuffer = inputChannel.requestBuffer();
			assertNotNull(exclusiveBuffer);

			final int numRecycleFloatingBuffers = 2;
			final ArrayDeque<Buffer> floatingBufferQueue = new ArrayDeque<>(numRecycleFloatingBuffers);
			for (int i = 0; i < numRecycleFloatingBuffers; i++) {
				Buffer floatingBuffer = bufferPool.requestBuffer();
				assertNotNull(floatingBuffer);
				floatingBufferQueue.add(floatingBuffer);
			}

			verify(bufferPool, times(numRecycleFloatingBuffers)).requestBuffer();

			// Receive the producer's backlog more than the number of available floating buffers
			inputChannel.onSenderBacklog(14);

			// The channel requests (backlog + numExclusiveBuffers) floating buffers from local pool.
			// It does not get enough floating buffers and register as buffer listener
			verify(bufferPool, times(15)).requestBuffer();
			verify(bufferPool, times(1)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 13 buffers available in the channel",
				13, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 16 buffers required in the channel",
				16, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffers available in local pool",
				0, bufferPool.getNumberOfAvailableMemorySegments());
			assertTrue(inputChannel.isWaitingForFloatingBuffers());

			// Increase the backlog
			inputChannel.onSenderBacklog(16);

			// The channel is already in the status of waiting for buffers and will not request any more
			verify(bufferPool, times(15)).requestBuffer();
			verify(bufferPool, times(1)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 13 buffers available in the channel",
				13, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 18 buffers required in the channel",
				18, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffers available in local pool",
				0, bufferPool.getNumberOfAvailableMemorySegments());
			assertTrue(inputChannel.isWaitingForFloatingBuffers());

			// Recycle one exclusive buffer
			exclusiveBuffer.recycleBuffer();

			// The exclusive buffer is returned to the channel directly
			verify(bufferPool, times(15)).requestBuffer();
			verify(bufferPool, times(1)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 14 buffers available in the channel",
				14, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 18 buffers required in the channel",
				18, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffers available in local pool",
				0, bufferPool.getNumberOfAvailableMemorySegments());
			assertTrue(inputChannel.isWaitingForFloatingBuffers());

			// Recycle one floating buffer
			floatingBufferQueue.poll().recycleBuffer();

			// Assign the floating buffer to the listener and the channel is still waiting for more floating buffers
			verify(bufferPool, times(15)).requestBuffer();
			verify(bufferPool, times(1)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 15 buffers available in the channel",
				15, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 18 buffers required in the channel",
				18, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffers available in local pool",
				0, bufferPool.getNumberOfAvailableMemorySegments());
			assertTrue(inputChannel.isWaitingForFloatingBuffers());

			// Decrease the backlog
			inputChannel.onSenderBacklog(13);

			// Only the number of required buffers is changed by (backlog + numExclusiveBuffers)
			verify(bufferPool, times(15)).requestBuffer();
			verify(bufferPool, times(1)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 15 buffers available in the channel",
				15, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 15 buffers required in the channel",
				15, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffers available in local pool",
				0, bufferPool.getNumberOfAvailableMemorySegments());
			assertTrue(inputChannel.isWaitingForFloatingBuffers());

			// Recycle one more floating buffer
			floatingBufferQueue.poll().recycleBuffer();

			// Return the floating buffer to the buffer pool and the channel is not waiting for more floating buffers
			verify(bufferPool, times(15)).requestBuffer();
			verify(bufferPool, times(1)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 15 buffers available in the channel",
				15, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 15 buffers required in the channel",
				15, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 1 buffers available in local pool",
				1, bufferPool.getNumberOfAvailableMemorySegments());
			assertFalse(inputChannel.isWaitingForFloatingBuffers());

			// Increase the backlog again
			inputChannel.onSenderBacklog(15);

			// The floating buffer is requested from the buffer pool and the channel is registered as listener again.
			verify(bufferPool, times(17)).requestBuffer();
			verify(bufferPool, times(2)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 16 buffers available in the channel",
				16, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 17 buffers required in the channel",
				17, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffers available in local pool",
				0, bufferPool.getNumberOfAvailableMemorySegments());
			assertTrue(inputChannel.isWaitingForFloatingBuffers());
		} catch (Throwable t) {
			thrown = t;
		} finally {
			cleanup(networkBufferPool, null, null, thrown, inputChannel);
		}
	}

	/**
	 * Tests to verify the behaviours of recycling floating and exclusive buffers if the number of available
	 * buffers equals to required buffers.
	 */
	@Test
	public void testAvailableBuffersEqualToRequiredBuffers() throws Exception {
		// Setup
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(16, 32);
		final int numFloatingBuffers = 14;

		final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		inputGate.setInputChannels(inputChannel);
		Throwable thrown = null;
		try {
			final BufferPool bufferPool = spy(networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers));
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments();
			inputChannel.requestSubpartition(0);

			// Prepare the exclusive and floating buffers to verify recycle logic later
			final Buffer exclusiveBuffer = inputChannel.requestBuffer();
			assertNotNull(exclusiveBuffer);
			final Buffer floatingBuffer = bufferPool.requestBuffer();
			assertNotNull(floatingBuffer);
			verify(bufferPool, times(1)).requestBuffer();

			// Receive the producer's backlog
			inputChannel.onSenderBacklog(12);

			// The channel requests (backlog + numExclusiveBuffers) floating buffers from local pool
			// and gets enough floating buffers
			verify(bufferPool, times(14)).requestBuffer();
			verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 14 buffers available in the channel",
				14, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 14 buffers required in the channel",
				14, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffers available in local pool",
				0, bufferPool.getNumberOfAvailableMemorySegments());

			// Recycle one floating buffer
			floatingBuffer.recycleBuffer();

			// The floating buffer is returned to local buffer directly because the channel is not waiting
			// for floating buffers
			verify(bufferPool, times(14)).requestBuffer();
			verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 14 buffers available in the channel",
				14, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 14 buffers required in the channel",
				14, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 1 buffer available in local pool",
				1, bufferPool.getNumberOfAvailableMemorySegments());

			// Recycle one exclusive buffer
			exclusiveBuffer.recycleBuffer();

			// Return one extra floating buffer to the local pool because the number of available buffers
			// already equals to required buffers
			verify(bufferPool, times(14)).requestBuffer();
			verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 14 buffers available in the channel",
				14, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 14 buffers required in the channel",
				14, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 2 buffers available in local pool",
				2, bufferPool.getNumberOfAvailableMemorySegments());
		} catch (Throwable t) {
			thrown = t;
		} finally {
			cleanup(networkBufferPool, null, null, thrown, inputChannel);
		}
	}

	/**
	 * Tests to verify the behaviours of recycling floating and exclusive buffers if the number of available
	 * buffers is more than required buffers by decreasing the sender's backlog.
	 */
	@Test
	public void testAvailableBuffersMoreThanRequiredBuffers() throws Exception {
		// Setup
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(16, 32);
		final int numFloatingBuffers = 14;

		final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		inputGate.setInputChannels(inputChannel);
		Throwable thrown = null;
		try {
			final BufferPool bufferPool = spy(networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers));
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments();
			inputChannel.requestSubpartition(0);

			// Prepare the exclusive and floating buffers to verify recycle logic later
			final Buffer exclusiveBuffer = inputChannel.requestBuffer();
			assertNotNull(exclusiveBuffer);

			final Buffer floatingBuffer = bufferPool.requestBuffer();
			assertNotNull(floatingBuffer);

			verify(bufferPool, times(1)).requestBuffer();

			// Receive the producer's backlog
			inputChannel.onSenderBacklog(12);

			// The channel gets enough floating buffers from local pool
			verify(bufferPool, times(14)).requestBuffer();
			verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 14 buffers available in the channel",
				14, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 14 buffers required in the channel",
				14, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffers available in local pool",
				0, bufferPool.getNumberOfAvailableMemorySegments());

			// Decrease the backlog to make the number of available buffers more than required buffers
			inputChannel.onSenderBacklog(10);

			// Only the number of required buffers is changed by (backlog + numExclusiveBuffers)
			verify(bufferPool, times(14)).requestBuffer();
			verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 14 buffers available in the channel",
				14, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 12 buffers required in the channel",
				12, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 0 buffers available in local pool",
				0, bufferPool.getNumberOfAvailableMemorySegments());

			// Recycle one exclusive buffer
			exclusiveBuffer.recycleBuffer();

			// Return one extra floating buffer to the local pool because the number of available buffers
			// is more than required buffers
			verify(bufferPool, times(14)).requestBuffer();
			verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 14 buffers available in the channel",
				14, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 12 buffers required in the channel",
				12, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 1 buffer available in local pool",
				1, bufferPool.getNumberOfAvailableMemorySegments());

			// Recycle one floating buffer
			floatingBuffer.recycleBuffer();

			// The floating buffer is returned to local pool directly because the channel is not waiting for
			// floating buffers
			verify(bufferPool, times(14)).requestBuffer();
			verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
			assertEquals("There should be 14 buffers available in the channel",
				14, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 12 buffers required in the channel",
				12, inputChannel.getNumberOfRequiredBuffers());
			assertEquals("There should be 2 buffers available in local pool",
				2, bufferPool.getNumberOfAvailableMemorySegments());
		} catch (Throwable t) {
			thrown = t;
		} finally {
			cleanup(networkBufferPool, null, null, thrown, inputChannel);
		}
	}

	/**
	 * Tests to verify that the buffer pool will distribute available floating buffers among
	 * all the channel listeners in a fair way.
	 */
	@Test
	public void testFairDistributionFloatingBuffers() throws Exception {
		// Setup
		final int numExclusiveBuffers = 2;
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(12, 32);
		final int numFloatingBuffers = 3;

		final SingleInputGate inputGate = createSingleInputGate(3, networkBufferPool);
		final RemoteInputChannel[] inputChannels = new RemoteInputChannel[3];
		inputChannels[0] = createRemoteInputChannel(inputGate);
		inputChannels[1] = createRemoteInputChannel(inputGate);
		inputChannels[2] = createRemoteInputChannel(inputGate);
		inputGate.setInputChannels(inputChannels);
		Throwable thrown = null;
		try {
			final BufferPool bufferPool = spy(networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers));
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments();
			inputGate.requestPartitions();
			for (RemoteInputChannel inputChannel : inputChannels) {
				inputChannel.requestSubpartition(0);
			}

			// Exhaust all the floating buffers
			final List<Buffer> floatingBuffers = new ArrayList<>(numFloatingBuffers);
			for (int i = 0; i < numFloatingBuffers; i++) {
				Buffer buffer = bufferPool.requestBuffer();
				assertNotNull(buffer);
				floatingBuffers.add(buffer);
			}

			// Receive the producer's backlog to trigger request floating buffers from pool
			// and register as listeners as a result
			for (RemoteInputChannel inputChannel : inputChannels) {
				inputChannel.onSenderBacklog(8);
				verify(bufferPool, times(1)).addBufferListener(inputChannel.getBufferManager());
				assertEquals("There should be " + numExclusiveBuffers + " buffers available in the channel",
					numExclusiveBuffers, inputChannel.getNumberOfAvailableBuffers());
			}

			// Recycle three floating buffers to trigger notify buffer available
			for (Buffer buffer : floatingBuffers) {
				buffer.recycleBuffer();
			}

			for (RemoteInputChannel inputChannel : inputChannels) {
				assertEquals("There should be 3 buffers available in the channel", 3, inputChannel.getNumberOfAvailableBuffers());
				assertEquals("There should be 1 unannounced credits in the channel", 1, inputChannel.getUnannouncedCredit());
			}
		} catch (Throwable t) {
			thrown = t;
		} finally {
			cleanup(networkBufferPool, null, null, thrown, inputChannels);
		}
	}

	/**
	 * Tests that failures are propagated correctly if
	 * {@link RemoteInputChannel#notifyBufferAvailable(int)} throws an exception. Also tests that
	 * a second listener will be notified in this case.
	 */
	@Test
	public void testFailureInNotifyBufferAvailable() throws Exception {
		// Setup
		final int numExclusiveBuffers = 1;
		final int numFloatingBuffers = 1;
		final int numTotalBuffers = numExclusiveBuffers + numFloatingBuffers;
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(numTotalBuffers, 32);

		final SingleInputGate inputGate = createSingleInputGate(1);
		final RemoteInputChannel successfulRemoteIC = createRemoteInputChannel(inputGate);
		successfulRemoteIC.requestSubpartition(0);

		// late creation -> no exclusive buffers, also no requested subpartition in successfulRemoteIC
		// (to trigger a failure in RemoteInputChannel#notifyBufferAvailable())
		final RemoteInputChannel failingRemoteIC = createRemoteInputChannel(inputGate);

		Buffer buffer = null;
		Throwable thrown = null;
		try {
			final BufferPool bufferPool =
				networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
			inputGate.setBufferPool(bufferPool);

			buffer = checkNotNull(bufferPool.requestBuffer());

			// trigger subscription to buffer pool
			failingRemoteIC.onSenderBacklog(1);
			successfulRemoteIC.onSenderBacklog(numExclusiveBuffers + 1);
			// recycling will call RemoteInputChannel#notifyBufferAvailable() which will fail and
			// this exception will be swallowed and set as an error in failingRemoteIC
			buffer.recycleBuffer();
			buffer = null;
			try {
				failingRemoteIC.checkError();
				fail("The input channel should have an error based on the failure in RemoteInputChannel#notifyBufferAvailable()");
			} catch (IOException e) {
				assertThat(e, hasProperty("cause", isA(IllegalStateException.class)));
			}
			// currently, the buffer is still enqueued in the bufferQueue of failingRemoteIC
			assertEquals(0, bufferPool.getNumberOfAvailableMemorySegments());
			buffer = successfulRemoteIC.requestBuffer();
			assertNull("buffer should still remain in failingRemoteIC", buffer);

			// releasing resources in failingRemoteIC should free the buffer again and immediately
			// recycle it into successfulRemoteIC
			failingRemoteIC.releaseAllResources();
			assertEquals(0, bufferPool.getNumberOfAvailableMemorySegments());
			buffer = successfulRemoteIC.requestBuffer();
			assertNotNull("no buffer given to successfulRemoteIC", buffer);
		} catch (Throwable t) {
			thrown = t;
		} finally {
			cleanup(networkBufferPool, null, buffer, thrown, failingRemoteIC, successfulRemoteIC);
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
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(130, 32);
		final int numFloatingBuffers = 128;

		final ExecutorService executor = Executors.newFixedThreadPool(2);

		final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		Throwable thrown = null;
		try {
			final BufferPool bufferPool = networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments();
			inputChannel.requestSubpartition(0);

			final Callable<Void> requestBufferTask = new Callable<Void>() {
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

			final Callable<Void> releaseTask = new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					inputChannel.releaseAllResources();

					return null;
				}
			};

			// Submit tasks and wait to finish
			submitTasksAndWaitForResults(executor, new Callable[]{requestBufferTask, releaseTask});

			assertEquals("There should be no buffers available in the channel.",
				0, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be 130 buffers available in local pool.",
				130, bufferPool.getNumberOfAvailableMemorySegments() + networkBufferPool.getNumberOfAvailableMemorySegments());
		} catch (Throwable t) {
			thrown = t;
		} finally {
			cleanup(networkBufferPool, executor, null, thrown, inputChannel);
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
		final int numExclusiveSegments = 120;
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(248, 32);
		final int numFloatingBuffers = 128;
		final int backlog = 128;

		final ExecutorService executor = Executors.newFixedThreadPool(3);

		final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
		final RemoteInputChannel inputChannel = InputChannelTestUtils.createRemoteInputChannel(inputGate, numExclusiveSegments);
		inputGate.setInputChannels(inputChannel);
		Throwable thrown = null;
		try {
			final BufferPool bufferPool = networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments();
			inputChannel.requestSubpartition(0);

			final Callable<Void> requestBufferTask = new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					for (int j = 1; j <= backlog; j++) {
						inputChannel.onSenderBacklog(j);
					}

					return null;
				}
			};

			// Submit tasks and wait to finish
			submitTasksAndWaitForResults(executor, new Callable[]{
				recycleBufferTask(inputChannel, bufferPool, numExclusiveSegments, numFloatingBuffers),
				requestBufferTask});

			assertEquals("There should be " + inputChannel.getNumberOfRequiredBuffers() + " buffers available in channel.",
				inputChannel.getNumberOfRequiredBuffers(), inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be no buffers available in local pool.",
				0, bufferPool.getNumberOfAvailableMemorySegments());
		} catch (Throwable t) {
			thrown = t;
		} finally {
			cleanup(networkBufferPool, executor, null, thrown, inputChannel);
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
		final int numExclusiveSegments = 120;
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(248, 32);
		final int numFloatingBuffers = 128;

		final ExecutorService executor = Executors.newFixedThreadPool(3);

		final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
		final RemoteInputChannel inputChannel = InputChannelTestUtils.createRemoteInputChannel(inputGate, numExclusiveSegments);
		inputGate.setInputChannels(inputChannel);
		Throwable thrown = null;
		try {
			final BufferPool bufferPool = networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments();
			inputChannel.requestSubpartition(0);

			final Callable<Void> releaseTask = new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					inputChannel.releaseAllResources();

					return null;
				}
			};

			// Submit tasks and wait to finish
			submitTasksAndWaitForResults(executor, new Callable[]{
				recycleBufferTask(inputChannel, bufferPool, numExclusiveSegments, numFloatingBuffers),
				releaseTask});

			assertEquals("There should be no buffers available in the channel.",
				0, inputChannel.getNumberOfAvailableBuffers());
			assertEquals("There should be " + numFloatingBuffers + " buffers available in local pool.",
				numFloatingBuffers, bufferPool.getNumberOfAvailableMemorySegments());
			assertEquals("There should be " + numExclusiveSegments + " buffers available in global pool.",
				numExclusiveSegments, networkBufferPool.getNumberOfAvailableMemorySegments());
		} catch (Throwable t) {
			thrown = t;
		} finally {
			cleanup(networkBufferPool, executor, null, thrown, inputChannel);
		}
	}

	/**
	 * Tests to verify that there is no race condition with two things running in parallel:
	 * recycling exclusive buffers and recycling external buffers to the buffer pool while the
	 * recycling of the exclusive buffer triggers recycling a floating buffer (FLINK-9676).
	 */
	@Test
	public void testConcurrentRecycleAndRelease2() throws Exception {
		// Setup
		final int retries = 1_000;
		final int numExclusiveBuffers = 2;
		final int numFloatingBuffers = 2;
		final int numTotalBuffers = numExclusiveBuffers + numFloatingBuffers;
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(numTotalBuffers, 32);

		final ExecutorService executor = Executors.newFixedThreadPool(2);

		final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		inputGate.setInputChannels(inputChannel);
		Throwable thrown = null;
		try {
			final BufferPool bufferPool = networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments();
			inputChannel.requestSubpartition(0);

			final Callable<Void> bufferPoolInteractionsTask = () -> {
				for (int i = 0; i < retries; ++i) {
					Buffer buffer = buildSingleBuffer(bufferPool.requestBufferBuilderBlocking());
					buffer.recycleBuffer();
				}
				return null;
			};

			final Callable<Void> channelInteractionsTask = () -> {
				ArrayList<Buffer> exclusiveBuffers = new ArrayList<>(numExclusiveBuffers);
				ArrayList<Buffer> floatingBuffers = new ArrayList<>(numExclusiveBuffers);
				try {
					for (int i = 0; i < retries; ++i) {
						// note: we may still have a listener on the buffer pool and receive
						// floating buffers as soon as we take exclusive ones
						for (int j = 0; j < numTotalBuffers; ++j) {
							Buffer buffer = inputChannel.requestBuffer();
							if (buffer == null) {
								break;
							} else {
								//noinspection ObjectEquality
								if (buffer.getRecycler() == inputChannel.getBufferManager()) {
									exclusiveBuffers.add(buffer);
								} else {
									floatingBuffers.add(buffer);
								}
							}
						}
						// recycle excess floating buffers (will go back into the channel)
						floatingBuffers.forEach(Buffer::recycleBuffer);
						floatingBuffers.clear();

						assertEquals(numExclusiveBuffers, exclusiveBuffers.size());
						inputChannel.onSenderBacklog(0); // trigger subscription to buffer pool
						// note: if we got a floating buffer by increasing the backlog, it will be released again when recycling the exclusive buffer, if not, we should release it once we get it
						exclusiveBuffers.forEach(Buffer::recycleBuffer);
						exclusiveBuffers.clear();
					}
				} finally {
					inputChannel.releaseAllResources();
				}

				return null;
			};

			// Submit tasks and wait to finish
			submitTasksAndWaitForResults(executor,
				new Callable[] {bufferPoolInteractionsTask, channelInteractionsTask});
		} catch (Throwable t) {
			thrown = t;
		} finally {
			cleanup(networkBufferPool, executor, null, thrown, inputChannel);
		}
	}

	@Test
	public void testConcurrentGetNextBufferAndRelease() throws Exception {
		final int numTotalBuffers  = 1_000;
		final int numFloatingBuffers = 998;
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(numTotalBuffers, 32);
		final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		inputGate.setInputChannels(inputChannel);

		final ExecutorService executor = Executors.newFixedThreadPool(2);
		Throwable thrown = null;
		try {
			BufferPool bufferPool = networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments();
			inputChannel.requestSubpartition(0);

			for (int i = 0; i < numTotalBuffers; i++) {
				Buffer buffer = inputChannel.requestBuffer();
				inputChannel.onBuffer(buffer, i, 0);
			}

			final Callable<Void> getNextBufferTask = () -> {
				try {
					for (int i = 0; i < numTotalBuffers; ++i) {
						Optional<InputChannel.BufferAndAvailability> bufferAndAvailability = inputChannel.getNextBuffer();
						bufferAndAvailability.ifPresent(buffer -> buffer.buffer().recycleBuffer());
					}
				} catch (Throwable t) {
					if (!inputChannel.isReleased()) {
						throw new AssertionError("Exceptions are expected here only if the input channel was released", t);
					}
				}
				return null;
			};

			final Callable<Void> releaseTask = () -> {
				inputChannel.releaseAllResources();
				return null;
			};

			submitTasksAndWaitForResults(executor, new Callable[] {getNextBufferTask, releaseTask});
		} catch (Throwable t) {
			thrown = t;
		} finally {
			cleanup(networkBufferPool, executor, null, thrown, inputChannel);
		}
	}

	/**
	 * Tests that {@link RemoteInputChannel#retriggerSubpartitionRequest(int)} would throw
	 * the {@link PartitionNotFoundException} if backoff is 0.
	 */
	@Test
	public void testPartitionNotFoundExceptionWhileRetriggeringRequest() throws Exception {
		final RemoteInputChannel inputChannel = InputChannelTestUtils.createRemoteInputChannel(
			createSingleInputGate(1), 0, new TestingConnectionManager());

		// Request partition to initialize client to avoid illegal state after retriggering partition
		inputChannel.requestSubpartition(0);
		// The default backoff is 0 then it would set PartitionNotFoundException on this channel
		inputChannel.retriggerSubpartitionRequest(0);
		try {
			inputChannel.checkError();

			fail("Should throw a PartitionNotFoundException.");
		} catch (PartitionNotFoundException notFound) {
			assertThat(inputChannel.getPartitionId(), is(notFound.getPartitionId()));
		}
	}

	/**
	 * Tests that any exceptions thrown by {@link ConnectionManager#createPartitionRequestClient(ConnectionID)}
	 * would be wrapped into {@link PartitionConnectionException} during
	 * {@link RemoteInputChannel#requestSubpartition(int)}.
	 */
	@Test
	public void testPartitionConnectionExceptionWhileRequestingPartition() throws Exception {
		final RemoteInputChannel inputChannel = InputChannelTestUtils.createRemoteInputChannel(
			createSingleInputGate(1), 0, new TestingExceptionConnectionManager());
		try {
			inputChannel.requestSubpartition(0);
			fail("Expected PartitionConnectionException.");
		} catch (PartitionConnectionException ex) {
			assertThat(inputChannel.getPartitionId(), is(ex.getPartitionId()));
		}
	}

	@Test(expected = IllegalStateException.class)
	public void testUnblockReleasedChannel() throws Exception {
		SingleInputGate inputGate = createSingleInputGate(1);
		RemoteInputChannel remoteChannel = createRemoteInputChannel(inputGate);

		remoteChannel.releaseAllResources();
		remoteChannel.resumeConsumption();
	}

	// ---------------------------------------------------------------------------------------------

	private RemoteInputChannel createRemoteInputChannel(SingleInputGate inputGate) {
		return createRemoteInputChannel(inputGate, 0, 0);
	}

	private RemoteInputChannel createRemoteInputChannel(SingleInputGate inputGate, int initialBackoff, int maxBackoff) {
		return InputChannelBuilder.newBuilder()
			.setInitialBackoff(initialBackoff)
			.setMaxBackoff(maxBackoff)
			.buildRemoteChannel(inputGate);
	}

	private RemoteInputChannel createRemoteInputChannel(
			SingleInputGate inputGate,
			ConnectionManager connectionManager,
			ResultPartitionID partitionId,
			int initialBackoff,
			int maxBackoff) {
		return InputChannelBuilder.newBuilder()
			.setInitialBackoff(initialBackoff)
			.setMaxBackoff(maxBackoff)
			.setPartitionId(partitionId)
			.setConnectionManager(connectionManager)
			.buildRemoteChannel(inputGate);
	}

	/**
	 * Test to guard against FLINK-13249.
	 */
	@Test
	public void testOnFailedPartitionRequestDoesNotBlockNetworkThreads() throws Exception {

		final long testBlockedWaitTimeoutMillis = 30_000L;

		final PartitionProducerStateChecker partitionProducerStateChecker =
			(jobId, intermediateDataSetId, resultPartitionId) -> CompletableFuture.completedFuture(ExecutionState.RUNNING);
		final NettyShuffleEnvironment shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();
		final Task task = new TestTaskBuilder(shuffleEnvironment)
			.setPartitionProducerStateChecker(partitionProducerStateChecker)
			.build();
		final SingleInputGate inputGate = new SingleInputGateBuilder()
			.setPartitionProducerStateProvider(task)
			.build();

		TestTaskBuilder.setTaskState(task, ExecutionState.RUNNING);

		final OneShotLatch ready = new OneShotLatch();
		final OneShotLatch blocker = new OneShotLatch();
		final AtomicBoolean timedOutOrInterrupted = new AtomicBoolean(false);

		final ConnectionManager blockingConnectionManager = new TestingConnectionManager() {

			@Override
			public PartitionRequestClient createPartitionRequestClient(
				ConnectionID connectionId) {
				ready.trigger();
				try {
					// We block here, in a section that holds the SingleInputGate#requestLock
					blocker.await(testBlockedWaitTimeoutMillis, TimeUnit.MILLISECONDS);
				} catch (InterruptedException | TimeoutException e) {
					timedOutOrInterrupted.set(true);
				}

				return new TestingPartitionRequestClient();
			}
		};

		final RemoteInputChannel remoteInputChannel =
			InputChannelBuilder.newBuilder()
				.setConnectionManager(blockingConnectionManager)
				.buildRemoteChannel(inputGate);
		inputGate.setInputChannels(remoteInputChannel);

		final Thread simulatedNetworkThread = new Thread(
			() -> {
				try {
					ready.await();
					// We want to make sure that our simulated network thread does not block on
					// SingleInputGate#requestLock as well through this call.
					remoteInputChannel.onFailedPartitionRequest();

					// Will only give free the blocker if we did not block ourselves.
					blocker.trigger();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			});

		simulatedNetworkThread.start();

		// The entry point to that will lead us into blockingConnectionManager#createPartitionRequestClient(...).
		inputGate.requestPartitions();

		simulatedNetworkThread.join();

		Assert.assertFalse(
			"Test ended by timeout or interruption - this indicates that the network thread was blocked.",
			timedOutOrInterrupted.get());
	}

	@Test
	public void testNoNotifyOnSavepoint() throws IOException {
		TestBufferReceivedListener listener = new TestBufferReceivedListener();
		SingleInputGate inputGate = new SingleInputGateBuilder().build();
		inputGate.registerBufferReceivedListener(listener);
		RemoteInputChannel channel = InputChannelTestUtils.createRemoteInputChannel(inputGate, 0);
		channel.onBuffer(toBuffer(new CheckpointBarrier(123L, 123L, new CheckpointOptions(SAVEPOINT, CheckpointStorageLocationReference.getDefault()))), 0, 0);
		channel.checkError();
		assertTrue(listener.notifiedOnBarriers.isEmpty());
	}

	/**
	 * Requests the buffers from input channel and buffer pool first and then recycles them by a callable task.
	 *
	 * @param inputChannel The input channel that exclusive buffers request from.
	 * @param bufferPool The buffer pool that floating buffers request from.
	 * @param numExclusiveSegments The number of exclusive buffers to request.
	 * @param numFloatingBuffers The number of floating buffers to request.
	 * @return The callable task to recycle exclusive and floating buffers.
	 */
	private Callable<Void> recycleBufferTask(
		RemoteInputChannel inputChannel,
		BufferPool bufferPool,
		int numExclusiveSegments,
		int numFloatingBuffers) throws Exception {

		Queue<Buffer> exclusiveBuffers = new ArrayDeque<>(numExclusiveSegments);
		// Exhaust all the exclusive buffers
		for (int i = 0; i < numExclusiveSegments; i++) {
			Buffer buffer = inputChannel.requestBuffer();
			assertNotNull(buffer);
			exclusiveBuffers.add(buffer);
		}

		Queue<Buffer> floatingBuffers = new ArrayDeque<>(numFloatingBuffers);
		// Exhaust all the floating buffers
		for (int i = 0; i < numFloatingBuffers; i++) {
			Buffer buffer = bufferPool.requestBuffer();
			assertNotNull(buffer);
			floatingBuffers.add(buffer);
		}

		return new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				Random random = new Random();

				while (!exclusiveBuffers.isEmpty() && !floatingBuffers.isEmpty()) {
					if (random.nextBoolean()) {
						exclusiveBuffers.poll().recycleBuffer();
					} else {
						floatingBuffers.poll().recycleBuffer();
					}
				}

				while (!exclusiveBuffers.isEmpty()) {
					exclusiveBuffers.poll().recycleBuffer();
				}

				while (!floatingBuffers.isEmpty()) {
					floatingBuffers.poll().recycleBuffer();
				}

				return null;
			}
		};
	}

	/**
	 * Submits all the callable tasks to the executor and waits for the results.
	 *
	 * @param executor The executor service for running tasks.
	 * @param tasks The callable tasks to be submitted and executed.
	 */
	static void submitTasksAndWaitForResults(ExecutorService executor, Callable[] tasks) throws Exception {
		final List<Future> results = Lists.newArrayListWithCapacity(tasks.length);

		for (Callable task : tasks) {
			//noinspection unchecked
			results.add(executor.submit(task));
		}

		for (Future result : results) {
			result.get();
		}
	}

	/**
	 * Helper code to ease cleanup handling with suppressed exceptions.
	 */
	public static void cleanup(
			NetworkBufferPool networkBufferPool,
			@Nullable ExecutorService executor,
			@Nullable Buffer buffer,
			@Nullable Throwable throwable,
			InputChannel... inputChannels) throws Exception {
		for (InputChannel inputChannel : inputChannels) {
			try {
				inputChannel.releaseAllResources();
			} catch (Throwable tInner) {
				throwable = ExceptionUtils.firstOrSuppressed(tInner, throwable);
			}
		}

		if (buffer != null && !buffer.isRecycled()) {
			buffer.recycleBuffer();
		}

		try {
			networkBufferPool.destroyAllBufferPools();
		} catch (Throwable tInner) {
			throwable = ExceptionUtils.firstOrSuppressed(tInner, throwable);
		}

		try {
			networkBufferPool.destroy();
		} catch (Throwable tInner) {
			throwable = ExceptionUtils.firstOrSuppressed(tInner, throwable);
		}

		if (executor != null) {
			executor.shutdown();
		}
		if (throwable != null) {
			ExceptionUtils.rethrowException(throwable);
		}
	}

	private static final class TestingExceptionConnectionManager extends TestingConnectionManager {
		@Override
		public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) throws IOException {
			throw new IOException("");
		}
	}

	private static final class TestPartitionProducerStateProvider implements PartitionProducerStateProvider {

		private boolean isInvoked;
		private final ResultPartitionID partitionId;

		TestPartitionProducerStateProvider(ResultPartitionID partitionId) {
			this.partitionId = checkNotNull(partitionId);
		}

		@Override
		public void requestPartitionProducerState(
			IntermediateDataSetID intermediateDataSetId,
			ResultPartitionID resultPartitionId,
			Consumer<? super ResponseHandle> responseConsumer) {

			assertEquals(partitionId, resultPartitionId);
			isInvoked = true;
		}

		boolean isInvoked() {
			return isInvoked;
		}
	}

	private static final class TestVerifyConnectionManager extends TestingConnectionManager {
		private final PartitionRequestClient client;

		TestVerifyConnectionManager(TestingPartitionRequestClient client) {
			this.client = checkNotNull(client);
		}

		@Override
		public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) {
			return client;
		}
	}

	private static final class TestVerifyPartitionRequestClient extends TestingPartitionRequestClient {
		private ResultPartitionID partitionId;
		private int subpartitionIndex;
		private int delayMs;

		@Override
		public void requestSubpartition(ResultPartitionID partitionId, int subpartitionIndex, RemoteInputChannel channel, int delayMs) {
			this.partitionId = partitionId;
			this.subpartitionIndex = subpartitionIndex;
			this.delayMs = delayMs;
		}

		void verifyResult(ResultPartitionID expectedId, int expectedSubpartitionIndex, int expectedDelayMs) {
			assertEquals(expectedId, partitionId);
			assertEquals(expectedSubpartitionIndex, subpartitionIndex);
			assertEquals(expectedDelayMs, delayMs);
		}
	}
}
