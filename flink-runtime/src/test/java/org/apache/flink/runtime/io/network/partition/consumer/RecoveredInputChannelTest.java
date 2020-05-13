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

import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderAndConsumerTest;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionTest;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionTest.ChannelStateReaderWithException;
import static org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannelTest.cleanup;
import static org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannelTest.submitTasksAndWaitForResults;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link RecoveredInputChannel}.
 */
@RunWith(Parameterized.class)
public class RecoveredInputChannelTest {

	private final boolean isRemote;

	@Parameterized.Parameters(name = "isRemote = {0}")
	public static Collection<Object[]> parameters() {
		return Arrays.asList(new Object[][] {
			{true},
			{false},
		});
	}

	public RecoveredInputChannelTest(boolean isRemote) {
		this.isRemote = isRemote;
	}

	@Test
	public void testConcurrentReadStateAndProcess() throws Exception {
		testConcurrentReadStateAndProcess(isRemote);
	}

	@Test
	public void testConcurrentReadStateAndRelease() throws Exception {
		testConcurrentReadStateAndRelease(isRemote);
	}

	/**
	 * Tests that there are no potential deadlock and buffer leak issues while the following actions happen concurrently:
	 * 1. Task thread processes the recovered state buffer from RecoveredInputChannel.
	 * 2. Unspilling IO thread reads the recovered state and queues the buffer into RecoveredInputChannel.
	 * 3. Canceler thread closes the input gate and releases the RecoveredInputChannel.
	 */
	@Test
	public void testConcurrentReadStateAndProcessAndRelease() throws Exception {
		testConcurrentReadStateAndProcessAndRelease(isRemote);
	}

	/**
	 * Tests that there are no buffers leak while recovering the empty input channel state.
	 */
	@Test
	public void testReadEmptyState() throws Exception {
		testReadEmptyStateOrThrowException(isRemote, ChannelStateReader.NO_OP);
	}

	/**
	 * Tests that there are no buffers leak while throwing exception during state recovery.
	 */
	@Test(expected = IOException.class)
	public void testReadStateWithException() throws Exception {
		testReadEmptyStateOrThrowException(isRemote, new ChannelStateReaderWithException());
	}

	private void testReadEmptyStateOrThrowException(boolean isRemote, ChannelStateReader reader) throws Exception {
		// setup
		final int totalBuffers = 10;
		final NetworkBufferPool globalPool = new NetworkBufferPool(totalBuffers, 32, 2);
		final SingleInputGate inputGate = createInputGate(globalPool);
		final RecoveredInputChannel inputChannel = createRecoveredChannel(isRemote, inputGate);

		try {
			inputGate.setInputChannels(inputChannel);
			inputGate.setup();

			// it would throw expected exception for the case of testReadStateWithException.
			inputChannel.readRecoveredState(reader);

			// the channel only has one EndOfChannelStateEvent in the queue for the case of testReadEmptyState.
			assertEquals(1, inputChannel.getNumberOfQueuedBuffers());
			assertFalse(inputChannel.getNextBuffer().isPresent());
			assertTrue(inputChannel.getStateConsumedFuture().isDone());
		} finally {
			// cleanup and verify no buffer leak
			inputGate.close();
			globalPool.destroyAllBufferPools();
			assertEquals(totalBuffers, globalPool.getNumberOfAvailableMemorySegments());
			globalPool.destroy();
		}
	}

	/**
	 * Tests that the process of reading recovered state executes concurrently with channel
	 * buffer processing, based on the condition of the total number of states is more that
	 * the total buffer amount, to confirm that the lifecycle(recycle) of exclusive/floating
	 * buffers works well.
	 */
	private void testConcurrentReadStateAndProcess(boolean isRemote) throws Exception {
		// setup
		final int totalBuffers = 10;
		final NetworkBufferPool globalPool = new NetworkBufferPool(totalBuffers, 32, 2);
		final SingleInputGate inputGate = createInputGate(globalPool);
		final RecoveredInputChannel inputChannel = createRecoveredChannel(isRemote, inputGate);

		// the number of states is more that the total buffer amount
		final int totalStates = 15;
		final int[] states = {1, 2, 3, 4};
		final ChannelStateReader reader = new ResultPartitionTest.FiniteChannelStateReader(totalStates, states);
		final ExecutorService executor = Executors.newFixedThreadPool(2);

		Throwable thrown = null;
		try {
			inputGate.setInputChannels(inputChannel);
			inputGate.setup();

			final Callable<Void> processTask = processRecoveredBufferTask(inputChannel, totalStates, states, false);
			final Callable<Void> readStateTask = readRecoveredStateTask(inputChannel, reader, false);

			submitTasksAndWaitForResults(executor, new Callable[] {readStateTask, processTask});
		} catch (Throwable t) {
			thrown = t;
		} finally {
			// cleanup
			cleanup(globalPool, executor, null, thrown, inputChannel);
		}
	}

	private void testConcurrentReadStateAndRelease(boolean isRemote) throws Exception {
		// setup
		final int totalBuffers = 10;
		final NetworkBufferPool globalPool = new NetworkBufferPool(totalBuffers, 32, 2);
		final SingleInputGate inputGate = createInputGate(globalPool);
		final RecoveredInputChannel inputChannel = createRecoveredChannel(isRemote, inputGate);

		// the number of states is more that the total buffer amount
		final int totalStates = 15;
		final int[] states = {1, 2, 3, 4};
		final ChannelStateReader reader = new ResultPartitionTest.FiniteChannelStateReader(totalStates, states);
		final ExecutorService executor = Executors.newFixedThreadPool(2);

		Throwable thrown = null;
		try {
			inputGate.setInputChannels(inputChannel);
			inputGate.setup();

			submitTasksAndWaitForResults(
				executor,
				new Callable[] {readRecoveredStateTask(inputChannel, reader, true), releaseChannelTask(inputChannel)});
		} catch (Throwable t) {
			thrown = t;
		} finally {
			// cleanup
			cleanup(globalPool, executor, null, thrown, inputChannel);
		}
	}

	private void testConcurrentReadStateAndProcessAndRelease(boolean isRemote) throws Exception {
		// setup
		final int totalBuffers = 10;
		final NetworkBufferPool globalPool = new NetworkBufferPool(totalBuffers, 32, 2);
		final SingleInputGate inputGate = createInputGate(globalPool);
		final RecoveredInputChannel inputChannel = createRecoveredChannel(isRemote, inputGate);

		// the number of states is more that the total buffer amount
		final int totalStates = 15;
		final int[] states = {1, 2, 3, 4};
		final ChannelStateReader reader = new ResultPartitionTest.FiniteChannelStateReader(totalStates, states);
		final ExecutorService executor = Executors.newFixedThreadPool(2);
		Throwable thrown = null;
		try {
			inputGate.setInputChannels(inputChannel);
			inputGate.setup();

			final Callable<Void> processTask = processRecoveredBufferTask(inputChannel, totalStates, states, true);
			final Callable<Void> readStateTask = readRecoveredStateTask(inputChannel, reader, true);
			final Callable<Void> releaseTask = releaseChannelTask(inputChannel);

			submitTasksAndWaitForResults(executor, new Callable[] {readStateTask, processTask, releaseTask});
		} catch (Throwable t) {
			thrown = t;
		} finally {
			// cleanup
			cleanup(globalPool, executor, null, thrown, inputChannel);
		}
	}

	private Callable<Void> readRecoveredStateTask(RecoveredInputChannel inputChannel, ChannelStateReader reader, boolean verifyRelease) {
		return () -> {
			try {
				inputChannel.readRecoveredState(reader);
			} catch (Throwable t) {
				if (!(verifyRelease && inputChannel.isReleased())) {
					throw new AssertionError("Exceptions are expected here only if the input channel was released", t);
				}
			}

			return null;
		};
	}

	private Callable<Void> processRecoveredBufferTask(RecoveredInputChannel inputChannel, int totalStates, int[] states, boolean verifyRelease) {
		return () -> {
			// process all the queued state buffers and verify the data
			int numProcessedStates = 0;
			while (numProcessedStates < totalStates) {
				if (verifyRelease && inputChannel.isReleased()) {
					break;
				}
				if (inputChannel.getNumberOfQueuedBuffers() == 0) {
					Thread.sleep(1);
					continue;
				}
				try {
					Optional<BufferAndAvailability> bufferAndAvailability = inputChannel.getNextBuffer();
					if (bufferAndAvailability.isPresent()) {
						Buffer buffer = bufferAndAvailability.get().buffer();
						BufferBuilderAndConsumerTest.assertContent(buffer, null, states);
						buffer.recycleBuffer();
						numProcessedStates++;
					}
				} catch (Throwable t) {
					if (!(verifyRelease && inputChannel.isReleased())) {
						throw new AssertionError("Exceptions are expected here only if the input channel was released", t);
					}
				}
			}

			return null;
		};
	}

	private Callable<Void> releaseChannelTask(RecoveredInputChannel inputChannel) {
		return () -> {
			inputChannel.releaseAllResources();
			return null;
		};
	}

	private RecoveredInputChannel createRecoveredChannel(boolean isRemote, SingleInputGate gate) {
		if (isRemote) {
			return new InputChannelBuilder().buildRemoteRecoveredChannel(gate);
		} else {
			return new InputChannelBuilder().buildLocalRecoveredChannel(gate);
		}
	}

	private SingleInputGate createInputGate(NetworkBufferPool globalPool) throws Exception {
		return new SingleInputGateBuilder()
			.setBufferPoolFactory(globalPool.createBufferPool(8, 8))
			.setSegmentProvider(globalPool)
			.build();
	}
}
