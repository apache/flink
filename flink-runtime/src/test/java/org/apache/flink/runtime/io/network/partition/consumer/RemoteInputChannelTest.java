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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
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
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.buffer.NoOpBufferPool;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TestTaskBuilder;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.CheckpointOptions.alignedWithTimeout;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.io.network.api.serialization.EventSerializer.toBuffer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSingleBuffer;
import static org.apache.flink.runtime.io.network.partition.AvailabilityUtil.assertAvailability;
import static org.apache.flink.runtime.io.network.partition.AvailabilityUtil.assertPriorityAvailability;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.apache.flink.runtime.io.network.util.TestBufferFactory.createBuffer;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the {@link RemoteInputChannel}. */
class RemoteInputChannelTest {
    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private static final long CHECKPOINT_ID = 1L;
    private static final CheckpointOptions UNALIGNED =
            CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, getDefault());
    private static final CheckpointOptions ALIGNED_WITH_TIMEOUT =
            alignedWithTimeout(CheckpointType.CHECKPOINT, getDefault(), 10);

    @Test
    void testGateNotifiedOnBarrierConversion() throws IOException, InterruptedException {
        final int sequenceNumber = 0;
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(1, 4096);
        try {
            SingleInputGate inputGate =
                    new SingleInputGateBuilder()
                            .setBufferPoolFactory(networkBufferPool.createBufferPool(1, 1))
                            .build();
            inputGate.setup();
            RemoteInputChannel channel =
                    InputChannelBuilder.newBuilder()
                            .setConnectionManager(
                                    new TestVerifyConnectionManager(
                                            new TestVerifyPartitionRequestClient()))
                            .buildRemoteChannel(inputGate);
            channel.requestSubpartitions();

            channel.onBuffer(
                    toBuffer(
                            new CheckpointBarrier(
                                    1L,
                                    123L,
                                    alignedWithTimeout(
                                            CheckpointType.CHECKPOINT,
                                            getDefault(),
                                            Integer.MAX_VALUE)),
                            false),
                    sequenceNumber,
                    0,
                    0);
            inputGate.pollNext(); // process announcement to allow the gate remember the SQN

            channel.convertToPriorityEvent(sequenceNumber);
            assertThat(inputGate.getPriorityEventAvailableFuture()).isDone();

        } finally {
            networkBufferPool.destroy();
        }
    }

    @Test
    void testExceptionOnReordering() throws Exception {
        // Setup
        final SingleInputGate inputGate = createSingleInputGate(1);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
        final Buffer buffer = createBuffer(TestBufferFactory.BUFFER_SIZE);

        // The test
        inputChannel.onBuffer(buffer.retainBuffer(), 0, -1, 0);

        // This does not yet throw the exception, but sets the error at the channel.
        inputChannel.onBuffer(buffer, 29, -1, 0);

        assertThatThrownBy(inputChannel::getNextBuffer)
                .withFailMessage(
                        "Did not throw expected exception after enqueuing an out-of-order buffer.");

        assertThat(buffer.isRecycled()).isFalse();
        // free remaining buffer instances
        inputChannel.releaseAllResources();
        assertThat(buffer.isRecycled()).isTrue();
    }

    @Test
    void testExceptionOnPersisting() throws Exception {
        // Setup
        final SingleInputGate inputGate = createSingleInputGate(1);
        final RemoteInputChannel inputChannel =
                InputChannelBuilder.newBuilder()
                        .setStateWriter(
                                new ChannelStateWriter.NoOpChannelStateWriter() {
                                    @Override
                                    public void addInputData(
                                            long checkpointId,
                                            InputChannelInfo info,
                                            int startSeqNum,
                                            CloseableIterator<Buffer> data) {
                                        try {
                                            data.close();
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                        throw new ExpectedTestException();
                                    }
                                })
                        .buildRemoteChannel(inputGate);

        inputChannel.checkpointStarted(
                new CheckpointBarrier(
                        42,
                        System.currentTimeMillis(),
                        CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, getDefault())));

        final Buffer buffer = createBuffer(TestBufferFactory.BUFFER_SIZE);

        assertThat(buffer.isRecycled()).isFalse();
        assertThatThrownBy(() -> inputChannel.onBuffer(buffer, 0, -1, 0))
                .isInstanceOf(ExpectedTestException.class);

        // This check is not strictly speaking necessary. Generally speaking if exception happens
        // during persisting, there are two potentially correct outcomes:
        // 1. buffer is recycled only once, in #onBuffer call when handling exception
        // 2. buffer is stored inside RemoteInputChannel and recycled on releaseAllResources.
        // What's not acceptable is that it would be released twice, in both places. Without this
        // check below, we would be just relaying on Buffer throwing IllegalReferenceCountException.
        // I've added this check just to be sure. It's freezing the current implementation that's
        // unlikely to change, on the other hand, thanks to it we don't need to relay on
        // IllegalReferenceCountException being thrown from the Buffer.
        //
        // In other words, if you end up reading this after refactoring RemoteInputChannel, it might
        // be safe to remove this assertion. Just make sure double recycling of the same buffer is
        // still throwing IllegalReferenceCountException.
        assertThat(buffer.isRecycled()).isFalse();

        inputChannel.releaseAllResources();
        assertThat(buffer.isRecycled()).isTrue();
    }

    @Test
    void testConcurrentOnBufferAndRelease() throws Exception {
        testConcurrentReleaseAndSomething(
                8192,
                (inputChannel, buffer, j) -> {
                    inputChannel.onBuffer(buffer, j, -1, 0);
                    return true;
                });
    }

    @Test
    void testConcurrentNotifyBufferAvailableAndRelease() throws Exception {
        testConcurrentReleaseAndSomething(
                1024,
                (inputChannel, buffer, j) ->
                        inputChannel.getBufferManager().notifyBufferAvailable(buffer));
    }

    private interface TriFunction<T, U, V, R> {
        R apply(T t, U u, V v) throws Exception;
    }

    /**
     * Repeatedly spawns two tasks: one to call <tt>function</tt> and the other to release the
     * channel concurrently. We do this repeatedly to provoke races.
     *
     * @param numberOfRepetitions how often to repeat the test
     * @param function function to call concurrently to {@link
     *     RemoteInputChannel#releaseAllResources()}
     */
    private void testConcurrentReleaseAndSomething(
            final int numberOfRepetitions,
            TriFunction<RemoteInputChannel, Buffer, Integer, Boolean> function)
            throws Exception {

        // Setup
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final Buffer buffer = createBuffer(TestBufferFactory.BUFFER_SIZE);

        try {
            // Test
            final SingleInputGate inputGate = createSingleInputGate(1);

            for (int i = 0; i < numberOfRepetitions; i++) {
                final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);

                final Callable<Void> enqueueTask =
                        () -> {
                            while (true) {
                                for (int j = 0; j < 128; j++) {
                                    // this is the same buffer over and over again which will be
                                    // recycled by the RemoteInputChannel
                                    if (!function.apply(inputChannel, buffer.retainBuffer(), j)) {
                                        buffer.recycleBuffer();
                                    }
                                }

                                if (inputChannel.isReleased()) {
                                    return null;
                                }
                            }
                        };

                final Callable<Void> releaseTask =
                        () -> {
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

                assertThat(inputChannel.getNumberOfQueuedBuffers())
                        .withFailMessage(
                                "Resource leak during concurrent release and notifyBufferAvailable.")
                        .isZero();
            }
        } finally {
            executor.shutdown();
            assertThat(buffer.isRecycled()).isFalse();
            buffer.recycleBuffer();
            assertThat(buffer.isRecycled()).isTrue();
        }
    }

    @Test
    void testRetriggerWithoutPartitionRequest() {
        SingleInputGate inputGate = createSingleInputGate(1);

        RemoteInputChannel ch = createRemoteInputChannel(inputGate, 0, 500, 3000);

        assertThatThrownBy(ch::retriggerSubpartitionRequest)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testPartitionRequestExponentialBackoff() throws Exception {
        // Start with initial backoff, then keep adding the partition request timeout, and cap at
        // max.
        int[] expectedDelays = {500, 1000, 1500, 2000};

        // Setup
        SingleInputGate inputGate = createSingleInputGate(1);
        ResultPartitionID partitionId = new ResultPartitionID();
        TestVerifyPartitionRequestClient client = new TestVerifyPartitionRequestClient();
        ConnectionManager connectionManager = new TestVerifyConnectionManager(client);
        RemoteInputChannel ch =
                createRemoteInputChannel(inputGate, connectionManager, partitionId, 500, 1000);

        // Initial request
        ch.requestSubpartitions();
        client.verifyResult(partitionId, 0, 0);

        // Request subpartition and verify that the actual back off.
        for (int expected : expectedDelays) {
            ch.retriggerSubpartitionRequest();
            assertThat(ch.getCurrentBackoff()).isEqualTo(expected);
        }

        // Exception after backoff is greater than the maximum backoff.
        assertThatThrownBy(
                        () -> {
                            ch.retriggerSubpartitionRequest();
                            ch.getNextBuffer();
                        })
                .isInstanceOf(IOException.class);
    }

    @Test
    void testPartitionRequestSingleBackoff() throws Exception {
        // Setup
        SingleInputGate inputGate = createSingleInputGate(1);
        ResultPartitionID partitionId = new ResultPartitionID();
        TestVerifyPartitionRequestClient client = new TestVerifyPartitionRequestClient();
        ConnectionManager connectionManager = new TestVerifyConnectionManager(client);
        RemoteInputChannel ch =
                createRemoteInputChannel(inputGate, connectionManager, partitionId, 500, 500);

        // No delay for first request
        ch.requestSubpartitions();
        client.verifyResult(partitionId, 0, 0);

        // The current backoff for second request
        ch.retriggerSubpartitionRequest();
        assertThat(ch.getCurrentBackoff()).isEqualTo(500);

        // Exception after backoff is greater than the maximum backoff.
        ch.retriggerSubpartitionRequest();
        assertThatThrownBy(ch::getNextBuffer).isInstanceOf(IOException.class);
    }

    @Test
    void testPartitionRequestNoBackoff() throws Exception {
        // Setup
        SingleInputGate inputGate = createSingleInputGate(1);
        ResultPartitionID partitionId = new ResultPartitionID();
        TestVerifyPartitionRequestClient client = new TestVerifyPartitionRequestClient();
        ConnectionManager connectionManager = new TestVerifyConnectionManager(client);
        RemoteInputChannel ch =
                createRemoteInputChannel(inputGate, connectionManager, partitionId, 0, 0);

        // No delay for first request
        ch.requestSubpartitions();
        client.verifyResult(partitionId, 0, 0);

        // Exception, because backoff is disabled.
        ch.retriggerSubpartitionRequest();
        assertThatThrownBy(ch::getNextBuffer).isInstanceOf(IOException.class);
    }

    @Test
    void testOnFailedPartitionRequest() {
        final ResultPartitionID partitionId = new ResultPartitionID();
        final TestPartitionProducerStateProvider provider =
                new TestPartitionProducerStateProvider(partitionId);
        final SingleInputGate inputGate =
                new SingleInputGateBuilder().setPartitionProducerStateProvider(provider).build();
        final RemoteInputChannel ch =
                InputChannelBuilder.newBuilder()
                        .setPartitionId(partitionId)
                        .buildRemoteChannel(inputGate);

        ch.onFailedPartitionRequest();

        assertThat(provider.isInvoked()).isTrue();
    }

    @Test
    void testProducerFailedException() throws Exception {

        ConnectionManager connManager = mock(ConnectionManager.class);
        when(connManager.createPartitionRequestClient(any(ConnectionID.class)))
                .thenReturn(mock(PartitionRequestClient.class));

        final SingleInputGate gate = createSingleInputGate(1);
        final RemoteInputChannel ch =
                InputChannelTestUtils.createRemoteInputChannel(gate, 0, connManager);

        ch.onError(new ProducerFailedException(new RuntimeException("Expected test exception.")));

        ch.requestSubpartitions();

        // Should throw an instance of CancelTaskException.
        assertThatThrownBy(ch::getNextBuffer).isInstanceOf(CancelTaskException.class);
    }

    @Test
    void testPartitionConnectionException() {
        final ConnectionManager connManager = new TestingExceptionConnectionManager();
        final SingleInputGate gate = createSingleInputGate(1);
        final RemoteInputChannel ch =
                InputChannelTestUtils.createRemoteInputChannel(gate, 0, connManager);
        gate.setInputChannels(ch);

        gate.requestPartitions();

        assertThatThrownBy(ch::getNextBuffer).isInstanceOf(PartitionConnectionException.class);
    }

    /**
     * Tests to verify the behaviours of three different processes if the number of available
     * buffers is less than required buffers.
     *
     * <ol>
     *   <li>Recycle the floating buffer
     *   <li>Recycle the exclusive buffer
     *   <li>Decrease the sender's backlog
     * </ol>
     */
    @Test
    void testAvailableBuffersLessThanRequiredBuffers() throws Exception {
        // Setup
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(16, 32);
        final int numFloatingBuffers = 14;

        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
        inputGate.setInputChannels(inputChannel);
        Throwable thrown = null;
        try {
            final BufferPool bufferPool =
                    spy(networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers));
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartitions();

            // Prepare the exclusive and floating buffers to verify recycle logic later
            final Buffer exclusiveBuffer = inputChannel.requestBuffer();
            assertThat(exclusiveBuffer).isNotNull();

            final int numRecycleFloatingBuffers = 2;
            final ArrayDeque<Buffer> floatingBufferQueue =
                    new ArrayDeque<>(numRecycleFloatingBuffers);
            for (int i = 0; i < numRecycleFloatingBuffers; i++) {
                Buffer floatingBuffer = bufferPool.requestBuffer();
                assertThat(floatingBuffer).isNotNull();
                floatingBufferQueue.add(floatingBuffer);
            }

            verify(bufferPool, times(numRecycleFloatingBuffers)).requestBuffer();

            // Receive the producer's backlog more than the number of available floating buffers
            inputChannel.onSenderBacklog(14);

            // The channel requests (backlog + numExclusiveBuffers) floating buffers from local
            // pool.
            // It does not get enough floating buffers and register as buffer listener
            verify(bufferPool, times(15)).requestBuffer();
            verify(bufferPool, times(1)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 13 buffers available in the channel")
                    .isEqualTo(13);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 16 buffers required in the channel")
                    .isEqualTo(16);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 0 buffers available in local pool")
                    .isZero();
            assertThat(inputChannel.isWaitingForFloatingBuffers()).isTrue();

            // Increase the backlog
            inputChannel.onSenderBacklog(16);

            // The channel is already in the status of waiting for buffers and will not request any
            // more
            verify(bufferPool, times(15)).requestBuffer();
            verify(bufferPool, times(1)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 13 buffers available in the channel")
                    .isEqualTo(13);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 18 buffers required in the channel")
                    .isEqualTo(18);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 0 buffers available in local pool")
                    .isZero();
            assertThat(inputChannel.isWaitingForFloatingBuffers()).isTrue();

            // Recycle one exclusive buffer
            exclusiveBuffer.recycleBuffer();

            // The exclusive buffer is returned to the channel directly
            verify(bufferPool, times(15)).requestBuffer();
            verify(bufferPool, times(1)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 14 buffers available in the channel")
                    .isEqualTo(14);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 18 buffers required in the channel")
                    .isEqualTo(18);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 0 buffers available in local pool")
                    .isZero();
            assertThat(inputChannel.isWaitingForFloatingBuffers()).isTrue();

            // Recycle one floating buffer
            floatingBufferQueue.poll().recycleBuffer();

            // Assign the floating buffer to the listener and the channel is still waiting for more
            // floating buffers
            verify(bufferPool, times(16)).requestBuffer();
            verify(bufferPool, times(2)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 15 buffers available in the channel")
                    .isEqualTo(15);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 18 buffers required in the channel")
                    .isEqualTo(18);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 0 buffers available in local pool")
                    .isZero();
            assertThat(inputChannel.isWaitingForFloatingBuffers()).isTrue();

            // Decrease the backlog
            inputChannel.onSenderBacklog(13);

            // Only the number of required buffers is changed by (backlog + numExclusiveBuffers)
            verify(bufferPool, times(16)).requestBuffer();
            verify(bufferPool, times(2)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 15 buffers available in the channel")
                    .isEqualTo(15);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 15 buffers required in the channel")
                    .isEqualTo(15);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 0 buffers available in local pool")
                    .isZero();
            assertThat(inputChannel.isWaitingForFloatingBuffers()).isTrue();

            // Recycle one more floating buffer
            floatingBufferQueue.poll().recycleBuffer();

            // Return the floating buffer to the buffer pool and the channel is not waiting for more
            // floating buffers
            verify(bufferPool, times(16)).requestBuffer();
            verify(bufferPool, times(2)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 15 buffers available in the channel")
                    .isEqualTo(15);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 15 buffers required in the channel")
                    .isEqualTo(15);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 1 buffers available in local pool")
                    .isOne();
            assertThat(inputChannel.isWaitingForFloatingBuffers()).isFalse();

            // Increase the backlog again
            inputChannel.onSenderBacklog(15);

            // The floating buffer is requested from the buffer pool and the channel is registered
            // as listener again.
            verify(bufferPool, times(18)).requestBuffer();
            verify(bufferPool, times(3)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 16 buffers available in the channel")
                    .isEqualTo(16);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 17 buffers required in the channel")
                    .isEqualTo(17);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 0 buffers available in local pool")
                    .isZero();
            assertThat(inputChannel.isWaitingForFloatingBuffers()).isTrue();
        } catch (Throwable t) {
            thrown = t;
        } finally {
            cleanup(networkBufferPool, null, null, thrown, inputChannel);
        }
    }

    /**
     * Tests to verify the behaviours of recycling floating and exclusive buffers if the number of
     * available buffers equals to required buffers.
     */
    @Test
    void testAvailableBuffersEqualToRequiredBuffers() throws Exception {
        // Setup
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(16, 32);
        final int numFloatingBuffers = 14;

        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
        inputGate.setInputChannels(inputChannel);
        Throwable thrown = null;
        try {
            final BufferPool bufferPool =
                    spy(networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers));
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartitions();

            // Prepare the exclusive and floating buffers to verify recycle logic later
            final Buffer exclusiveBuffer = inputChannel.requestBuffer();
            assertThat(exclusiveBuffer).isNotNull();
            final Buffer floatingBuffer = bufferPool.requestBuffer();
            assertThat(floatingBuffer).isNotNull();
            verify(bufferPool, times(1)).requestBuffer();

            // Receive the producer's backlog
            inputChannel.onSenderBacklog(12);

            // The channel requests (backlog + numExclusiveBuffers) floating buffers from local pool
            // and gets enough floating buffers
            verify(bufferPool, times(14)).requestBuffer();
            verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 14 buffers available in the channel")
                    .isEqualTo(14);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 14 buffers required in the channel")
                    .isEqualTo(14);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 0 buffers available in local pool")
                    .isZero();

            // Recycle one floating buffer
            floatingBuffer.recycleBuffer();

            // The floating buffer is returned to local buffer directly because the channel is not
            // waiting
            // for floating buffers
            verify(bufferPool, times(14)).requestBuffer();
            verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 14 buffers available in the channel")
                    .isEqualTo(14);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 14 buffers required in the channel")
                    .isEqualTo(14);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 1 buffers available in local pool")
                    .isOne();

            // Recycle one exclusive buffer
            exclusiveBuffer.recycleBuffer();

            // Return one extra floating buffer to the local pool because the number of available
            // buffers
            // already equals to required buffers
            verify(bufferPool, times(14)).requestBuffer();
            verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 14 buffers available in the channel")
                    .isEqualTo(14);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 14 buffers required in the channel")
                    .isEqualTo(14);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 2 buffers available in local pool")
                    .isEqualTo(2);
        } catch (Throwable t) {
            thrown = t;
        } finally {
            cleanup(networkBufferPool, null, null, thrown, inputChannel);
        }
    }

    /**
     * Tests to verify the behaviours of recycling floating and exclusive buffers if the number of
     * available buffers is more than required buffers by decreasing the sender's backlog.
     */
    @Test
    void testAvailableBuffersMoreThanRequiredBuffers() throws Exception {
        // Setup
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(16, 32);
        final int numFloatingBuffers = 14;

        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
        inputGate.setInputChannels(inputChannel);
        Throwable thrown = null;
        try {
            final BufferPool bufferPool =
                    spy(networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers));
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartitions();

            // Prepare the exclusive and floating buffers to verify recycle logic later
            final Buffer exclusiveBuffer = inputChannel.requestBuffer();
            assertThat(exclusiveBuffer).isNotNull();

            final Buffer floatingBuffer = bufferPool.requestBuffer();
            assertThat(floatingBuffer).isNotNull();

            verify(bufferPool, times(1)).requestBuffer();

            // Receive the producer's backlog
            inputChannel.onSenderBacklog(12);

            // The channel gets enough floating buffers from local pool
            verify(bufferPool, times(14)).requestBuffer();
            verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 14 buffers available in the channel")
                    .isEqualTo(14);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 14 buffers required in the channel")
                    .isEqualTo(14);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 0 buffers available in local pool")
                    .isZero();

            // Decrease the backlog to make the number of available buffers more than required
            // buffers
            inputChannel.onSenderBacklog(10);

            // Only the number of required buffers is changed by (backlog + numExclusiveBuffers)
            verify(bufferPool, times(14)).requestBuffer();
            verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 14 buffers available in the channel")
                    .isEqualTo(14);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 12 buffers required in the channel")
                    .isEqualTo(12);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 0 buffers available in local pool")
                    .isZero();

            // Recycle one exclusive buffer
            exclusiveBuffer.recycleBuffer();

            // Return one extra floating buffer to the local pool because the number of available
            // buffers
            // is more than required buffers
            verify(bufferPool, times(14)).requestBuffer();
            verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 14 buffers available in the channel")
                    .isEqualTo(14);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 12 buffers required in the channel")
                    .isEqualTo(12);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 1 buffers available in local pool")
                    .isOne();

            // Recycle one floating buffer
            floatingBuffer.recycleBuffer();

            // The floating buffer is returned to local pool directly because the channel is not
            // waiting for
            // floating buffers
            verify(bufferPool, times(14)).requestBuffer();
            verify(bufferPool, times(0)).addBufferListener(inputChannel.getBufferManager());
            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be 14 buffers available in the channel")
                    .isEqualTo(14);
            assertThat(inputChannel.getNumberOfRequiredBuffers())
                    .withFailMessage("There should be 12 buffers required in the channel")
                    .isEqualTo(12);
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 2 buffers available in local pool")
                    .isEqualTo(2);
        } catch (Throwable t) {
            thrown = t;
        } finally {
            cleanup(networkBufferPool, null, null, thrown, inputChannel);
        }
    }

    /**
     * Tests to verify that the buffer pool will distribute available floating buffers among all the
     * channel listeners in a fair way.
     */
    @Test
    void testFairDistributionFloatingBuffers() throws Exception {
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
            final BufferPool bufferPool =
                    spy(networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers));
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputGate.requestPartitions();
            for (RemoteInputChannel inputChannel : inputChannels) {
                inputChannel.requestSubpartitions();
            }

            // Exhaust all the floating buffers
            final List<Buffer> floatingBuffers = new ArrayList<>(numFloatingBuffers);
            for (int i = 0; i < numFloatingBuffers; i++) {
                Buffer buffer = bufferPool.requestBuffer();
                assertThat(buffer).isNotNull();
                floatingBuffers.add(buffer);
            }

            // Receive the producer's backlog to trigger request floating buffers from pool
            // and register as listeners as a result
            for (RemoteInputChannel inputChannel : inputChannels) {
                inputChannel.onSenderBacklog(8);
                verify(bufferPool, times(1)).addBufferListener(inputChannel.getBufferManager());
                assertThat(inputChannel.getNumberOfAvailableBuffers())
                        .withFailMessage(
                                "There should be %d buffers available in the channel",
                                numExclusiveBuffers)
                        .isEqualTo(numExclusiveBuffers);
            }

            // Recycle three floating buffers to trigger notify buffer available
            for (Buffer buffer : floatingBuffers) {
                buffer.recycleBuffer();
            }

            for (RemoteInputChannel inputChannel : inputChannels) {
                assertThat(inputChannel.getNumberOfAvailableBuffers())
                        .withFailMessage("There should be 3 buffers available in the channel")
                        .isEqualTo(3);
                assertThat(inputChannel.getUnannouncedCredit())
                        .withFailMessage("There should be 1 unannounced credits in the channel")
                        .isOne();
            }
        } catch (Throwable t) {
            thrown = t;
        } finally {
            cleanup(networkBufferPool, null, null, thrown, inputChannels);
        }
    }

    /**
     * Tests that failures are propagated correctly if {@link
     * RemoteInputChannel#notifyBufferAvailable(int)} throws an exception. Also tests that a second
     * listener will be notified in this case.
     */
    @Test
    void testFailureInNotifyBufferAvailable() throws Exception {
        // Setup
        final int numExclusiveBuffers = 1;
        final int numFloatingBuffers = 1;
        final int numTotalBuffers = numExclusiveBuffers + numFloatingBuffers;
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(numTotalBuffers, 32);

        final SingleInputGate inputGate = createSingleInputGate(1);
        final RemoteInputChannel successfulRemoteIC = createRemoteInputChannel(inputGate);
        successfulRemoteIC.requestSubpartitions();

        // late creation -> no exclusive buffers, also no requested subpartition in
        // successfulRemoteIC
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

            assertThatThrownBy(failingRemoteIC::checkError)
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(IllegalStateException.class);

            // currently, the buffer is still enqueued in the bufferQueue of failingRemoteIC
            assertThat(bufferPool.getNumberOfAvailableMemorySegments()).isZero();
            buffer = successfulRemoteIC.requestBuffer();
            assertThat(buffer)
                    .withFailMessage("buffer should still remain in failingRemoteIC")
                    .isNull();

            // releasing resources in failingRemoteIC should free the buffer again and immediately
            // recycle it into successfulRemoteIC
            failingRemoteIC.releaseAllResources();
            assertThat(bufferPool.getNumberOfAvailableMemorySegments()).isZero();
            buffer = successfulRemoteIC.requestBuffer();
            assertThat(buffer).withFailMessage("no buffer given to successfulRemoteIC").isNotNull();
        } catch (Throwable t) {
            thrown = t;
        } finally {
            cleanup(networkBufferPool, null, buffer, thrown, failingRemoteIC, successfulRemoteIC);
        }
    }

    /**
     * Tests to verify that there is no race condition with two things running in parallel:
     * requesting floating buffers on sender backlog and some other thread releasing the input
     * channel.
     */
    @Test
    void testConcurrentOnSenderBacklogAndRelease() throws Exception {
        // Setup
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(130, 32);
        final int numFloatingBuffers = 128;

        final ExecutorService executor = Executors.newFixedThreadPool(2);

        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
        Throwable thrown = null;
        try {
            final BufferPool bufferPool =
                    networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartitions();

            final Callable<Void> requestBufferTask =
                    new Callable<Void>() {
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

            final Callable<Void> releaseTask =
                    new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            inputChannel.releaseAllResources();

                            return null;
                        }
                    };

            // Submit tasks and wait to finish
            submitTasksAndWaitForResults(executor, new Callable[] {requestBufferTask, releaseTask});

            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be no buffers available in the channel.")
                    .isZero();
            assertThat(
                            bufferPool.getNumberOfAvailableMemorySegments()
                                    + networkBufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be 130 buffers available in local pool.")
                    .isEqualTo(130);
        } catch (Throwable t) {
            thrown = t;
        } finally {
            cleanup(networkBufferPool, executor, null, thrown, inputChannel);
        }
    }

    /**
     * Tests to verify that there is no race condition with two things running in parallel:
     * requesting floating buffers on sender backlog and some other thread recycling floating or
     * exclusive buffers.
     */
    @Test
    void testConcurrentOnSenderBacklogAndRecycle() throws Exception {
        // Setup
        final int numExclusiveSegments = 120;
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(248, 32);
        final int numFloatingBuffers = 128;
        final int backlog = 128;

        final ExecutorService executor = Executors.newFixedThreadPool(3);

        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel =
                InputChannelTestUtils.createRemoteInputChannel(inputGate, numExclusiveSegments);
        inputGate.setInputChannels(inputChannel);
        Throwable thrown = null;
        try {
            final BufferPool bufferPool =
                    networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartitions();

            final Callable<Void> requestBufferTask =
                    new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            for (int j = 1; j <= backlog; j++) {
                                inputChannel.onSenderBacklog(j);
                            }

                            return null;
                        }
                    };

            // Submit tasks and wait to finish
            submitTasksAndWaitForResults(
                    executor,
                    new Callable[] {
                        recycleBufferTask(
                                inputChannel, bufferPool, numExclusiveSegments, numFloatingBuffers),
                        requestBufferTask
                    });

            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage(
                            "There should be %d buffers available in the channel.",
                            numFloatingBuffers)
                    .isEqualTo(inputChannel.getNumberOfRequiredBuffers());
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage("There should be no buffers available in local pool.")
                    .isZero();
        } catch (Throwable t) {
            thrown = t;
        } finally {
            cleanup(networkBufferPool, executor, null, thrown, inputChannel);
        }
    }

    /**
     * Tests to verify that there is no race condition with two things running in parallel:
     * recycling the exclusive or floating buffers and some other thread releasing the input
     * channel.
     */
    @Test
    void testConcurrentRecycleAndRelease() throws Exception {
        // Setup
        final int numExclusiveSegments = 120;
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(248, 32);
        final int numFloatingBuffers = 128;

        final ExecutorService executor = Executors.newFixedThreadPool(3);

        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel =
                InputChannelTestUtils.createRemoteInputChannel(inputGate, numExclusiveSegments);
        inputGate.setInputChannels(inputChannel);
        Throwable thrown = null;
        try {
            final BufferPool bufferPool =
                    networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartitions();

            final Callable<Void> releaseTask =
                    new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            inputChannel.releaseAllResources();

                            return null;
                        }
                    };

            // Submit tasks and wait to finish
            submitTasksAndWaitForResults(
                    executor,
                    new Callable[] {
                        recycleBufferTask(
                                inputChannel, bufferPool, numExclusiveSegments, numFloatingBuffers),
                        releaseTask
                    });

            assertThat(inputChannel.getNumberOfAvailableBuffers())
                    .withFailMessage("There should be no buffers available in the channel.")
                    .isZero();
            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage(
                            "There should be %d buffers available in local pool.",
                            numFloatingBuffers)
                    .isEqualTo(numFloatingBuffers);
            assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                    .withFailMessage(
                            "There should be %d buffers available in global pool.",
                            numExclusiveSegments)
                    .isEqualTo(numExclusiveSegments);
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
    void testConcurrentRecycleAndRelease2() throws Exception {
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
            final BufferPool bufferPool =
                    networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartitions();

            final Callable<Void> bufferPoolInteractionsTask =
                    () -> {
                        for (int i = 0; i < retries; ++i) {
                            try (BufferBuilder bufferBuilder =
                                    bufferPool.requestBufferBuilderBlocking()) {
                                Buffer buffer = buildSingleBuffer(bufferBuilder);
                                buffer.recycleBuffer();
                            }
                        }
                        return null;
                    };

            final Callable<Void> channelInteractionsTask =
                    () -> {
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
                                        if (buffer.getRecycler()
                                                == inputChannel.getBufferManager()) {
                                            exclusiveBuffers.add(buffer);
                                        } else {
                                            floatingBuffers.add(buffer);
                                        }
                                    }
                                }
                                // recycle excess floating buffers (will go back into the channel)
                                floatingBuffers.forEach(Buffer::recycleBuffer);
                                floatingBuffers.clear();

                                assertThat(exclusiveBuffers).hasSize(numExclusiveBuffers);
                                inputChannel.onSenderBacklog(
                                        0); // trigger subscription to buffer pool
                                // note: if we got a floating buffer by increasing the backlog, it
                                // will be released again when recycling the exclusive buffer, if
                                // not, we should release it once we get it
                                exclusiveBuffers.forEach(Buffer::recycleBuffer);
                                exclusiveBuffers.clear();
                            }
                        } finally {
                            inputChannel.releaseAllResources();
                        }

                        return null;
                    };

            // Submit tasks and wait to finish
            submitTasksAndWaitForResults(
                    executor, new Callable[] {bufferPoolInteractionsTask, channelInteractionsTask});
        } catch (Throwable t) {
            thrown = t;
        } finally {
            cleanup(networkBufferPool, executor, null, thrown, inputChannel);
        }
    }

    @Test
    void testConcurrentGetNextBufferAndRelease() throws Exception {
        final int numTotalBuffers = 1_000;
        final int numFloatingBuffers = 998;
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(numTotalBuffers, 32);
        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
        inputGate.setInputChannels(inputChannel);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        Throwable thrown = null;
        try {
            BufferPool bufferPool =
                    networkBufferPool.createBufferPool(numFloatingBuffers, numFloatingBuffers);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartitions();

            for (int i = 0; i < numTotalBuffers; i++) {
                Buffer buffer = inputChannel.requestBuffer();
                inputChannel.onBuffer(buffer, i, 0, 0);
            }

            final Callable<Void> getNextBufferTask =
                    () -> {
                        try {
                            for (int i = 0; i < numTotalBuffers; ++i) {
                                Optional<InputChannel.BufferAndAvailability> bufferAndAvailability =
                                        inputChannel.getNextBuffer();
                                bufferAndAvailability.ifPresent(
                                        buffer -> buffer.buffer().recycleBuffer());
                            }
                        } catch (Throwable t) {
                            if (!inputChannel.isReleased()) {
                                throw new AssertionError(
                                        "Exceptions are expected here only if the input channel was released",
                                        t);
                            }
                        }
                        return null;
                    };

            final Callable<Void> releaseTask =
                    () -> {
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
     * Tests that {@link RemoteInputChannel#retriggerSubpartitionRequest()} would throw the {@link
     * PartitionNotFoundException} if backoff is 0.
     */
    @Test
    void testPartitionNotFoundExceptionWhileRetriggeringRequest() throws Exception {
        final RemoteInputChannel inputChannel =
                InputChannelTestUtils.createRemoteInputChannel(
                        createSingleInputGate(1), 0, new TestingConnectionManager());

        // Request partition to initialize client to avoid illegal state after retriggering
        // partition
        inputChannel.requestSubpartitions();
        // The default backoff is 0 then it would set PartitionNotFoundException on this channel
        inputChannel.retriggerSubpartitionRequest();
        assertThatThrownBy(inputChannel::checkError)
                .isInstanceOfSatisfying(
                        PartitionNotFoundException.class,
                        notFound ->
                                assertThat(inputChannel.getPartitionId())
                                        .isEqualTo(notFound.getPartitionId()));
    }

    /**
     * Tests that any exceptions thrown by {@link
     * ConnectionManager#createPartitionRequestClient(ConnectionID)} would be wrapped into {@link
     * PartitionConnectionException} during {@link RemoteInputChannel#requestSubpartitions()}.
     */
    @Test
    void testPartitionConnectionExceptionWhileRequestingPartition() throws Exception {
        final RemoteInputChannel inputChannel =
                InputChannelTestUtils.createRemoteInputChannel(
                        createSingleInputGate(1), 0, new TestingExceptionConnectionManager());
        assertThatThrownBy(inputChannel::requestSubpartitions)
                .isInstanceOfSatisfying(
                        PartitionConnectionException.class,
                        ex ->
                                assertThat(inputChannel.getPartitionId())
                                        .isEqualTo(ex.getPartitionId()));
    }

    @Test
    void testUnblockReleasedChannel() throws Exception {
        SingleInputGate inputGate = createSingleInputGate(1);
        RemoteInputChannel remoteChannel = createRemoteInputChannel(inputGate);

        remoteChannel.releaseAllResources();
        assertThatThrownBy(remoteChannel::resumeConsumption)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testReleasedChannelAnnounceBufferSize() throws Exception {
        SingleInputGate inputGate = createSingleInputGate(1);
        RemoteInputChannel remoteChannel = createRemoteInputChannel(inputGate);

        remoteChannel.releaseAllResources();
        assertThatThrownBy(() -> remoteChannel.announceBufferSize(10))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testOnUpstreamBlockedAndResumed() throws Exception {
        BufferPool bufferPool = new TestBufferPool();
        SingleInputGate inputGate = createSingleInputGate(bufferPool);

        RemoteInputChannel remoteChannel1 = createRemoteInputChannel(inputGate, 0, 2);
        RemoteInputChannel remoteChannel2 = createRemoteInputChannel(inputGate, 1, 0);
        inputGate.setup();
        remoteChannel1.requestSubpartitions();
        remoteChannel2.requestSubpartitions();

        remoteChannel1.onSenderBacklog(2);
        remoteChannel2.onSenderBacklog(2);

        assertThat(remoteChannel1.getNumberOfAvailableBuffers()).isEqualTo(4);
        assertThat(remoteChannel2.getNumberOfAvailableBuffers()).isEqualTo(2);

        Buffer barrier =
                EventSerializer.toBuffer(
                        new CheckpointBarrier(
                                1L,
                                123L,
                                alignedWithTimeout(
                                        CheckpointType.CHECKPOINT,
                                        getDefault(),
                                        Integer.MAX_VALUE)),
                        false);
        remoteChannel1.onBuffer(barrier, 0, 0, 0);
        remoteChannel2.onBuffer(barrier, 0, 0, 0);

        assertThat(remoteChannel1.getNumberOfAvailableBuffers()).isEqualTo(4);
        assertThat(remoteChannel2.getNumberOfAvailableBuffers()).isZero();

        remoteChannel1.resumeConsumption();
        remoteChannel2.resumeConsumption();

        assertThat(remoteChannel1.getUnannouncedCredit()).isEqualTo(4);
        assertThat(remoteChannel2.getUnannouncedCredit()).isZero();

        remoteChannel1.onSenderBacklog(4);
        remoteChannel2.onSenderBacklog(4);

        assertThat(remoteChannel1.getNumberOfAvailableBuffers()).isEqualTo(6);
        assertThat(remoteChannel2.getNumberOfAvailableBuffers()).isEqualTo(4);

        assertThat(remoteChannel1.getUnannouncedCredit()).isEqualTo(6);
        assertThat(remoteChannel2.getUnannouncedCredit()).isEqualTo(4);
    }

    @Test
    void testRequestBuffer() throws Exception {
        BufferPool bufferPool = new TestBufferPool();
        SingleInputGate inputGate = createSingleInputGate(bufferPool);

        RemoteInputChannel remoteChannel1 = createRemoteInputChannel(inputGate, 0, 2);
        RemoteInputChannel remoteChannel2 = createRemoteInputChannel(inputGate, 1, 0);
        inputGate.setup();
        remoteChannel1.requestSubpartitions();
        remoteChannel2.requestSubpartitions();

        remoteChannel1.onSenderBacklog(2);
        remoteChannel2.onSenderBacklog(2);

        for (int i = 4; i >= 0; --i) {
            assertThat(remoteChannel1.getNumberOfRequiredBuffers()).isEqualTo(i);
            remoteChannel1.requestBuffer();
        }

        for (int i = 2; i >= 0; --i) {
            assertThat(remoteChannel2.getNumberOfRequiredBuffers()).isEqualTo(i);
            remoteChannel2.requestBuffer();
        }
    }

    @Test
    void testPrioritySequenceNumbers() throws Exception {
        int sequenceNumber = 0;
        int bufferSize = 1;
        final RemoteInputChannel channel = buildInputGateAndGetChannel(sequenceNumber);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBarrier(channel, sequenceNumber++, UNALIGNED);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);

        assertGetNextBufferSequenceNumbers(channel, 2, 0, 1, 3, 4);
    }

    @Test
    void testGetInflightBuffers() throws Exception {
        int bufferSize = 1;
        int sequenceNumber = 0;
        final RemoteInputChannel channel = buildInputGateAndGetChannel(sequenceNumber);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBarrier(channel, sequenceNumber++, UNALIGNED);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        assertInflightBufferSizes(channel, 1, 2);
    }

    @Test
    void testGetAllInflightBuffers() throws Exception {
        int sequenceNumber = Integer.MAX_VALUE - 2;
        int bufferSize = 1;
        final RemoteInputChannel channel = buildInputGateAndGetChannel(sequenceNumber);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        assertInflightBufferSizes(channel, 1, 2, 3, 4);
    }

    @Test
    void testGetInflightBuffersOverflow() throws Exception {
        for (int startingSequence = Integer.MAX_VALUE - 10;
                startingSequence != Integer.MIN_VALUE + 2;
                startingSequence++) {
            final RemoteInputChannel channel = buildInputGateAndGetChannel(startingSequence);
            int bufferSize = 1;
            int sequenceNumber = startingSequence;
            sendBuffer(channel, sequenceNumber++, bufferSize++);
            sendBuffer(channel, sequenceNumber++, bufferSize++);
            sendBarrier(channel, sequenceNumber++, UNALIGNED);
            sendBuffer(channel, sequenceNumber++, bufferSize++);
            sendBuffer(channel, sequenceNumber++, bufferSize++);
            assertThat(toBufferSizes(channel.getInflightBuffers(CHECKPOINT_ID)))
                    .withFailMessage("For starting sequence " + startingSequence)
                    .contains(1, 2);
        }
    }

    @Test
    void testGetInflightBuffersAfterPollingBuffer() throws Exception {
        int bufferSize = 1;
        int sequenceNumber = 0;
        final RemoteInputChannel channel = buildInputGateAndGetChannel(sequenceNumber);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBarrier(channel, sequenceNumber++, UNALIGNED);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        assertGetNextBufferSequenceNumbers(channel, 2, 0);
        assertInflightBufferSizes(channel, 2);
    }

    private static List<Integer> toBufferSizes(List<Buffer> inflightBuffers) {
        return inflightBuffers.stream()
                .map(buffer -> buffer.getSize())
                .collect(Collectors.toList());
    }

    @Test
    void testRequiresAnnouncement() throws Exception {
        int sequenceNumber = 0;
        int bufferSize = 1;
        final RemoteInputChannel channel = buildInputGateAndGetChannel(sequenceNumber);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBarrier(channel, sequenceNumber++, ALIGNED_WITH_TIMEOUT);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);

        BufferAndAvailability nextBuffer = channel.getNextBuffer().get();
        assertThat(nextBuffer.getSequenceNumber()).isEqualTo(2);
        assertThat(nextBuffer.morePriorityEvents()).isFalse();
        assertThat(nextBuffer.moreAvailable()).isTrue();
        assertThat(nextBuffer.buffer().getDataType()).isEqualTo(DataType.PRIORITIZED_EVENT_BUFFER);

        assertGetNextBufferSequenceNumbers(channel, 0, 1);

        nextBuffer = channel.getNextBuffer().get();
        assertThat(nextBuffer.getSequenceNumber()).isEqualTo(2);
        assertThat(nextBuffer.buffer().getDataType())
                .isEqualTo(DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER);

        assertThat(channel.getNextBuffer().get().getSequenceNumber()).isEqualTo(3);
    }

    @Test
    void testGetInflightBuffersBeforeProcessingAnnouncement() throws Exception {
        int bufferSize = 1;
        int sequenceNumber = 0;
        final RemoteInputChannel channel = buildInputGateAndGetChannel(sequenceNumber);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBarrier(channel, sequenceNumber++, ALIGNED_WITH_TIMEOUT);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        assertInflightBufferSizes(channel, 1, 2);
    }

    @Test
    void testGetInflightBuffersAfterProcessingAnnouncement() throws Exception {
        int bufferSize = 1;
        int sequenceNumber = 0;
        final RemoteInputChannel channel = buildInputGateAndGetChannel(sequenceNumber);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBarrier(channel, sequenceNumber++, ALIGNED_WITH_TIMEOUT);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        assertGetNextBufferSequenceNumbers(channel, 2);
        assertInflightBufferSizes(channel, 1, 2);
    }

    @Test
    void testGetInflightBuffersAfterProcessingAnnouncementAndBuffer() throws Exception {
        int bufferSize = 1;
        int sequenceNumber = 0;
        final RemoteInputChannel channel = buildInputGateAndGetChannel(sequenceNumber);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBarrier(channel, sequenceNumber++, ALIGNED_WITH_TIMEOUT);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        sendBuffer(channel, sequenceNumber++, bufferSize++);
        assertGetNextBufferSequenceNumbers(channel, 2, 0);
        assertInflightBufferSizes(channel, 2);
    }

    @Test
    void testSizeOfQueuedBuffers() throws Exception {
        int sequenceNumber = 0;
        int bufferSize = 1;
        int queueSize = 0;
        final RemoteInputChannel channel = buildInputGateAndGetChannel(sequenceNumber);
        assertThat(channel.unsynchronizedGetSizeOfQueuedBuffers()).isZero();

        // Receive a couple of buffers.
        for (int i = 0; i < 2; i++) {
            queueSize += bufferSize;
            sendBuffer(channel, sequenceNumber++, bufferSize++);
            assertThat(channel.unsynchronizedGetSizeOfQueuedBuffers()).isEqualTo(queueSize);
        }

        // Receive the event.
        queueSize +=
                EventSerializer.toSerializedEvent(new CheckpointBarrier(1L, 123L, UNALIGNED))
                        .remaining();
        sendBarrier(channel, sequenceNumber++, UNALIGNED);
        assertThat(channel.unsynchronizedGetSizeOfQueuedBuffers()).isEqualTo(queueSize);

        // Poll all received buffers.
        for (int i = 0; i < 3; i++) {
            Optional<BufferAndAvailability> nextBuffer = channel.getNextBuffer();
            queueSize -= nextBuffer.get().buffer().getSize();
            assertThat(channel.unsynchronizedGetSizeOfQueuedBuffers()).isEqualTo(queueSize);
        }

        assertThat(channel.unsynchronizedGetSizeOfQueuedBuffers()).isZero();
    }

    private void sendBarrier(
            RemoteInputChannel channel, int sequenceNumber, CheckpointOptions checkpointOptions)
            throws IOException {
        send(
                channel,
                sequenceNumber,
                toBuffer(
                        new CheckpointBarrier(1L, 123L, checkpointOptions),
                        checkpointOptions.isUnalignedCheckpoint()));
    }

    private void sendBuffer(RemoteInputChannel channel, int sequenceNumber, int dataSize)
            throws IOException {
        send(channel, sequenceNumber, createBuffer(dataSize));
    }

    private void send(RemoteInputChannel channel, int sequenceNumber, Buffer buffer)
            throws IOException {
        channel.onBuffer(buffer, sequenceNumber, 0, 0);
        channel.checkError();
    }

    private void assertInflightBufferSizes(RemoteInputChannel channel, Integer... bufferSizes)
            throws CheckpointException {
        assertThat(toBufferSizes(channel.getInflightBuffers(CHECKPOINT_ID)))
                .containsExactly(bufferSizes);
    }

    private void assertGetNextBufferSequenceNumbers(
            RemoteInputChannel channel, Integer... sequenceNumbers) throws IOException {
        List<Integer> actualSequenceNumbers = new ArrayList<>();
        for (int i = 0; i < sequenceNumbers.length; i++) {
            channel.getNextBuffer()
                    .map(BufferAndAvailability::getSequenceNumber)
                    .ifPresent(actualSequenceNumbers::add);
        }
        assertThat(actualSequenceNumbers).contains(sequenceNumbers);
    }

    // ---------------------------------------------------------------------------------------------

    private RemoteInputChannel createRemoteInputChannel(SingleInputGate inputGate) {
        return createRemoteInputChannel(inputGate, 0, 0, 0);
    }

    private RemoteInputChannel createRemoteInputChannel(
            SingleInputGate inputGate, int consumedSubpartitionIndex, int initialCredits) {
        return InputChannelBuilder.newBuilder()
                .setSubpartitionIndexSet(new ResultSubpartitionIndexSet(consumedSubpartitionIndex))
                .setNetworkBuffersPerChannel(initialCredits)
                .buildRemoteChannel(inputGate);
    }

    private RemoteInputChannel createRemoteInputChannel(
            SingleInputGate inputGate,
            int consumedSubpartitionIndex,
            int partitionRequestTimeout,
            int maxBackoff) {
        return InputChannelBuilder.newBuilder()
                .setSubpartitionIndexSet(new ResultSubpartitionIndexSet(consumedSubpartitionIndex))
                .setPartitionRequestListenerTimeout(partitionRequestTimeout)
                .setMaxBackoff(maxBackoff)
                .buildRemoteChannel(inputGate);
    }

    private RemoteInputChannel createRemoteInputChannel(
            SingleInputGate inputGate,
            ConnectionManager connectionManager,
            ResultPartitionID partitionId,
            int partitionRequestTimeout,
            int maxBackoff) {
        return InputChannelBuilder.newBuilder()
                .setPartitionRequestListenerTimeout(partitionRequestTimeout)
                .setMaxBackoff(maxBackoff)
                .setPartitionId(partitionId)
                .setConnectionManager(connectionManager)
                .buildRemoteChannel(inputGate);
    }

    private RemoteInputChannel buildInputGateAndGetChannel(int expectedSequenceNumber)
            throws IOException {
        final RemoteInputChannel channel = buildInputGateAndGetChannel();
        channel.setExpectedSequenceNumber(expectedSequenceNumber);
        return channel;
    }

    private RemoteInputChannel buildInputGateAndGetChannel() throws IOException {
        return (RemoteInputChannel) buildInputGate().getChannel(0);
    }

    private SingleInputGate buildInputGate() throws IOException {
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(4, 4096);
        SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildRemoteChannel)
                        .setBufferPoolFactory(networkBufferPool.createBufferPool(1, 4))
                        .setSegmentProvider(networkBufferPool)
                        .build();
        inputGate.setup();
        inputGate.requestPartitions();
        return inputGate;
    }

    /** Test to guard against FLINK-13249. */
    @Test
    void testOnFailedPartitionRequestDoesNotBlockNetworkThreads() throws Exception {

        final long testBlockedWaitTimeoutMillis = 30_000L;

        final PartitionProducerStateChecker partitionProducerStateChecker =
                (jobId, intermediateDataSetId, resultPartitionId) ->
                        CompletableFuture.completedFuture(ExecutionState.RUNNING);
        final NettyShuffleEnvironment shuffleEnvironment =
                new NettyShuffleEnvironmentBuilder().build();
        final Task task =
                new TestTaskBuilder(shuffleEnvironment)
                        .setPartitionProducerStateChecker(partitionProducerStateChecker)
                        .build(EXECUTOR_EXTENSION.getExecutor());
        final SingleInputGate inputGate =
                new SingleInputGateBuilder().setPartitionProducerStateProvider(task).build();

        TestTaskBuilder.setTaskState(task, ExecutionState.RUNNING);

        final OneShotLatch ready = new OneShotLatch();
        final OneShotLatch blocker = new OneShotLatch();
        final AtomicBoolean timedOutOrInterrupted = new AtomicBoolean(false);

        final ConnectionManager blockingConnectionManager =
                new TestingConnectionManager() {

                    @Override
                    public PartitionRequestClient createPartitionRequestClient(
                            ConnectionID connectionId) {
                        ready.trigger();
                        try {
                            // We block here, in a section that holds the
                            // SingleInputGate#requestLock
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

        final Thread simulatedNetworkThread =
                new Thread(
                        () -> {
                            try {
                                ready.await();
                                // We want to make sure that our simulated network thread does not
                                // block on
                                // SingleInputGate#requestLock as well through this call.
                                remoteInputChannel.onFailedPartitionRequest();

                                // Will only give free the blocker if we did not block ourselves.
                                blocker.trigger();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });

        simulatedNetworkThread.start();

        // The entry point to that will lead us into
        // blockingConnectionManager#createPartitionRequestClient(...).
        inputGate.requestPartitions();

        simulatedNetworkThread.join();

        assertThat(timedOutOrInterrupted)
                .withFailMessage(
                        "Test ended by timeout or interruption - this indicates that the network thread was blocked.")
                .isFalse();
    }

    @Test
    void testNotifyOnPriority() throws IOException {
        SingleInputGate inputGate = new SingleInputGateBuilder().build();
        RemoteInputChannel channel = InputChannelTestUtils.createRemoteInputChannel(inputGate, 0);

        CheckpointOptions options = new CheckpointOptions(CHECKPOINT, getDefault());
        assertPriorityAvailability(
                inputGate,
                false,
                false,
                () ->
                        assertAvailability(
                                inputGate,
                                false,
                                true,
                                () -> {
                                    channel.onBuffer(
                                            toBuffer(
                                                    new CheckpointBarrier(1L, 123L, options),
                                                    false),
                                            0,
                                            0,
                                            0);
                                }));
        assertPriorityAvailability(
                inputGate,
                false,
                true,
                () ->
                        assertAvailability(
                                inputGate,
                                true,
                                true,
                                () -> {
                                    channel.onBuffer(
                                            toBuffer(
                                                    new CheckpointBarrier(2L, 123L, options), true),
                                            1,
                                            0,
                                            0);
                                }));
    }

    @Test
    void testBuffersInUseCount() throws Exception {
        // Setup
        RemoteInputChannel remoteInputChannel = buildInputGateAndGetChannel();

        final Buffer buffer = createBuffer(TestBufferFactory.BUFFER_SIZE);

        // Receiving the buffer with backlog.
        remoteInputChannel.onBuffer(buffer.retainBuffer(), 0, 1, 0);
        // 1 buffer + 1 backlog.
        assertThat(remoteInputChannel.getBuffersInUseCount()).isEqualTo(2);

        remoteInputChannel.onBuffer(buffer.retainBuffer(), 1, 3, 0);
        // 2 buffer + 3 backlog.
        assertThat(remoteInputChannel.getBuffersInUseCount()).isEqualTo(5);

        // 1 buffer + 3 backlog.
        remoteInputChannel.getNextBuffer();
        assertThat(remoteInputChannel.getBuffersInUseCount()).isEqualTo(4);

        // 0 buffer + 3 backlog.
        remoteInputChannel.getNextBuffer();
        assertThat(remoteInputChannel.getBuffersInUseCount()).isEqualTo(3);

        // 0 buffer + 3 backlog. Nothing changes from previous case because receivedBuffers was
        // already empty.
        remoteInputChannel.getNextBuffer();
        assertThat(remoteInputChannel.getBuffersInUseCount()).isEqualTo(3);
    }

    @Test
    void testReleasedChannelNotifyRequiredSegmentId() throws Exception {
        SingleInputGate inputGate = createSingleInputGate(1);
        RemoteInputChannel remoteChannel = createRemoteInputChannel(inputGate);

        remoteChannel.releaseAllResources();
        assertThatThrownBy(() -> remoteChannel.notifyRequiredSegmentId(0, 0))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * Requests the buffers from input channel and buffer pool first and then recycles them by a
     * callable task.
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
            int numFloatingBuffers)
            throws Exception {

        Queue<Buffer> exclusiveBuffers = new ArrayDeque<>(numExclusiveSegments);
        // Exhaust all the exclusive buffers
        for (int i = 0; i < numExclusiveSegments; i++) {
            Buffer buffer = inputChannel.requestBuffer();
            assertThat(buffer).isNotNull();
            exclusiveBuffers.add(buffer);
        }

        Queue<Buffer> floatingBuffers = new ArrayDeque<>(numFloatingBuffers);
        // Exhaust all the floating buffers
        for (int i = 0; i < numFloatingBuffers; i++) {
            Buffer buffer = bufferPool.requestBuffer();
            assertThat(buffer).isNotNull();
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
    static void submitTasksAndWaitForResults(ExecutorService executor, Callable[] tasks)
            throws Exception {
        final List<Future> results = Lists.newArrayListWithCapacity(tasks.length);

        for (Callable task : tasks) {
            //noinspection unchecked
            results.add(executor.submit(task));
        }

        for (Future result : results) {
            result.get();
        }
    }

    /** Helper code to ease cleanup handling with suppressed exceptions. */
    public static void cleanup(
            NetworkBufferPool networkBufferPool,
            @Nullable ExecutorService executor,
            @Nullable Buffer buffer,
            @Nullable Throwable throwable,
            InputChannel... inputChannels)
            throws Exception {
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
        public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
                throws IOException {
            throw new IOException("");
        }
    }

    private static final class TestPartitionProducerStateProvider
            implements PartitionProducerStateProvider {

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

            assertThat(resultPartitionId).isEqualTo(partitionId);
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

    private static final class TestVerifyPartitionRequestClient
            extends TestingPartitionRequestClient {
        private ResultPartitionID partitionId;
        private ResultSubpartitionIndexSet subpartitionIndexSet;
        private int delayMs;

        @Override
        public void requestSubpartition(
                ResultPartitionID partitionId,
                ResultSubpartitionIndexSet subpartitionIndexSet,
                RemoteInputChannel channel,
                int delayMs) {
            this.partitionId = partitionId;
            this.subpartitionIndexSet = subpartitionIndexSet;
            this.delayMs = delayMs;
        }

        void verifyResult(
                ResultPartitionID expectedId, int expectedSubpartitionIndex, int expectedDelayMs) {
            assertThat(partitionId).isEqualTo(expectedId);
            assertThat(new ResultSubpartitionIndexSet(expectedSubpartitionIndex))
                    .isEqualTo(subpartitionIndexSet);
            assertThat(delayMs).isEqualTo(expectedDelayMs);
        }
    }

    private static final class TestBufferPool extends NoOpBufferPool {

        @Override
        public Buffer requestBuffer() {
            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);
            return new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        }
    }
}
