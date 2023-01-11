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

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.disk.NoOpFileChannelManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.BufferWritingResultPartition;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.PipelinedResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.io.network.util.TestPartitionProducer;
import org.apache.flink.runtime.io.network.util.TestProducerSource;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.CheckedSupplier;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledFinishedBufferConsumer;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createLocalInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.apache.flink.runtime.io.network.partition.InputGateFairnessTest.setupInputGate;
import static org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateTest.TestingResultPartitionManager;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the {@link LocalInputChannel}. */
public class LocalInputChannelTest {

    @Test
    public void testNoDataPersistedAfterReceivingAlignedBarrier() throws Exception {
        CheckpointBarrier barrier =
                new CheckpointBarrier(
                        1L,
                        0L,
                        CheckpointOptions.alignedWithTimeout(
                                CheckpointType.CHECKPOINT, getDefault(), 123L));
        BufferConsumer barrierHolder = EventSerializer.toBufferConsumer(barrier, false);
        BufferConsumer data = createFilledFinishedBufferConsumer(1);

        RecordingChannelStateWriter stateWriter = new RecordingChannelStateWriter();
        LocalInputChannel channel =
                InputChannelBuilder.newBuilder()
                        .setPartitionManager(
                                new TestingResultPartitionManager(
                                        InputChannelTestUtils.createResultSubpartitionView(
                                                barrierHolder, data)))
                        .setStateWriter(stateWriter)
                        .buildLocalChannel(new SingleInputGateBuilder().build());
        channel.requestSubpartition();

        // pull AC barrier
        channel.getNextBuffer();
        // pretend that alignment timed out
        stateWriter.start(barrier.getId(), barrier.getCheckpointOptions());
        channel.checkpointStarted(barrier);
        // pull data
        channel.getNextBuffer();
        assertTrue(
                "no data should be persisted after receiving a barrier",
                stateWriter.getAddedInput().isEmpty());
    }

    /**
     * Tests the consumption of multiple subpartitions via local input channels.
     *
     * <p>Multiple producer tasks produce pipelined partitions, which are consumed by multiple tasks
     * via local input channels.
     */
    @Test
    public void testConcurrentConsumeMultiplePartitions() throws Exception {
        // Config
        final int parallelism = 32;
        final int producerBufferPoolSize = parallelism + 1;
        final int numberOfBuffersPerChannel = 1024;

        // Setup
        // One thread per produced partition and one per consumer
        final ExecutorService executor = Executors.newFixedThreadPool(2 * parallelism);

        final NetworkBufferPool networkBuffers =
                new NetworkBufferPool(
                        (parallelism * producerBufferPoolSize) + (parallelism * parallelism),
                        TestBufferFactory.BUFFER_SIZE);

        final ResultPartitionManager partitionManager = new ResultPartitionManager();

        final ResultPartitionID[] partitionIds = new ResultPartitionID[parallelism];
        final TestPartitionProducer[] partitionProducers = new TestPartitionProducer[parallelism];

        // Create all partitions
        for (int i = 0; i < parallelism; i++) {
            partitionIds[i] = new ResultPartitionID();

            final ResultPartition partition =
                    new ResultPartitionBuilder()
                            .setResultPartitionId(partitionIds[i])
                            .setNumberOfSubpartitions(parallelism)
                            .setNumTargetKeyGroups(parallelism)
                            .setResultPartitionManager(partitionManager)
                            .setBufferPoolFactory(
                                    () ->
                                            networkBuffers.createBufferPool(
                                                    producerBufferPoolSize,
                                                    producerBufferPoolSize,
                                                    parallelism,
                                                    Integer.MAX_VALUE,
                                                    0))
                            .build();

            // Create a buffer pool for this partition
            partition.setup();

            // Create the producer
            partitionProducers[i] =
                    new TestPartitionProducer(
                            (BufferWritingResultPartition) partition,
                            false,
                            new TestPartitionProducerBufferSource(
                                    parallelism,
                                    TestBufferFactory.BUFFER_SIZE,
                                    numberOfBuffersPerChannel));
        }

        // Test
        try {
            // Submit producer tasks
            List<CompletableFuture<?>> results = Lists.newArrayListWithCapacity(parallelism + 1);

            for (int i = 0; i < parallelism; i++) {
                results.add(
                        CompletableFuture.supplyAsync(
                                CheckedSupplier.unchecked(partitionProducers[i]::call), executor));
            }

            // Submit consumer
            for (int i = 0; i < parallelism; i++) {
                final TestLocalInputChannelConsumer consumer =
                        new TestLocalInputChannelConsumer(
                                i,
                                parallelism,
                                numberOfBuffersPerChannel,
                                networkBuffers.createBufferPool(parallelism, parallelism),
                                partitionManager,
                                new TaskEventDispatcher(),
                                partitionIds);

                results.add(
                        CompletableFuture.supplyAsync(
                                CheckedSupplier.unchecked(consumer::call), executor));
            }

            FutureUtils.waitForAll(results).get();
        } finally {
            networkBuffers.destroyAllBufferPools();
            networkBuffers.destroy();
            executor.shutdown();
        }
    }

    @Test
    public void testPartitionRequestExponentialBackoff() throws Exception {
        // Config
        int initialBackoff = 500;
        int maxBackoff = 3000;

        // Start with initial backoff, then keep doubling, and cap at max.
        int[] expectedDelays = {initialBackoff, 1000, 2000, maxBackoff};

        // Setup
        SingleInputGate inputGate = mock(SingleInputGate.class);

        BufferProvider bufferProvider = mock(BufferProvider.class);
        when(inputGate.getBufferProvider()).thenReturn(bufferProvider);

        ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);

        LocalInputChannel ch =
                createLocalInputChannel(inputGate, partitionManager, initialBackoff, maxBackoff);

        when(partitionManager.createSubpartitionView(
                        eq(ch.partitionId), eq(0), any(BufferAvailabilityListener.class)))
                .thenThrow(new PartitionNotFoundException(ch.partitionId));

        Timer timer = mock(Timer.class);
        doAnswer(
                        (Answer<Void>)
                                invocation -> {
                                    ((TimerTask) invocation.getArguments()[0]).run();
                                    return null;
                                })
                .when(timer)
                .schedule(any(TimerTask.class), anyLong());

        // Initial request
        ch.requestSubpartition();
        verify(partitionManager)
                .createSubpartitionView(
                        eq(ch.partitionId), eq(0), any(BufferAvailabilityListener.class));

        // Request subpartition and verify that the actual requests are delayed.
        for (long expected : expectedDelays) {
            ch.retriggerSubpartitionRequest(timer);

            verify(timer).schedule(any(TimerTask.class), eq(expected));
        }

        // Exception after backoff is greater than the maximum backoff.
        try {
            ch.retriggerSubpartitionRequest(timer);
            ch.getNextBuffer();
            fail("Did not throw expected exception.");
        } catch (Exception expected) {
        }
    }

    @Test(expected = CancelTaskException.class)
    public void testProducerFailedException() throws Exception {
        ResultSubpartitionView view = mock(ResultSubpartitionView.class);
        when(view.isReleased()).thenReturn(true);
        when(view.getFailureCause()).thenReturn(new Exception("Expected test exception"));

        ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
        when(partitionManager.createSubpartitionView(
                        any(ResultPartitionID.class),
                        anyInt(),
                        any(BufferAvailabilityListener.class)))
                .thenReturn(view);

        SingleInputGate inputGate = mock(SingleInputGate.class);
        BufferProvider bufferProvider = mock(BufferProvider.class);
        when(inputGate.getBufferProvider()).thenReturn(bufferProvider);

        LocalInputChannel ch = createLocalInputChannel(inputGate, partitionManager);

        ch.requestSubpartition();

        // Should throw an instance of CancelTaskException.
        ch.getNextBuffer();
    }

    /**
     * Tests that {@link LocalInputChannel#requestSubpartition()} throws {@link
     * PartitionNotFoundException} if the result partition was not registered in {@link
     * ResultPartitionManager} and no backoff.
     */
    @Test
    public void testPartitionNotFoundExceptionWhileRequestingPartition() throws Exception {
        final SingleInputGate inputGate = createSingleInputGate(1);
        final LocalInputChannel localChannel =
                createLocalInputChannel(inputGate, new ResultPartitionManager());

        try {
            localChannel.requestSubpartition();

            fail("Should throw a PartitionNotFoundException.");
        } catch (PartitionNotFoundException notFound) {
            assertThat(localChannel.getPartitionId(), Matchers.is(notFound.getPartitionId()));
        }
    }

    /**
     * Tests that {@link SingleInputGate#retriggerPartitionRequest(IntermediateResultPartitionID)}
     * is triggered after {@link LocalInputChannel#requestSubpartition()} throws {@link
     * PartitionNotFoundException} within backoff.
     */
    @Test
    public void testRetriggerPartitionRequestWhilePartitionNotFound() throws Exception {
        final SingleInputGate inputGate = createSingleInputGate(1);
        final LocalInputChannel localChannel =
                createLocalInputChannel(inputGate, new ResultPartitionManager(), 1, 1);

        inputGate.setInputChannels(localChannel);
        localChannel.requestSubpartition();

        // The timer should be initialized at the first time of retriggering partition request.
        assertNotNull(inputGate.getRetriggerLocalRequestTimer());
    }

    /**
     * Tests that {@link LocalInputChannel#retriggerSubpartitionRequest(Timer)} would throw {@link
     * PartitionNotFoundException} which is set onto the input channel then.
     */
    @Test
    public void testChannelErrorWhileRetriggeringRequest() {
        final SingleInputGate inputGate = createSingleInputGate(1);
        final LocalInputChannel localChannel =
                createLocalInputChannel(inputGate, new ResultPartitionManager());

        final Timer timer =
                new Timer(true) {
                    @Override
                    public void schedule(TimerTask task, long delay) {
                        task.run();

                        try {
                            localChannel.checkError();

                            fail("Should throw a PartitionNotFoundException.");
                        } catch (PartitionNotFoundException notFound) {
                            assertThat(
                                    localChannel.partitionId,
                                    Matchers.is(notFound.getPartitionId()));
                        } catch (IOException ex) {
                            fail("Should throw a PartitionNotFoundException.");
                        }
                    }
                };

        try {
            localChannel.retriggerSubpartitionRequest(timer);
        } finally {
            timer.cancel();
        }
    }

    /**
     * Verifies that concurrent release via the SingleInputGate and re-triggering of a partition
     * request works smoothly.
     *
     * <ul>
     *   <li>SingleInputGate acquires its request lock and tries to release all registered channels.
     *       When releasing a channel, it needs to acquire the channel's shared request-release
     *       lock.
     *   <li>If a LocalInputChannel concurrently retriggers a partition request via a Timer Thread
     *       it acquires the channel's request-release lock and calls the retrigger callback on the
     *       SingleInputGate, which again tries to acquire the gate's request lock.
     * </ul>
     *
     * <p>For certain timings this obviously leads to a deadlock. This test reliably reproduced such
     * a timing (reported in FLINK-5228). This test is pretty much testing the buggy implementation
     * and has not much more general value. If it becomes obsolete at some point (future greatness
     * ;)), feel free to remove it.
     *
     * <p>The fix in the end was to not acquire the channels lock when releasing it and/or not doing
     * any input gate callbacks while holding the channel's lock. I decided to do both.
     */
    @Test
    public void testConcurrentReleaseAndRetriggerPartitionRequest() throws Exception {
        final SingleInputGate gate = createSingleInputGate(1);

        ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
        when(partitionManager.createSubpartitionView(
                        any(ResultPartitionID.class),
                        anyInt(),
                        any(BufferAvailabilityListener.class)))
                .thenAnswer(
                        (Answer<ResultSubpartitionView>)
                                invocationOnMock -> {
                                    // Sleep here a little to give the releaser Thread
                                    // time to acquire the input gate lock. We throw
                                    // the Exception to retrigger the request.
                                    Thread.sleep(100);
                                    throw new PartitionNotFoundException(new ResultPartitionID());
                                });

        final LocalInputChannel channel = createLocalInputChannel(gate, partitionManager, 1, 1);

        Thread releaser =
                new Thread(
                        () -> {
                            try {
                                gate.close();
                            } catch (IOException ignored) {
                            }
                        });

        Thread requester =
                new Thread(
                        () -> {
                            try {
                                channel.requestSubpartition();
                            } catch (IOException ignored) {
                            }
                        });

        requester.start();
        releaser.start();

        releaser.join();
        requester.join();
    }

    /**
     * Tests that reading from a channel when after the partition has been released are handled and
     * don't lead to NPEs.
     */
    @Test
    public void testGetNextAfterPartitionReleased() throws Exception {
        ResultSubpartitionView subpartitionView =
                InputChannelTestUtils.createResultSubpartitionView(false);
        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(subpartitionView);
        LocalInputChannel channel =
                createLocalInputChannel(new SingleInputGateBuilder().build(), partitionManager);

        channel.requestSubpartition();
        assertFalse(channel.getNextBuffer().isPresent());

        // release the subpartition view
        subpartitionView.releaseAllResources();

        try {
            channel.getNextBuffer();
            fail("Did not throw expected CancelTaskException");
        } catch (CancelTaskException ignored) {
        }

        channel.releaseAllResources();
        assertFalse(channel.getNextBuffer().isPresent());
    }

    /** Verifies that buffer is not compressed when getting from a {@link LocalInputChannel}. */
    @Test
    public void testGetBufferFromLocalChannelWhenCompressionEnabled() throws Exception {
        ResultSubpartitionView subpartitionView =
                InputChannelTestUtils.createResultSubpartitionView(true);
        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(subpartitionView);
        LocalInputChannel channel =
                createLocalInputChannel(new SingleInputGateBuilder().build(), partitionManager);

        // request partition and get next buffer
        channel.requestSubpartition();
        Optional<InputChannel.BufferAndAvailability> bufferAndAvailability =
                channel.getNextBuffer();
        assertTrue(bufferAndAvailability.isPresent());
        assertFalse(bufferAndAvailability.get().buffer().isCompressed());
    }

    @Test(expected = IllegalStateException.class)
    public void testUnblockReleasedChannel() throws Exception {
        SingleInputGate inputGate = createSingleInputGate(1);
        LocalInputChannel localChannel =
                createLocalInputChannel(inputGate, new ResultPartitionManager());

        localChannel.releaseAllResources();
        localChannel.resumeConsumption();
    }

    @Test(expected = IllegalStateException.class)
    public void testAnnounceBufferSize() throws Exception {
        // given: Initialized local input channel.
        AtomicInteger lastBufferSize = new AtomicInteger(0);
        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(
                        InputChannelTestUtils.createResultSubpartitionView(true));
        SingleInputGate inputGate = createSingleInputGate(1);
        LocalInputChannel localChannel = createLocalInputChannel(inputGate, partitionManager);
        localChannel.requestSubpartition();

        localChannel.announceBufferSize(10);

        // when: Release all resources.
        localChannel.releaseAllResources();

        // then: Announcement buffer size should lead to exception.
        localChannel.announceBufferSize(12);
    }

    @Test
    public void testEnqueueAvailableChannelWhenResuming() throws IOException, InterruptedException {
        PipelinedResultPartition parent =
                (PipelinedResultPartition)
                        PartitionTestUtils.createPartition(
                                ResultPartitionType.PIPELINED, NoOpFileChannelManager.INSTANCE);
        ResultSubpartition subpartition = parent.getAllPartitions()[0];
        ResultSubpartitionView subpartitionView = subpartition.createReadView(() -> {});

        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(subpartitionView);
        LocalInputChannel channel =
                createLocalInputChannel(new SingleInputGateBuilder().build(), partitionManager);
        channel.requestSubpartition();

        // Block the subpartition
        subpartition.add(
                EventSerializer.toBufferConsumer(
                        new CheckpointBarrier(
                                1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                        false));
        assertTrue(channel.getNextBuffer().isPresent());

        // Add more data
        subpartition.add(createFilledFinishedBufferConsumer(4096));
        subpartition.flush();

        // No buffer since the subpartition is blocked.
        assertFalse(channel.inputGate.pollNext().isPresent());

        // Resumption makes the subpartition available.
        channel.resumeConsumption();
        Optional<BufferOrEvent> nextBuffer = channel.inputGate.pollNext();
        assertTrue(nextBuffer.isPresent());
        assertTrue(nextBuffer.get().isBuffer());
    }

    @Test
    public void testCheckpointingInflightData() throws Exception {
        SingleInputGate inputGate = new SingleInputGateBuilder().build();

        PipelinedResultPartition parent =
                (PipelinedResultPartition)
                        PartitionTestUtils.createPartition(
                                ResultPartitionType.PIPELINED, NoOpFileChannelManager.INSTANCE);
        ResultSubpartition subpartition = parent.getAllPartitions()[0];
        ResultSubpartitionView subpartitionView = subpartition.createReadView(() -> {});

        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(subpartitionView);
        final RecordingChannelStateWriter stateWriter = new RecordingChannelStateWriter();
        LocalInputChannel channel =
                createLocalInputChannel(
                        inputGate, partitionManager, 0, 0, b -> b.setStateWriter(stateWriter));
        inputGate.setInputChannels(channel);
        channel.requestSubpartition();

        final CheckpointStorageLocationReference location = getDefault();
        CheckpointOptions options =
                CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location);
        stateWriter.start(0, options);

        final CheckpointBarrier barrier = new CheckpointBarrier(0, 123L, options);
        channel.checkpointStarted(barrier);

        // add 1 buffer before barrier and 1 buffer afterwards. Only the first buffer should be
        // written.
        subpartition.add(createFilledFinishedBufferConsumer(1));
        assertTrue(channel.getNextBuffer().isPresent());

        subpartition.add(EventSerializer.toBufferConsumer(barrier, true));
        assertTrue(channel.getNextBuffer().isPresent());

        subpartition.add(createFilledFinishedBufferConsumer(2));
        assertTrue(channel.getNextBuffer().isPresent());

        assertArrayEquals(
                stateWriter.getAddedInput().get(channel.getChannelInfo()).stream()
                        .mapToInt(Buffer::getSize)
                        .toArray(),
                new int[] {1});
    }

    @Test
    public void testAnnounceNewBufferSize() throws IOException, InterruptedException {
        // given: Configured LocalInputChannel and pipelined subpartition.
        PipelinedResultPartition parent =
                (PipelinedResultPartition)
                        new ResultPartitionBuilder()
                                .setResultPartitionType(ResultPartitionType.PIPELINED)
                                .setFileChannelManager(NoOpFileChannelManager.INSTANCE)
                                .setNumberOfSubpartitions(2)
                                .build();
        ResultSubpartition subpartition0 = parent.getAllPartitions()[0];
        ResultSubpartition subpartition1 = parent.getAllPartitions()[1];

        LocalInputChannel channel0 =
                createLocalInputChannel(
                        new SingleInputGateBuilder().build(),
                        new TestingResultPartitionManager(subpartition0.createReadView(() -> {})));
        LocalInputChannel channel1 =
                createLocalInputChannel(
                        new SingleInputGateBuilder().build(),
                        new TestingResultPartitionManager(subpartition1.createReadView(() -> {})));

        channel0.requestSubpartition();
        channel1.requestSubpartition();

        // and: Preferable buffer size is default value.
        assertEquals(Integer.MAX_VALUE, subpartition0.add(createFilledFinishedBufferConsumer(16)));
        assertEquals(Integer.MAX_VALUE, subpartition1.add(createFilledFinishedBufferConsumer(16)));

        // when: Announce the different buffer size for different channels via LocalInputChannel.
        channel0.announceBufferSize(9);
        channel1.announceBufferSize(20);

        // then: The corresponded subpartitions have the new size.
        assertEquals(9, subpartition0.add(createFilledFinishedBufferConsumer(16)));
        assertEquals(20, subpartition1.add(createFilledFinishedBufferConsumer(16)));
    }

    @Test
    public void testReceivingBuffersInUseBeforeSubpartitionViewInitialization() throws Exception {
        // given: Local input channel without initialized subpartition view.
        ResultSubpartitionView subpartitionView =
                InputChannelTestUtils.createResultSubpartitionView(
                        createFilledFinishedBufferConsumer(4096),
                        createFilledFinishedBufferConsumer(4096),
                        createFilledFinishedBufferConsumer(4096));
        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(subpartitionView);
        final SingleInputGate inputGate = createSingleInputGate(1);
        final LocalInputChannel localChannel = createLocalInputChannel(inputGate, partitionManager);

        inputGate.setInputChannels(localChannel);

        // then: Buffers in use should be equal to 0 until subpartition view initialization.
        assertEquals(0, localChannel.getBuffersInUseCount());

        // when: The subpartition view is initialized.
        localChannel.requestSubpartition();

        // then: Buffers in use should show correct value.
        assertEquals(3, localChannel.getBuffersInUseCount());
    }

    // ---------------------------------------------------------------------------------------------

    /** Returns the configured number of buffers for each channel in a random order. */
    private static class TestPartitionProducerBufferSource implements TestProducerSource {

        private final int bufferSize;

        private final List<Byte> channelIndexes;

        public TestPartitionProducerBufferSource(
                int parallelism, int bufferSize, int numberOfBuffersToProduce) {

            this.bufferSize = bufferSize;
            this.channelIndexes =
                    Lists.newArrayListWithCapacity(parallelism * numberOfBuffersToProduce);

            // Array of channel indexes to produce buffers for
            for (byte i = 0; i < parallelism; i++) {
                for (int j = 0; j < numberOfBuffersToProduce; j++) {
                    channelIndexes.add(i);
                }
            }

            // Random buffer to channel ordering
            Collections.shuffle(channelIndexes);
        }

        @Override
        public BufferAndChannel getNextBuffer() throws Exception {
            if (channelIndexes.size() > 0) {
                final int channelIndex = channelIndexes.remove(0);
                return new BufferAndChannel(new byte[bufferSize], channelIndex);
            }

            return null;
        }
    }

    /**
     * Consumed the configured result partitions and verifies that each channel receives the
     * expected number of buffers.
     */
    private static class TestLocalInputChannelConsumer implements Callable<Void> {

        private final SingleInputGate inputGate;

        private final int numberOfInputChannels;

        private final int numberOfExpectedBuffersPerChannel;

        public TestLocalInputChannelConsumer(
                int subpartitionIndex,
                int numberOfInputChannels,
                int numberOfExpectedBuffersPerChannel,
                BufferPool bufferPool,
                ResultPartitionManager partitionManager,
                TaskEventDispatcher taskEventDispatcher,
                ResultPartitionID[] consumedPartitionIds)
                throws IOException {

            checkArgument(numberOfInputChannels >= 1);
            checkArgument(numberOfExpectedBuffersPerChannel >= 1);

            this.inputGate =
                    new SingleInputGateBuilder()
                            .setNumberOfChannels(numberOfInputChannels)
                            .setBufferPoolFactory(bufferPool)
                            .build();
            InputChannel[] inputChannels = new InputChannel[numberOfInputChannels];

            // Setup input channels
            for (int i = 0; i < numberOfInputChannels; i++) {
                inputChannels[i] =
                        InputChannelBuilder.newBuilder()
                                .setChannelIndex(i)
                                .setConsumedSubpartitionIndex(subpartitionIndex)
                                .setPartitionManager(partitionManager)
                                .setPartitionId(consumedPartitionIds[i])
                                .setTaskEventPublisher(taskEventDispatcher)
                                .buildLocalChannel(inputGate);
            }

            setupInputGate(inputGate, inputChannels);

            this.numberOfInputChannels = numberOfInputChannels;
            this.numberOfExpectedBuffersPerChannel = numberOfExpectedBuffersPerChannel;
        }

        @Override
        public Void call() throws Exception {
            // One counter per input channel. Expect the same number of buffers from each channel.
            final int[] numberOfBuffersPerChannel = new int[numberOfInputChannels];

            try {
                Optional<BufferOrEvent> boe;
                while ((boe = inputGate.getNext()).isPresent()) {
                    if (boe.get().isBuffer()) {
                        boe.get().getBuffer().recycleBuffer();

                        // Check that we don't receive too many buffers
                        if (++numberOfBuffersPerChannel[
                                        boe.get().getChannelInfo().getInputChannelIdx()]
                                > numberOfExpectedBuffersPerChannel) {

                            throw new IllegalStateException(
                                    "Received more buffers than expected "
                                            + "on channel "
                                            + boe.get().getChannelInfo()
                                            + ".");
                        }
                    }
                }

                // Verify that we received the expected number of buffers on each channel
                for (int i = 0; i < numberOfBuffersPerChannel.length; i++) {
                    final int actualNumberOfReceivedBuffers = numberOfBuffersPerChannel[i];

                    if (actualNumberOfReceivedBuffers != numberOfExpectedBuffersPerChannel) {
                        throw new IllegalStateException(
                                "Received unexpected number of buffers "
                                        + "on channel "
                                        + i
                                        + " ("
                                        + actualNumberOfReceivedBuffers
                                        + " instead "
                                        + "of "
                                        + numberOfExpectedBuffersPerChannel
                                        + ").");
                    }
                }
            } finally {
                inputGate.close();
            }

            return null;
        }
    }
}
