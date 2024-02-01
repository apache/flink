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
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.io.network.util.TestPartitionProducer;
import org.apache.flink.runtime.io.network.util.TestProducerSource;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.CheckedSupplier;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the {@link LocalInputChannel}. */
class LocalInputChannelTest {

    @Test
    void testNoDataPersistedAfterReceivingAlignedBarrier() throws Exception {
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
        channel.requestSubpartitions();

        // pull AC barrier
        channel.getNextBuffer();
        // pretend that alignment timed out
        stateWriter.start(barrier.getId(), barrier.getCheckpointOptions());
        channel.checkpointStarted(barrier);
        // pull data
        channel.getNextBuffer();
        assertThat(stateWriter.getAddedInput().isEmpty())
                .withFailMessage("no data should be persisted after receiving a barrier")
                .isTrue();
    }

    /**
     * Tests the consumption of multiple subpartitions via local input channels.
     *
     * <p>Multiple producer tasks produce pipelined partitions, which are consumed by multiple tasks
     * via local input channels.
     */
    @Test
    void testConcurrentConsumeMultiplePartitions() throws Exception {
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
    void testPartitionRequestExponentialBackoff() throws Exception {
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
                        eq(ch.partitionId),
                        any(ResultSubpartitionIndexSet.class),
                        any(BufferAvailabilityListener.class)))
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
        ch.requestSubpartitions();
        verify(partitionManager)
                .createSubpartitionView(
                        eq(ch.partitionId),
                        any(ResultSubpartitionIndexSet.class),
                        any(BufferAvailabilityListener.class));

        // Request subpartition and verify that the actual requests are delayed.
        for (long expected : expectedDelays) {
            ch.retriggerSubpartitionRequest(timer);

            verify(timer).schedule(any(TimerTask.class), eq(expected));
        }

        // Exception after backoff is greater than the maximum backoff.
        ch.retriggerSubpartitionRequest(timer);
        assertThatThrownBy(ch::getNextBuffer);
    }

    @Test
    void testProducerFailedException() throws Exception {
        ResultSubpartitionView view = mock(ResultSubpartitionView.class);
        when(view.isReleased()).thenReturn(true);
        when(view.getFailureCause()).thenReturn(new Exception("Expected test exception"));

        ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
        when(partitionManager.createSubpartitionView(
                        any(ResultPartitionID.class),
                        any(ResultSubpartitionIndexSet.class),
                        any(BufferAvailabilityListener.class)))
                .thenReturn(view);

        SingleInputGate inputGate = mock(SingleInputGate.class);
        BufferProvider bufferProvider = mock(BufferProvider.class);
        when(inputGate.getBufferProvider()).thenReturn(bufferProvider);

        LocalInputChannel ch = createLocalInputChannel(inputGate, partitionManager);

        ch.requestSubpartitions();

        // Should throw an instance of CancelTaskException.
        assertThatThrownBy(ch::getNextBuffer).isInstanceOf(CancelTaskException.class);
    }

    /**
     * Tests that {@link LocalInputChannel#requestSubpartitions()} throws {@link
     * PartitionNotFoundException} if the result partition was not registered in {@link
     * ResultPartitionManager} and no backoff.
     */
    @Test
    void testPartitionNotFoundExceptionWhileRequestingPartition() throws Exception {
        final SingleInputGate inputGate = createSingleInputGate(1);
        final LocalInputChannel localChannel =
                createLocalInputChannel(inputGate, new ResultPartitionManager());

        assertThatThrownBy(localChannel::requestSubpartitions)
                .isInstanceOfSatisfying(
                        PartitionNotFoundException.class,
                        notFound ->
                                assertThat(localChannel.getPartitionId())
                                        .isEqualTo(notFound.getPartitionId()));
    }

    /**
     * Tests that {@link SingleInputGate#retriggerPartitionRequest(IntermediateResultPartitionID)}
     * is triggered after {@link LocalInputChannel#requestSubpartitions()} throws {@link
     * PartitionNotFoundException} within backoff.
     */
    @Test
    void testRetriggerPartitionRequestWhilePartitionNotFound() throws Exception {
        final SingleInputGate inputGate = createSingleInputGate(1);
        final LocalInputChannel localChannel =
                createLocalInputChannel(inputGate, new ResultPartitionManager(), 1, 1);

        inputGate.setInputChannels(localChannel);
        localChannel.requestSubpartitions();

        // The timer should be initialized at the first time of retriggering partition request.
        assertThat(inputGate.getRetriggerLocalRequestTimer()).isNotNull();
    }

    /**
     * Tests that {@link LocalInputChannel#retriggerSubpartitionRequest(Timer)} would throw {@link
     * PartitionNotFoundException} which is set onto the input channel then.
     */
    @Test
    void testChannelErrorWhileRetriggeringRequest() {
        final SingleInputGate inputGate = createSingleInputGate(1);
        final LocalInputChannel localChannel =
                createLocalInputChannel(inputGate, new ResultPartitionManager());

        final Timer timer =
                new Timer(true) {
                    @Override
                    public void schedule(TimerTask task, long delay) {
                        task.run();
                        assertThatThrownBy(localChannel::checkError)
                                .isInstanceOfSatisfying(
                                        PartitionNotFoundException.class,
                                        notFound ->
                                                assertThat(localChannel.partitionId)
                                                        .isEqualTo(notFound.getPartitionId()));
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
    void testConcurrentReleaseAndRetriggerPartitionRequest() throws Exception {
        final SingleInputGate gate = createSingleInputGate(1);

        ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
        when(partitionManager.createSubpartitionView(
                        any(ResultPartitionID.class),
                        any(ResultSubpartitionIndexSet.class),
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
                                channel.requestSubpartitions();
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
    void testGetNextAfterPartitionReleased() throws Exception {
        ResultSubpartitionView subpartitionView =
                InputChannelTestUtils.createResultSubpartitionView(false);
        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(subpartitionView);
        LocalInputChannel channel =
                createLocalInputChannel(new SingleInputGateBuilder().build(), partitionManager);

        channel.requestSubpartitions();
        assertThat(channel.getNextBuffer()).isNotPresent();

        // release the subpartition view
        subpartitionView.releaseAllResources();

        assertThatThrownBy(channel::getNextBuffer).isInstanceOf(CancelTaskException.class);

        channel.releaseAllResources();
        assertThat(channel.getNextBuffer()).isNotPresent();
    }

    /** Verifies that buffer is not compressed when getting from a {@link LocalInputChannel}. */
    @Test
    void testGetBufferFromLocalChannelWhenCompressionEnabled() throws Exception {
        ResultSubpartitionView subpartitionView =
                InputChannelTestUtils.createResultSubpartitionView(true);
        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(subpartitionView);
        LocalInputChannel channel =
                createLocalInputChannel(new SingleInputGateBuilder().build(), partitionManager);

        // request partition and get next buffer
        channel.requestSubpartitions();
        Optional<InputChannel.BufferAndAvailability> bufferAndAvailability =
                channel.getNextBuffer();

        assertThat(bufferAndAvailability)
                .hasValueSatisfying(value -> assertThat(value.buffer().isCompressed()).isFalse());
    }

    @Test
    void testUnblockReleasedChannel() throws Exception {
        SingleInputGate inputGate = createSingleInputGate(1);
        LocalInputChannel localChannel =
                createLocalInputChannel(inputGate, new ResultPartitionManager());

        localChannel.releaseAllResources();
        assertThatThrownBy(localChannel::resumeConsumption)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testAnnounceBufferSize() throws Exception {
        // given: Initialized local input channel.
        AtomicInteger lastBufferSize = new AtomicInteger(0);
        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(
                        InputChannelTestUtils.createResultSubpartitionView(true));
        SingleInputGate inputGate = createSingleInputGate(1);
        LocalInputChannel localChannel = createLocalInputChannel(inputGate, partitionManager);
        localChannel.requestSubpartitions();

        localChannel.announceBufferSize(10);

        // when: Release all resources.
        localChannel.releaseAllResources();

        // then: Announcement buffer size should lead to exception.
        assertThatThrownBy(() -> localChannel.announceBufferSize(12))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testEnqueueAvailableChannelWhenResuming() throws IOException, InterruptedException {
        PipelinedResultPartition parent =
                (PipelinedResultPartition)
                        PartitionTestUtils.createPartition(
                                ResultPartitionType.PIPELINED, NoOpFileChannelManager.INSTANCE);
        ResultSubpartition subpartition = parent.getAllPartitions()[0];
        ResultSubpartitionView subpartitionView =
                subpartition.createReadView((ResultSubpartitionView view) -> {});

        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(subpartitionView);
        LocalInputChannel channel =
                createLocalInputChannel(new SingleInputGateBuilder().build(), partitionManager);
        channel.requestSubpartitions();

        // Block the subpartition
        subpartition.add(
                EventSerializer.toBufferConsumer(
                        new CheckpointBarrier(
                                1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                        false));
        assertThat(channel.getNextBuffer()).isPresent();

        // Add more data
        subpartition.add(createFilledFinishedBufferConsumer(4096));
        subpartition.flush();

        // No buffer since the subpartition is blocked.
        assertThat(channel.inputGate.pollNext()).isNotPresent();

        // Resumption makes the subpartition available.
        channel.resumeConsumption();
        Optional<BufferOrEvent> nextBuffer = channel.inputGate.pollNext();
        assertThat(nextBuffer).hasValueSatisfying(value -> assertThat(value.isBuffer()).isTrue());
    }

    @Test
    void testCheckpointingInflightData() throws Exception {
        SingleInputGate inputGate = new SingleInputGateBuilder().build();

        PipelinedResultPartition parent =
                (PipelinedResultPartition)
                        PartitionTestUtils.createPartition(
                                ResultPartitionType.PIPELINED, NoOpFileChannelManager.INSTANCE);
        ResultSubpartition subpartition = parent.getAllPartitions()[0];
        ResultSubpartitionView subpartitionView =
                subpartition.createReadView((ResultSubpartitionView view) -> {});

        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(subpartitionView);
        final RecordingChannelStateWriter stateWriter = new RecordingChannelStateWriter();
        LocalInputChannel channel =
                createLocalInputChannel(
                        inputGate, partitionManager, 0, 0, b -> b.setStateWriter(stateWriter));
        inputGate.setInputChannels(channel);
        channel.requestSubpartitions();

        final CheckpointStorageLocationReference location = getDefault();
        CheckpointOptions options =
                CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location);
        stateWriter.start(0, options);

        final CheckpointBarrier barrier = new CheckpointBarrier(0, 123L, options);
        channel.checkpointStarted(barrier);

        // add 1 buffer before barrier and 1 buffer afterwards. Only the first buffer should be
        // written.
        subpartition.add(createFilledFinishedBufferConsumer(1));
        assertThat(channel.getNextBuffer()).isPresent();

        subpartition.add(EventSerializer.toBufferConsumer(barrier, true));
        assertThat(channel.getNextBuffer()).isPresent();

        subpartition.add(createFilledFinishedBufferConsumer(2));
        assertThat(channel.getNextBuffer()).isPresent();

        assertThat(
                        stateWriter.getAddedInput().get(channel.getChannelInfo()).stream()
                                .mapToInt(Buffer::getSize)
                                .toArray())
                .containsExactly(1);
    }

    @Test
    void testAnnounceNewBufferSize() throws IOException, InterruptedException {
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
                        new TestingResultPartitionManager(
                                subpartition0.createReadView((ResultSubpartitionView view) -> {})));
        LocalInputChannel channel1 =
                createLocalInputChannel(
                        new SingleInputGateBuilder().build(),
                        new TestingResultPartitionManager(
                                subpartition1.createReadView((ResultSubpartitionView view) -> {})));

        channel0.requestSubpartitions();
        channel1.requestSubpartitions();

        // and: Preferable buffer size is default value.
        assertThat(subpartition0.add(createFilledFinishedBufferConsumer(16)))
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(subpartition1.add(createFilledFinishedBufferConsumer(16)))
                .isEqualTo(Integer.MAX_VALUE);

        // when: Announce the different buffer size for different channels via LocalInputChannel.
        channel0.announceBufferSize(9);
        channel1.announceBufferSize(20);

        // then: The corresponded subpartitions have the new size.
        assertThat(subpartition0.add(createFilledFinishedBufferConsumer(16))).isEqualTo(9);
        assertThat(subpartition1.add(createFilledFinishedBufferConsumer(16))).isEqualTo(20);
    }

    @Test
    void testReceivingBuffersInUseBeforeSubpartitionViewInitialization() throws Exception {
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
        assertThat(localChannel.getBuffersInUseCount()).isZero();

        // when: The subpartition view is initialized.
        localChannel.requestSubpartitions();

        // then: Buffers in use should show correct value.
        assertThat(localChannel.getBuffersInUseCount()).isEqualTo(3);
    }

    // ---------------------------------------------------------------------------------------------

    /** Returns the configured number of buffers for each channel in a random order. */
    private static class TestPartitionProducerBufferSource implements TestProducerSource {

        private final int bufferSize;

        private final List<Byte> channelIndexes;

        TestPartitionProducerBufferSource(
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

        TestLocalInputChannelConsumer(
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
                                .setSubpartitionIndexSet(
                                        new ResultSubpartitionIndexSet(subpartitionIndex))
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
