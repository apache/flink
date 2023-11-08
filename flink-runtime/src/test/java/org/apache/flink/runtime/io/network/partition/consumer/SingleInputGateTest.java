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
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.BufferWritingResultPartition;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.NoOpResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate.SubpartitionInfo;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;
import org.apache.flink.util.CompressedSerializedValue;

import org.apache.flink.shaded.guava31.com.google.common.io.Closer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.apache.flink.runtime.checkpoint.CheckpointOptions.alignedNoTimeout;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createLocalInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createResultSubpartitionView;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.newUnregisteredInputChannelMetrics;
import static org.apache.flink.runtime.io.network.partition.InputGateFairnessTest.setupInputGate;
import static org.apache.flink.runtime.io.network.util.TestBufferFactory.createBuffer;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder.createRemoteWithIdAndLocation;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SingleInputGate}. */
public class SingleInputGateTest extends InputGateTestBase {

    @Test
    void testCheckpointsDeclinedUnlessAllChannelsAreKnown() throws CheckpointException {
        SingleInputGate gate =
                createInputGate(createNettyShuffleEnvironment(), 1, ResultPartitionType.PIPELINED);
        gate.setInputChannels(
                new InputChannelBuilder().setChannelIndex(0).buildUnknownChannel(gate));
        assertThatThrownBy(
                        () ->
                                gate.checkpointStarted(
                                        new CheckpointBarrier(
                                                1L,
                                                1L,
                                                alignedNoTimeout(CHECKPOINT, getDefault()))))
                .isInstanceOf(CheckpointException.class);
    }

    @Test
    void testCheckpointsDeclinedUnlessStateConsumed() throws CheckpointException {
        SingleInputGate gate = createInputGate(createNettyShuffleEnvironment());
        checkState(!gate.getStateConsumedFuture().isDone());
        assertThatThrownBy(
                        () ->
                                gate.checkpointStarted(
                                        new CheckpointBarrier(
                                                1L,
                                                1L,
                                                alignedNoTimeout(CHECKPOINT, getDefault()))))
                .isInstanceOf(CheckpointException.class);
    }

    /**
     * Tests {@link InputGate#setup()} should create the respective {@link BufferPool} and assign
     * exclusive buffers for {@link RemoteInputChannel}s, but should not request partitions.
     */
    @Test
    void testSetupLogic() throws Exception {
        final NettyShuffleEnvironment environment = createNettyShuffleEnvironment();
        final SingleInputGate inputGate = createInputGate(environment);
        try (Closer closer = Closer.create()) {
            closer.register(environment::close);
            closer.register(inputGate::close);

            // before setup
            assertThat(inputGate.getBufferPool()).isNull();
            for (InputChannel inputChannel : inputGate.getInputChannels().values()) {
                assertThat(
                                inputChannel instanceof RecoveredInputChannel
                                        || inputChannel instanceof UnknownInputChannel)
                        .isTrue();
                if (inputChannel instanceof RecoveredInputChannel) {
                    assertThat(
                                    ((RecoveredInputChannel) inputChannel)
                                            .bufferManager.getNumberOfAvailableBuffers())
                            .isEqualTo(0);
                }
            }

            inputGate.setup();

            // after setup
            assertThat(inputGate.getBufferPool()).isNotNull();
            assertThat(inputGate.getBufferPool().getNumberOfRequiredMemorySegments()).isEqualTo(1);
            for (InputChannel inputChannel : inputGate.getInputChannels().values()) {
                if (inputChannel instanceof RemoteRecoveredInputChannel) {
                    assertThat(
                                    ((RemoteRecoveredInputChannel) inputChannel)
                                            .bufferManager.getNumberOfAvailableBuffers())
                            .isEqualTo(0);
                } else if (inputChannel instanceof LocalRecoveredInputChannel) {
                    assertThat(
                                    ((LocalRecoveredInputChannel) inputChannel)
                                            .bufferManager.getNumberOfAvailableBuffers())
                            .isEqualTo(0);
                }
            }

            inputGate.convertRecoveredInputChannels();
            assertThat(inputGate.getBufferPool()).isNotNull();
            assertThat(inputGate.getBufferPool().getNumberOfRequiredMemorySegments()).isEqualTo(1);
            for (InputChannel inputChannel : inputGate.getInputChannels().values()) {
                if (inputChannel instanceof RemoteInputChannel) {
                    assertThat(((RemoteInputChannel) inputChannel).getNumberOfAvailableBuffers())
                            .isEqualTo(2);
                }
            }
        }
    }

    @Test
    void testPartitionRequestLogic() throws Exception {
        final NettyShuffleEnvironment environment = new NettyShuffleEnvironmentBuilder().build();
        final SingleInputGate gate = createInputGate(environment);

        try (Closer closer = Closer.create()) {
            closer.register(environment::close);
            closer.register(gate::close);

            gate.finishReadRecoveredState();
            while (!gate.getStateConsumedFuture().isDone()) {
                gate.pollNext();
            }
            gate.requestPartitions();
            // check channel error during above partition request
            gate.pollNext();

            final InputChannel remoteChannel = gate.getChannel(0);
            assertThat(remoteChannel).isInstanceOf(RemoteInputChannel.class);
            assertThat(((RemoteInputChannel) remoteChannel).getPartitionRequestClient())
                    .isNotNull();
            assertThat(((RemoteInputChannel) remoteChannel).getInitialCredit()).isEqualTo(2);

            final InputChannel localChannel = gate.getChannel(1);
            assertThat(localChannel).isInstanceOf(LocalInputChannel.class);
            assertThat(((LocalInputChannel) localChannel).getSubpartitionView()).isNotNull();

            assertThat(gate.getChannel(2)).isInstanceOf(UnknownInputChannel.class);
        }
    }

    /**
     * Tests basic correctness of buffer-or-event interleaving and correct <code>null</code> return
     * value after receiving all end-of-partition events.
     */
    @Test
    void testBasicGetNextLogic() throws Exception {
        // Setup
        final SingleInputGate inputGate = createInputGate();

        final TestInputChannel[] inputChannels =
                new TestInputChannel[] {
                    new TestInputChannel(inputGate, 0), new TestInputChannel(inputGate, 1)
                };

        inputGate.setInputChannels(inputChannels);

        // Test
        inputChannels[0].readBuffer();
        inputChannels[0].readBuffer();
        inputChannels[1].readBuffer();
        inputChannels[1].readEndOfData();
        inputChannels[0].readEndOfData();
        inputChannels[1].readEndOfPartitionEvent();
        inputChannels[0].readEndOfPartitionEvent();

        inputGate.notifyChannelNonEmpty(inputChannels[0]);
        inputGate.notifyChannelNonEmpty(inputChannels[1]);

        verifyBufferOrEvent(inputGate, true, 0, true);
        verifyBufferOrEvent(inputGate, true, 1, true);
        verifyBufferOrEvent(inputGate, true, 0, true);
        verifyBufferOrEvent(inputGate, false, 1, true);
        // we have received EndOfData on a single channel only
        assertThat(inputGate.hasReceivedEndOfData())
                .isEqualTo(PullingAsyncDataInput.EndOfDataStatus.NOT_END_OF_DATA);
        verifyBufferOrEvent(inputGate, false, 0, true);
        assertThat(inputGate.isFinished()).isFalse();
        assertThat(inputGate.hasReceivedEndOfData())
                .isEqualTo(PullingAsyncDataInput.EndOfDataStatus.DRAINED);
        verifyBufferOrEvent(inputGate, false, 1, true);
        verifyBufferOrEvent(inputGate, false, 0, false);

        // Return null when the input gate has received all end-of-partition events
        assertThat(inputGate.hasReceivedEndOfData())
                .isEqualTo(PullingAsyncDataInput.EndOfDataStatus.DRAINED);
        assertThat(inputGate.isFinished()).isTrue();

        for (TestInputChannel ic : inputChannels) {
            ic.assertReturnedEventsAreRecycled();
        }
    }

    @Test
    void testDrainFlagComputation() throws Exception {
        // Setup
        final SingleInputGate inputGate1 = createInputGate();
        final SingleInputGate inputGate2 = createInputGate();

        final TestInputChannel[] inputChannels1 =
                new TestInputChannel[] {
                    new TestInputChannel(inputGate1, 0), new TestInputChannel(inputGate1, 1)
                };
        inputGate1.setInputChannels(inputChannels1);
        final TestInputChannel[] inputChannels2 =
                new TestInputChannel[] {
                    new TestInputChannel(inputGate2, 0), new TestInputChannel(inputGate2, 1)
                };
        inputGate2.setInputChannels(inputChannels2);

        // Test
        inputChannels1[1].readEndOfData(StopMode.DRAIN);
        inputChannels1[0].readEndOfData(StopMode.NO_DRAIN);

        inputChannels2[1].readEndOfData(StopMode.DRAIN);
        inputChannels2[0].readEndOfData(StopMode.DRAIN);

        inputGate1.notifyChannelNonEmpty(inputChannels1[0]);
        inputGate1.notifyChannelNonEmpty(inputChannels1[1]);
        inputGate2.notifyChannelNonEmpty(inputChannels2[0]);
        inputGate2.notifyChannelNonEmpty(inputChannels2[1]);

        verifyBufferOrEvent(inputGate1, false, 0, true);
        // we have received EndOfData on a single channel only
        assertThat(inputGate1.hasReceivedEndOfData())
                .isEqualTo(PullingAsyncDataInput.EndOfDataStatus.NOT_END_OF_DATA);
        verifyBufferOrEvent(inputGate1, false, 1, true);
        // one of the channels said we should not drain
        assertThat(inputGate1.hasReceivedEndOfData())
                .isEqualTo(PullingAsyncDataInput.EndOfDataStatus.STOPPED);

        verifyBufferOrEvent(inputGate2, false, 0, true);
        // we have received EndOfData on a single channel only
        assertThat(inputGate2.hasReceivedEndOfData())
                .isEqualTo(PullingAsyncDataInput.EndOfDataStatus.NOT_END_OF_DATA);
        verifyBufferOrEvent(inputGate2, false, 1, true);
        // both channels said we should drain
        assertThat(inputGate2.hasReceivedEndOfData())
                .isEqualTo(PullingAsyncDataInput.EndOfDataStatus.DRAINED);
    }

    /**
     * Tests that the compressed buffer will be decompressed after calling {@link
     * SingleInputGate#getNext()}.
     */
    @ParameterizedTest
    @ValueSource(strings = {"LZ4", "LZO", "ZSTD"})
    void testGetCompressedBuffer(final String compressionCodec) throws Exception {
        int bufferSize = 1024;
        BufferCompressor compressor = new BufferCompressor(bufferSize, compressionCodec);
        BufferDecompressor decompressor = new BufferDecompressor(bufferSize, compressionCodec);

        try (SingleInputGate inputGate =
                new SingleInputGateBuilder().setBufferDecompressor(decompressor).build()) {
            TestInputChannel inputChannel = new TestInputChannel(inputGate, 0);

            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
            for (int i = 0; i < bufferSize; i += 8) {
                segment.putLongLittleEndian(i, i);
            }
            Buffer uncompressedBuffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
            uncompressedBuffer.setSize(bufferSize);
            Buffer compressedBuffer = compressor.compressToOriginalBuffer(uncompressedBuffer);
            assertThat(compressedBuffer.isCompressed()).isTrue();

            inputChannel.read(compressedBuffer);
            inputGate.setInputChannels(inputChannel);
            inputGate.notifyChannelNonEmpty(inputChannel);

            Optional<BufferOrEvent> bufferOrEvent = inputGate.getNext();
            assertThat(bufferOrEvent.isPresent()).isTrue();
            assertThat(bufferOrEvent.get().isBuffer()).isTrue();
            ByteBuffer buffer =
                    bufferOrEvent
                            .get()
                            .getBuffer()
                            .getNioBufferReadable()
                            .order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < bufferSize; i += 8) {
                assertThat(buffer.getLong()).isEqualTo(i);
            }
        }
    }

    @Test
    void testNotifyAfterEndOfPartition() throws Exception {
        final SingleInputGate inputGate = createInputGate(2);
        TestInputChannel inputChannel = new TestInputChannel(inputGate, 0);
        inputGate.setInputChannels(inputChannel, new TestInputChannel(inputGate, 1));

        inputChannel.readEndOfPartitionEvent();
        inputChannel.notifyChannelNonEmpty();
        assertThat(inputGate.pollNext().get().getEvent()).isEqualTo(EndOfPartitionEvent.INSTANCE);

        // gate is still active because of secondary channel
        // test if released channel is enqueued
        inputChannel.notifyChannelNonEmpty();
        assertThat(inputGate.pollNext().isPresent()).isFalse();
    }

    @Test
    void testIsAvailable() throws Exception {
        final SingleInputGate inputGate = createInputGate(1);
        TestInputChannel inputChannel = new TestInputChannel(inputGate, 0);
        inputGate.setInputChannels(inputChannel);

        testIsAvailable(inputGate, inputGate, inputChannel);
    }

    @Test
    void testIsAvailableAfterFinished() throws Exception {
        final SingleInputGate inputGate = createInputGate(1);
        TestInputChannel inputChannel = new TestInputChannel(inputGate, 0);
        inputGate.setInputChannels(inputChannel);

        testIsAvailableAfterFinished(
                inputGate,
                () -> {
                    inputChannel.readEndOfPartitionEvent();
                    inputGate.notifyChannelNonEmpty(inputChannel);
                });
    }

    @Test
    void testIsMoreAvailableReadingFromSingleInputChannel() throws Exception {
        // Setup
        final SingleInputGate inputGate = createInputGate();

        final TestInputChannel[] inputChannels =
                new TestInputChannel[] {
                    new TestInputChannel(inputGate, 0), new TestInputChannel(inputGate, 1)
                };

        inputGate.setInputChannels(inputChannels);

        // Test
        inputChannels[0].readBuffer();
        inputChannels[0].readEndOfPartitionEvent();

        inputGate.notifyChannelNonEmpty(inputChannels[0]);

        verifyBufferOrEvent(inputGate, true, 0, true);
        verifyBufferOrEvent(inputGate, false, 0, false);
    }

    @Test
    void testBackwardsEventWithUninitializedChannel() throws Exception {
        // Setup environment
        TestingTaskEventPublisher taskEventPublisher = new TestingTaskEventPublisher();

        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(new NoOpResultSubpartitionView());

        // Setup reader with one local and one unknown input channel

        NettyShuffleEnvironment environment = createNettyShuffleEnvironment();
        final SingleInputGate inputGate =
                createInputGate(environment, 2, ResultPartitionType.PIPELINED);
        final InputChannel[] inputChannels = new InputChannel[2];
        try (Closer closer = Closer.create()) {
            closer.register(environment::close);
            closer.register(inputGate::close);

            // Local
            ResultPartitionID localPartitionId = new ResultPartitionID();

            inputChannels[0] =
                    InputChannelBuilder.newBuilder()
                            .setPartitionId(localPartitionId)
                            .setPartitionManager(partitionManager)
                            .setTaskEventPublisher(taskEventPublisher)
                            .buildLocalChannel(inputGate);

            // Unknown
            ResultPartitionID unknownPartitionId = new ResultPartitionID();

            inputChannels[1] =
                    InputChannelBuilder.newBuilder()
                            .setChannelIndex(1)
                            .setPartitionId(unknownPartitionId)
                            .setPartitionManager(partitionManager)
                            .setTaskEventPublisher(taskEventPublisher)
                            .buildUnknownChannel(inputGate);

            setupInputGate(inputGate, inputChannels);

            // Only the local channel can request
            assertThat(partitionManager.counter).isEqualTo(1);

            // Send event backwards and initialize unknown channel afterwards
            final TaskEvent event = new TestTaskEvent();
            inputGate.sendTaskEvent(event);

            // Only the local channel can send out the event
            assertThat(taskEventPublisher.counter).isEqualTo(1);

            // After the update, the pending event should be send to local channel

            ResourceID location = ResourceID.generate();
            inputGate.updateInputChannel(
                    location,
                    createRemoteWithIdAndLocation(unknownPartitionId.getPartitionId(), location));

            assertThat(partitionManager.counter).isEqualTo(2);
            assertThat(taskEventPublisher.counter).isEqualTo(2);
        }
    }

    /**
     * Tests that an update channel does not trigger a partition request before the UDF has
     * requested any partitions. Otherwise, this can lead to races when registering a listener at
     * the gate (e.g. in UnionInputGate), which can result in missed buffer notifications at the
     * listener.
     */
    @Test
    void testUpdateChannelBeforeRequest() throws Exception {
        SingleInputGate inputGate = createInputGate(1);

        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(new NoOpResultSubpartitionView());

        InputChannel unknown =
                InputChannelBuilder.newBuilder()
                        .setPartitionManager(partitionManager)
                        .buildUnknownChannel(inputGate);
        inputGate.setInputChannels(unknown);

        // Update to a local channel and verify that no request is triggered
        ResultPartitionID resultPartitionID = unknown.getPartitionId();
        ResourceID location = ResourceID.generate();
        inputGate.updateInputChannel(
                location,
                createRemoteWithIdAndLocation(resultPartitionID.getPartitionId(), location));

        assertThat(partitionManager.counter).isEqualTo(0);
    }

    /**
     * Test unknown input channel can set resultPartitionId correctly when update to local input
     * channel, this occurs in the case of speculative execution that unknown input channel only
     * carries original resultPartitionId.
     */
    @Test
    void testUpdateLocalInputChannelWithNewPartitionId() throws Exception {
        SingleInputGate inputGate = createInputGate(1);

        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(new NoOpResultSubpartitionView());

        ResultPartitionID oldPartitionId = new ResultPartitionID();

        InputChannel unknown =
                InputChannelBuilder.newBuilder()
                        .setPartitionManager(partitionManager)
                        .setPartitionId(oldPartitionId)
                        .buildUnknownChannel(inputGate);
        inputGate.setInputChannels(unknown);

        // Update to a local channel and verify that no request is triggered
        ResultPartitionID resultPartitionID = unknown.getPartitionId();
        assertThat(resultPartitionID).isEqualTo(oldPartitionId);

        ResultPartitionID newPartitionId =
                new ResultPartitionID(
                        // speculative execution have the same IntermediateResultPartitionID with
                        // original, only executionAttemptID is different.
                        oldPartitionId.getPartitionId(), ExecutionAttemptID.randomId());
        ResourceID location = ResourceID.generate();
        NettyShuffleDescriptor nettyShuffleDescriptor =
                NettyShuffleDescriptorBuilder.newBuilder()
                        .setId(newPartitionId)
                        .setProducerLocation(location)
                        .buildLocal();
        inputGate.updateInputChannel(location, nettyShuffleDescriptor);

        InputChannel newChannel = inputGate.getChannel(0);
        assertThat(newChannel).isInstanceOf(LocalInputChannel.class);
        assertThat(newChannel.partitionId).isEqualTo(newPartitionId);
    }

    /**
     * Tests that the release of the input gate is noticed while polling the channels for available
     * data.
     */
    @Test
    void testReleaseWhilePollingChannel() throws Exception {
        final AtomicReference<Exception> asyncException = new AtomicReference<>();

        // Setup the input gate with a single channel that does nothing
        final SingleInputGate inputGate = createInputGate(1);

        InputChannel inputChannel = InputChannelBuilder.newBuilder().buildUnknownChannel(inputGate);
        inputGate.setInputChannels(inputChannel);

        // Start the consumer in a separate Thread
        Thread asyncConsumer =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            inputGate.getNext();
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
        assertThat(success).as("Did not trigger blocking buffer request.").isTrue();

        // Release the input gate
        inputGate.close();

        // Wait for Thread to finish and verify expected Exceptions. If the
        // input gate status is not properly checked during requests, this
        // call will never return.
        asyncConsumer.join();

        assertThat(asyncException.get()).isNotNull();
        assertThat(asyncException.get().getClass()).isEqualTo(IllegalStateException.class);
    }

    /** Tests request back off configuration is correctly forwarded to the channels. */
    @Test
    void testRequestBackoffConfiguration() throws Exception {
        IntermediateResultPartitionID[] partitionIds =
                new IntermediateResultPartitionID[] {
                    new IntermediateResultPartitionID(),
                    new IntermediateResultPartitionID(),
                    new IntermediateResultPartitionID()
                };

        int initialBackoff = 137;
        int partitionRequestTimeout = 600;
        int maxBackoff = 1001;

        final NettyShuffleEnvironment netEnv =
                new NettyShuffleEnvironmentBuilder()
                        .setPartitionRequestInitialBackoff(initialBackoff)
                        .setPartitionRequestTimeout(partitionRequestTimeout)
                        .setPartitionRequestMaxBackoff(maxBackoff)
                        .build();

        SingleInputGate gate =
                createSingleInputGate(partitionIds, ResultPartitionType.PIPELINED, netEnv);
        gate.setChannelStateWriter(ChannelStateWriter.NO_OP);

        gate.finishReadRecoveredState();
        while (!gate.getStateConsumedFuture().isDone()) {
            gate.pollNext();
        }
        gate.convertRecoveredInputChannels();

        try (Closer closer = Closer.create()) {
            closer.register(netEnv::close);
            closer.register(gate::close);

            assertThat(gate.getConsumedPartitionType()).isEqualTo(ResultPartitionType.PIPELINED);

            Map<SubpartitionInfo, InputChannel> channelMap = gate.getInputChannels();

            assertThat(channelMap.size()).isEqualTo(3);
            channelMap
                    .values()
                    .forEach(
                            channel -> {
                                try {
                                    channel.checkError();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            InputChannel localChannel = channelMap.get(createSubpartitionInfo(partitionIds[0]));
            assertThat(localChannel.getClass()).isEqualTo(LocalInputChannel.class);

            InputChannel unknownChannel = channelMap.get(createSubpartitionInfo(partitionIds[2]));
            assertThat(unknownChannel.getClass()).isEqualTo(UnknownInputChannel.class);

            InputChannel[] channels = new InputChannel[] {localChannel, unknownChannel};
            for (InputChannel ch : channels) {
                assertThat(ch.getCurrentBackoff()).isEqualTo(0);

                assertThat(ch.increaseBackoff()).isTrue();
                assertThat(ch.getCurrentBackoff()).isEqualTo(initialBackoff);

                assertThat(ch.increaseBackoff()).isTrue();
                assertThat(ch.getCurrentBackoff()).isEqualTo(initialBackoff * 2);

                assertThat(ch.increaseBackoff()).isTrue();
                assertThat(ch.getCurrentBackoff()).isEqualTo(initialBackoff * 2 * 2);

                assertThat(ch.increaseBackoff()).isTrue();
                assertThat(ch.getCurrentBackoff()).isEqualTo(maxBackoff);

                assertThat(ch.increaseBackoff()).isFalse();
            }

            InputChannel remoteChannel = channelMap.get(createSubpartitionInfo(partitionIds[1]));
            assertThat(remoteChannel.getClass()).isEqualTo(RemoteInputChannel.class);

            assertThat(remoteChannel.getCurrentBackoff()).isEqualTo(0);

            assertThat(remoteChannel.increaseBackoff()).isTrue();
            assertThat(remoteChannel.getCurrentBackoff()).isEqualTo(partitionRequestTimeout);

            assertThat(remoteChannel.increaseBackoff()).isTrue();
            assertThat(remoteChannel.getCurrentBackoff()).isEqualTo(partitionRequestTimeout * 2);

            assertThat(remoteChannel.increaseBackoff()).isTrue();
            assertThat(remoteChannel.getCurrentBackoff()).isEqualTo(partitionRequestTimeout * 3);

            assertThat(remoteChannel.increaseBackoff()).isFalse();
        }
    }

    /** Tests that input gate requests and assigns network buffers for remote input channel. */
    @Test
    void testRequestBuffersWithRemoteInputChannel() throws Exception {
        final NettyShuffleEnvironment network = createNettyShuffleEnvironment();
        final SingleInputGate inputGate =
                createInputGate(network, 1, ResultPartitionType.PIPELINED_BOUNDED);
        int buffersPerChannel = 2;
        int extraNetworkBuffersPerGate = 8;

        try (Closer closer = Closer.create()) {
            closer.register(network::close);
            closer.register(inputGate::close);

            RemoteInputChannel remote =
                    InputChannelBuilder.newBuilder()
                            .setupFromNettyShuffleEnvironment(network)
                            .setConnectionManager(new TestingConnectionManager())
                            .buildRemoteChannel(inputGate);
            inputGate.setInputChannels(remote);
            inputGate.setup();

            NetworkBufferPool bufferPool = network.getNetworkBufferPool();
            // only the exclusive buffers should be assigned/available now
            assertThat(remote.getNumberOfAvailableBuffers()).isEqualTo(buffersPerChannel);

            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .isEqualTo(bufferPool.getTotalNumberOfMemorySegments() - buffersPerChannel - 1);
            // note: exclusive buffers are not handed out into LocalBufferPool and are thus not
            // counted
            assertThat(bufferPool.countBuffers()).isEqualTo(extraNetworkBuffersPerGate);
        }
    }

    /**
     * Tests that input gate requests and assigns network buffers when unknown input channel updates
     * to remote input channel.
     */
    @Test
    void testRequestBuffersWithUnknownInputChannel() throws Exception {
        final NettyShuffleEnvironment network = createNettyShuffleEnvironment();
        final SingleInputGate inputGate =
                createInputGate(network, 1, ResultPartitionType.PIPELINED_BOUNDED);
        int buffersPerChannel = 2;
        int extraNetworkBuffersPerGate = 8;

        try (Closer closer = Closer.create()) {
            closer.register(network::close);
            closer.register(inputGate::close);

            final ResultPartitionID resultPartitionId = new ResultPartitionID();
            InputChannel inputChannel =
                    buildUnknownInputChannel(network, inputGate, resultPartitionId, 0);

            inputGate.setInputChannels(inputChannel);
            inputGate.setup();
            NetworkBufferPool bufferPool = network.getNetworkBufferPool();

            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .isEqualTo(bufferPool.getTotalNumberOfMemorySegments() - 1);
            // note: exclusive buffers are not handed out into LocalBufferPool and are thus not
            // counted
            assertThat(bufferPool.countBuffers()).isEqualTo(extraNetworkBuffersPerGate);

            // Trigger updates to remote input channel from unknown input channel
            inputGate.updateInputChannel(
                    ResourceID.generate(),
                    createRemoteWithIdAndLocation(
                            resultPartitionId.getPartitionId(), ResourceID.generate()));

            RemoteInputChannel remote =
                    (RemoteInputChannel)
                            inputGate
                                    .getInputChannels()
                                    .get(
                                            createSubpartitionInfo(
                                                    resultPartitionId.getPartitionId()));
            // only the exclusive buffers should be assigned/available now
            assertThat(remote.getNumberOfAvailableBuffers()).isEqualTo(buffersPerChannel);

            assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                    .isEqualTo(bufferPool.getTotalNumberOfMemorySegments() - buffersPerChannel - 1);
            // note: exclusive buffers are not handed out into LocalBufferPool and are thus not
            // counted
            assertThat(bufferPool.countBuffers()).isEqualTo(extraNetworkBuffersPerGate);
        }
    }

    /**
     * Tests that input gate can successfully convert unknown input channels into local and remote
     * channels.
     */
    @Test
    void testUpdateUnknownInputChannel() throws Exception {
        final NettyShuffleEnvironment network = createNettyShuffleEnvironment();

        final ResultPartition localResultPartition =
                new ResultPartitionBuilder()
                        .setResultPartitionManager(network.getResultPartitionManager())
                        .setupBufferPoolFactoryFromNettyShuffleEnvironment(network)
                        .build();

        final ResultPartition remoteResultPartition =
                new ResultPartitionBuilder()
                        .setResultPartitionManager(network.getResultPartitionManager())
                        .setupBufferPoolFactoryFromNettyShuffleEnvironment(network)
                        .build();

        localResultPartition.setup();
        remoteResultPartition.setup();

        final SingleInputGate inputGate =
                createInputGate(network, 2, ResultPartitionType.PIPELINED);
        final InputChannel[] inputChannels = new InputChannel[2];

        try (Closer closer = Closer.create()) {
            closer.register(network::close);
            closer.register(inputGate::close);

            final ResultPartitionID localResultPartitionId = localResultPartition.getPartitionId();
            inputChannels[0] =
                    buildUnknownInputChannel(network, inputGate, localResultPartitionId, 0);

            final ResultPartitionID remoteResultPartitionId =
                    remoteResultPartition.getPartitionId();
            inputChannels[1] =
                    buildUnknownInputChannel(network, inputGate, remoteResultPartitionId, 1);

            inputGate.setInputChannels(inputChannels);
            inputGate.setup();

            assertThat(
                            inputGate
                                    .getInputChannels()
                                    .get(
                                            createSubpartitionInfo(
                                                    remoteResultPartitionId.getPartitionId())))
                    .isInstanceOf(UnknownInputChannel.class);
            assertThat(
                            inputGate
                                    .getInputChannels()
                                    .get(
                                            createSubpartitionInfo(
                                                    localResultPartitionId.getPartitionId())))
                    .isInstanceOf(UnknownInputChannel.class);

            ResourceID localLocation = ResourceID.generate();

            // Trigger updates to remote input channel from unknown input channel
            inputGate.updateInputChannel(
                    localLocation,
                    createRemoteWithIdAndLocation(
                            remoteResultPartitionId.getPartitionId(), ResourceID.generate()));

            assertThat(
                            inputGate
                                    .getInputChannels()
                                    .get(
                                            createSubpartitionInfo(
                                                    remoteResultPartitionId.getPartitionId())))
                    .isInstanceOf(RemoteInputChannel.class);
            assertThat(
                            inputGate
                                    .getInputChannels()
                                    .get(
                                            createSubpartitionInfo(
                                                    localResultPartitionId.getPartitionId())))
                    .isInstanceOf(UnknownInputChannel.class);

            // Trigger updates to local input channel from unknown input channel
            inputGate.updateInputChannel(
                    localLocation,
                    createRemoteWithIdAndLocation(
                            localResultPartitionId.getPartitionId(), localLocation));

            assertThat(
                            inputGate
                                    .getInputChannels()
                                    .get(
                                            createSubpartitionInfo(
                                                    remoteResultPartitionId.getPartitionId())))
                    .isInstanceOf(RemoteInputChannel.class);
            assertThat(
                            inputGate
                                    .getInputChannels()
                                    .get(
                                            createSubpartitionInfo(
                                                    localResultPartitionId.getPartitionId())))
                    .isInstanceOf(LocalInputChannel.class);
        }
    }

    @Test
    void testSingleInputGateWithSubpartitionIndexRange() throws IOException, InterruptedException {

        IntermediateResultPartitionID[] partitionIds =
                new IntermediateResultPartitionID[] {
                    new IntermediateResultPartitionID(),
                    new IntermediateResultPartitionID(),
                    new IntermediateResultPartitionID()
                };

        IndexRange subpartitionIndexRange = new IndexRange(0, 1);
        final NettyShuffleEnvironment netEnv = new NettyShuffleEnvironmentBuilder().build();

        ResourceID localLocation = ResourceID.generate();

        SingleInputGate gate =
                createSingleInputGate(
                        partitionIds,
                        ResultPartitionType.BLOCKING,
                        subpartitionIndexRange,
                        netEnv,
                        localLocation,
                        new TestingConnectionManager(),
                        new TestingResultPartitionManager(new NoOpResultSubpartitionView()));

        for (InputChannel channel : gate.getInputChannels().values()) {
            if (channel instanceof ChannelStateHolder) {
                ((ChannelStateHolder) channel).setChannelStateWriter(ChannelStateWriter.NO_OP);
            }
        }

        SubpartitionInfo info1 = createSubpartitionInfo(partitionIds[0], 0);
        SubpartitionInfo info2 = createSubpartitionInfo(partitionIds[0], 1);
        SubpartitionInfo info3 = createSubpartitionInfo(partitionIds[1], 0);
        SubpartitionInfo info4 = createSubpartitionInfo(partitionIds[1], 1);
        SubpartitionInfo info5 = createSubpartitionInfo(partitionIds[2], 0);
        SubpartitionInfo info6 = createSubpartitionInfo(partitionIds[2], 1);

        assertThat(gate.getInputChannels().size()).isEqualTo(6);
        assertThat(gate.getInputChannels().get(info1).getConsumedSubpartitionIndex()).isEqualTo(0);
        assertThat(gate.getInputChannels().get(info2).getConsumedSubpartitionIndex()).isEqualTo(1);
        assertThat(gate.getInputChannels().get(info3).getConsumedSubpartitionIndex()).isEqualTo(0);
        assertThat(gate.getInputChannels().get(info4).getConsumedSubpartitionIndex()).isEqualTo(1);
        assertThat(gate.getInputChannels().get(info5).getConsumedSubpartitionIndex()).isEqualTo(0);
        assertThat(gate.getInputChannels().get(info6).getConsumedSubpartitionIndex()).isEqualTo(1);

        assertChannelsType(gate, LocalRecoveredInputChannel.class, Arrays.asList(info1, info2));
        assertChannelsType(gate, RemoteRecoveredInputChannel.class, Arrays.asList(info3, info4));
        assertChannelsType(gate, UnknownInputChannel.class, Arrays.asList(info5, info6));

        // test setup
        gate.setup();
        assertThat(gate.getBufferPool()).isNotNull();
        assertThat(gate.getBufferPool().getNumberOfRequiredMemorySegments()).isEqualTo(1);

        gate.finishReadRecoveredState();
        while (!gate.getStateConsumedFuture().isDone()) {
            gate.pollNext();
        }

        // test request partitions
        gate.requestPartitions();
        gate.pollNext();
        assertChannelsType(gate, LocalInputChannel.class, Arrays.asList(info1, info2));
        assertChannelsType(gate, RemoteInputChannel.class, Arrays.asList(info3, info4));
        assertChannelsType(gate, UnknownInputChannel.class, Arrays.asList(info5, info6));
        for (InputChannel inputChannel : gate.getInputChannels().values()) {
            if (inputChannel instanceof RemoteInputChannel) {
                assertThat(((RemoteInputChannel) inputChannel).getPartitionRequestClient())
                        .isNotNull();
                assertThat(((RemoteInputChannel) inputChannel).getInitialCredit()).isEqualTo(2);
            } else if (inputChannel instanceof LocalInputChannel) {
                assertThat(((LocalInputChannel) inputChannel).getSubpartitionView()).isNotNull();
            }
        }

        // test update channels
        gate.updateInputChannel(
                localLocation, createRemoteWithIdAndLocation(partitionIds[2], localLocation));
        assertChannelsType(gate, LocalInputChannel.class, Arrays.asList(info1, info2));
        assertChannelsType(gate, RemoteInputChannel.class, Arrays.asList(info3, info4));
        assertChannelsType(gate, LocalInputChannel.class, Arrays.asList(info5, info6));
    }

    private void assertChannelsType(
            SingleInputGate gate, Class<?> clazz, List<SubpartitionInfo> infos) {
        for (SubpartitionInfo subpartitionInfo : infos) {
            assertThat(gate.getInputChannels().get(subpartitionInfo)).isInstanceOf(clazz);
        }
    }

    @Test
    void testQueuedBuffers() throws Exception {
        final NettyShuffleEnvironment network = createNettyShuffleEnvironment();

        final BufferWritingResultPartition resultPartition =
                (BufferWritingResultPartition)
                        new ResultPartitionBuilder()
                                .setResultPartitionManager(network.getResultPartitionManager())
                                .setupBufferPoolFactoryFromNettyShuffleEnvironment(network)
                                .build();

        final SingleInputGate inputGate =
                createInputGate(network, 2, ResultPartitionType.PIPELINED);

        final ResultPartitionID localResultPartitionId = resultPartition.getPartitionId();
        final InputChannel[] inputChannels = new InputChannel[2];

        final RemoteInputChannel remoteInputChannel =
                InputChannelBuilder.newBuilder()
                        .setChannelIndex(1)
                        .setupFromNettyShuffleEnvironment(network)
                        .setConnectionManager(new TestingConnectionManager())
                        .buildRemoteChannel(inputGate);
        inputChannels[0] = remoteInputChannel;

        inputChannels[1] =
                InputChannelBuilder.newBuilder()
                        .setChannelIndex(0)
                        .setPartitionId(localResultPartitionId)
                        .setupFromNettyShuffleEnvironment(network)
                        .setConnectionManager(new TestingConnectionManager())
                        .buildLocalChannel(inputGate);

        try (Closer closer = Closer.create()) {
            closer.register(network::close);
            closer.register(inputGate::close);
            closer.register(resultPartition::release);

            resultPartition.setup();
            setupInputGate(inputGate, inputChannels);

            remoteInputChannel.onBuffer(createBuffer(1), 0, 0);
            assertThat(inputGate.getNumberOfQueuedBuffers()).isEqualTo(1);

            resultPartition.emitRecord(ByteBuffer.allocate(1), 0);
            assertThat(inputGate.getNumberOfQueuedBuffers()).isEqualTo(2);
        }
    }

    /**
     * Tests that if the {@link PartitionNotFoundException} is set onto one {@link InputChannel},
     * then it would be thrown directly via {@link SingleInputGate#getNext()}. So we could confirm
     * the {@link SingleInputGate} would not swallow or transform the original exception.
     */
    @Test
    void testPartitionNotFoundExceptionWhileGetNextBuffer() throws Exception {
        final SingleInputGate inputGate = InputChannelTestUtils.createSingleInputGate(1);
        final LocalInputChannel localChannel =
                createLocalInputChannel(inputGate, new ResultPartitionManager());
        final ResultPartitionID partitionId = localChannel.getPartitionId();

        inputGate.setInputChannels(localChannel);
        localChannel.setError(new PartitionNotFoundException(partitionId));
        assertThatThrownBy(inputGate::getNext)
                .isInstanceOfSatisfying(
                        PartitionNotFoundException.class,
                        (notFoundException) ->
                                assertThat(notFoundException.getPartitionId())
                                        .isEqualTo(partitionId));
    }

    @Test
    void testAnnounceBufferSize() throws Exception {
        final SingleInputGate inputGate = InputChannelTestUtils.createSingleInputGate(2);
        final LocalInputChannel localChannel =
                createLocalInputChannel(
                        inputGate,
                        new TestingResultPartitionManager(createResultSubpartitionView()));
        RemoteInputChannel remoteInputChannel = createRemoteInputChannel(inputGate, 1);

        inputGate.setInputChannels(localChannel, remoteInputChannel);
        inputGate.requestPartitions();

        inputGate.announceBufferSize(10);

        // Release all channels and gate one by one.

        localChannel.releaseAllResources();

        inputGate.announceBufferSize(11);

        remoteInputChannel.releaseAllResources();

        inputGate.announceBufferSize(12);

        inputGate.close();

        inputGate.announceBufferSize(13);

        // No exceptions should happen.
    }

    @Test
    void testInputGateRemovalFromNettyShuffleEnvironment() throws Exception {
        NettyShuffleEnvironment network = createNettyShuffleEnvironment();

        try (Closer closer = Closer.create()) {
            closer.register(network::close);

            int numberOfGates = 10;
            Map<InputGateID, SingleInputGate> createdInputGatesById =
                    createInputGateWithLocalChannels(network, numberOfGates, 1);

            assertThat(createdInputGatesById.size()).isEqualTo(numberOfGates);

            for (InputGateID id : createdInputGatesById.keySet()) {
                assertThat(network.getInputGate(id).isPresent()).isTrue();
                createdInputGatesById.get(id).close();
                assertThat(network.getInputGate(id).isPresent()).isFalse();
            }
        }
    }

    @Test
    void testSingleInputGateInfo() {
        final int numSingleInputGates = 2;
        final int numInputChannels = 3;

        for (int i = 0; i < numSingleInputGates; i++) {
            final SingleInputGate gate =
                    new SingleInputGateBuilder()
                            .setSingleInputGateIndex(i)
                            .setNumberOfChannels(numInputChannels)
                            .build();

            int channelCounter = 0;
            for (InputChannel inputChannel : gate.getInputChannels().values()) {
                InputChannelInfo channelInfo = inputChannel.getChannelInfo();

                assertThat(channelInfo.getGateIdx()).isEqualTo(i);
                assertThat(channelInfo.getInputChannelIdx()).isEqualTo(channelCounter++);
            }
        }
    }

    @Test
    void testGetUnfinishedChannels() throws IOException, InterruptedException {
        SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setSingleInputGateIndex(1)
                        .setNumberOfChannels(3)
                        .build();
        final TestInputChannel[] inputChannels =
                new TestInputChannel[] {
                    new TestInputChannel(inputGate, 0),
                    new TestInputChannel(inputGate, 1),
                    new TestInputChannel(inputGate, 2)
                };
        inputGate.setInputChannels(inputChannels);

        assertThat(inputGate.getUnfinishedChannels())
                .isEqualTo(
                        Arrays.asList(
                                inputChannels[0].getChannelInfo(),
                                inputChannels[1].getChannelInfo(),
                                inputChannels[2].getChannelInfo()));

        inputChannels[1].readEndOfPartitionEvent();
        inputGate.notifyChannelNonEmpty(inputChannels[1]);
        inputGate.getNext();
        assertThat(inputGate.getUnfinishedChannels())
                .isEqualTo(
                        Arrays.asList(
                                inputChannels[0].getChannelInfo(),
                                inputChannels[2].getChannelInfo()));

        inputChannels[0].readEndOfPartitionEvent();
        inputGate.notifyChannelNonEmpty(inputChannels[0]);
        inputGate.getNext();
        assertThat(inputGate.getUnfinishedChannels())
                .isEqualTo(Collections.singletonList(inputChannels[2].getChannelInfo()));

        inputChannels[2].readEndOfPartitionEvent();
        inputGate.notifyChannelNonEmpty(inputChannels[2]);
        inputGate.getNext();
        assertThat(inputGate.getUnfinishedChannels()).isEqualTo(Collections.emptyList());
    }

    @Test
    void testBufferInUseCount() throws Exception {
        // Setup
        final SingleInputGate inputGate = createInputGate();

        final TestInputChannel[] inputChannels =
                new TestInputChannel[] {
                    new TestInputChannel(inputGate, 0), new TestInputChannel(inputGate, 1)
                };

        inputGate.setInputChannels(inputChannels);

        // It should be no buffers when all channels are empty.
        assertThat(inputGate.getBuffersInUseCount()).isEqualTo(0);

        // Add buffers into channels.
        inputChannels[0].readBuffer();
        assertThat(inputGate.getBuffersInUseCount()).isEqualTo(1);

        inputChannels[0].readBuffer();
        assertThat(inputGate.getBuffersInUseCount()).isEqualTo(2);

        inputChannels[1].readBuffer();
        assertThat(inputGate.getBuffersInUseCount()).isEqualTo(3);
    }

    @Test
    void testCalculateInputGateNetworkBuffers() throws Exception {
        verifyBuffersInBufferPool(true, 2);
        verifyBuffersInBufferPool(false, 2);
        verifyBuffersInBufferPool(true, 500);
        verifyBuffersInBufferPool(false, 500);
    }

    // ---------------------------------------------------------------------------------------------

    private static void verifyBuffersInBufferPool(boolean isPipeline, int subpartitionRandSize)
            throws Exception {
        IntermediateResultPartitionID[] partitionIds =
                new IntermediateResultPartitionID[] {
                    new IntermediateResultPartitionID(),
                    new IntermediateResultPartitionID(),
                    new IntermediateResultPartitionID()
                };

        IndexRange subpartitionIndexRange = new IndexRange(0, subpartitionRandSize - 1);
        NettyShuffleEnvironmentBuilder nettyShuffleEnvironmentBuilder =
                new NettyShuffleEnvironmentBuilder();
        Optional<Integer> expectMaxRequiredBuffersPerGate =
                isPipeline
                        ? Optional.of(
                                InputGateSpecUtils.DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_STREAM)
                        : Optional.of(
                                InputGateSpecUtils.DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_BATCH);
        nettyShuffleEnvironmentBuilder.setMaxRequiredBuffersPerGate(
                expectMaxRequiredBuffersPerGate);
        NettyShuffleEnvironment netEnv = nettyShuffleEnvironmentBuilder.build();

        SingleInputGate gate =
                createSingleInputGate(
                        partitionIds,
                        isPipeline ? ResultPartitionType.PIPELINED : ResultPartitionType.BLOCKING,
                        subpartitionIndexRange,
                        netEnv,
                        ResourceID.generate(),
                        new TestingConnectionManager(),
                        new TestingResultPartitionManager(new NoOpResultSubpartitionView()));
        gate.setup();

        for (InputChannel inputChannel : gate.getInputChannels().values()) {
            if (inputChannel instanceof RemoteInputChannel) {
                assertThat(((RemoteInputChannel) inputChannel).getInitialCredit()).isEqualTo(0);
            }
        }

        int targetTotalBuffersPerGate = 2 * partitionIds.length * subpartitionRandSize + 8;
        int requiredFloatingBuffersPerGate;
        int totalFloatingBuffersPerGate;
        if (targetTotalBuffersPerGate >= expectMaxRequiredBuffersPerGate.get()) {
            requiredFloatingBuffersPerGate = expectMaxRequiredBuffersPerGate.get();
            totalFloatingBuffersPerGate = targetTotalBuffersPerGate;
        } else {
            requiredFloatingBuffersPerGate = 1;
            totalFloatingBuffersPerGate = 8;
        }
        assertThat(gate.getBufferPool().getNumberOfRequiredMemorySegments())
                .isEqualTo(requiredFloatingBuffersPerGate);
        assertThat(gate.getBufferPool().getMaxNumberOfMemorySegments())
                .isEqualTo(totalFloatingBuffersPerGate);
    }

    private static SubpartitionInfo createSubpartitionInfo(
            IntermediateResultPartitionID partitionId) {
        return createSubpartitionInfo(partitionId, 0);
    }

    private static SubpartitionInfo createSubpartitionInfo(
            IntermediateResultPartitionID partitionId, int subpartitionIndex) {
        return new SubpartitionInfo(partitionId, subpartitionIndex);
    }

    static SingleInputGate createSingleInputGate(
            IntermediateResultPartitionID[] partitionIds,
            ResultPartitionType resultPartitionType,
            NettyShuffleEnvironment netEnv)
            throws IOException {
        return createSingleInputGate(
                partitionIds,
                resultPartitionType,
                new IndexRange(0, 0),
                netEnv,
                ResourceID.generate(),
                null,
                null);
    }

    static SingleInputGate createSingleInputGate(
            IntermediateResultPartitionID[] partitionIds,
            ResultPartitionType resultPartitionType,
            IndexRange subpartitionIndexRange,
            NettyShuffleEnvironment netEnv,
            ResourceID localLocation,
            ConnectionManager connectionManager,
            ResultPartitionManager resultPartitionManager)
            throws IOException {

        ShuffleDescriptorAndIndex[] channelDescs =
                new ShuffleDescriptorAndIndex[] {
                    // Local
                    new ShuffleDescriptorAndIndex(
                            createRemoteWithIdAndLocation(partitionIds[0], localLocation), 0),
                    // Remote
                    new ShuffleDescriptorAndIndex(
                            createRemoteWithIdAndLocation(partitionIds[1], ResourceID.generate()),
                            1),
                    // Unknown
                    new ShuffleDescriptorAndIndex(
                            new UnknownShuffleDescriptor(
                                    new ResultPartitionID(
                                            partitionIds[2], createExecutionAttemptId())),
                            2)
                };

        InputGateDeploymentDescriptor gateDesc =
                new InputGateDeploymentDescriptor(
                        new IntermediateDataSetID(),
                        resultPartitionType,
                        subpartitionIndexRange,
                        channelDescs.length,
                        Collections.singletonList(
                                new TaskDeploymentDescriptor.NonOffloaded<>(
                                        CompressedSerializedValue.fromObject(
                                                new ShuffleDescriptorGroup(channelDescs)))));

        final TaskMetricGroup taskMetricGroup =
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
        return new SingleInputGateFactory(
                        localLocation,
                        netEnv.getConfiguration(),
                        connectionManager != null
                                ? connectionManager
                                : netEnv.getConnectionManager(),
                        resultPartitionManager != null
                                ? resultPartitionManager
                                : netEnv.getResultPartitionManager(),
                        new TaskEventDispatcher(),
                        netEnv.getNetworkBufferPool(),
                        null,
                        null)
                .create(
                        netEnv.createShuffleIOOwnerContext(
                                "TestTask", taskMetricGroup.executionId(), taskMetricGroup),
                        0,
                        gateDesc,
                        SingleInputGateBuilder.NO_OP_PRODUCER_CHECKER,
                        newUnregisteredInputChannelMetrics());
    }

    private static Map<InputGateID, SingleInputGate> createInputGateWithLocalChannels(
            NettyShuffleEnvironment network,
            int numberOfGates,
            @SuppressWarnings("SameParameterValue") int numberOfLocalChannels)
            throws IOException {
        ShuffleDescriptorAndIndex[] channelDescs =
                new ShuffleDescriptorAndIndex[numberOfLocalChannels];
        for (int i = 0; i < numberOfLocalChannels; i++) {
            channelDescs[i] =
                    new ShuffleDescriptorAndIndex(
                            createRemoteWithIdAndLocation(
                                    new IntermediateResultPartitionID(), ResourceID.generate()),
                            i);
        }

        InputGateDeploymentDescriptor[] gateDescs =
                new InputGateDeploymentDescriptor[numberOfGates];
        IntermediateDataSetID[] ids = new IntermediateDataSetID[numberOfGates];
        for (int i = 0; i < numberOfGates; i++) {
            ids[i] = new IntermediateDataSetID();
            gateDescs[i] =
                    new InputGateDeploymentDescriptor(
                            ids[i], ResultPartitionType.PIPELINED, 0, channelDescs);
        }

        ExecutionAttemptID consumerID = createExecutionAttemptId();
        SingleInputGate[] gates =
                network.createInputGates(
                                network.createShuffleIOOwnerContext(
                                        "", consumerID, new UnregisteredMetricsGroup()),
                                SingleInputGateBuilder.NO_OP_PRODUCER_CHECKER,
                                asList(gateDescs))
                        .toArray(new SingleInputGate[] {});
        Map<InputGateID, SingleInputGate> inputGatesById = new HashMap<>();
        for (int i = 0; i < numberOfGates; i++) {
            inputGatesById.put(new InputGateID(ids[i], consumerID), gates[i]);
        }

        return inputGatesById;
    }

    private InputChannel buildUnknownInputChannel(
            NettyShuffleEnvironment network,
            SingleInputGate inputGate,
            ResultPartitionID partitionId,
            int channelIndex) {
        return InputChannelBuilder.newBuilder()
                .setChannelIndex(channelIndex)
                .setPartitionId(partitionId)
                .setupFromNettyShuffleEnvironment(network)
                .setConnectionManager(new TestingConnectionManager())
                .buildUnknownChannel(inputGate);
    }

    private NettyShuffleEnvironment createNettyShuffleEnvironment() {
        return new NettyShuffleEnvironmentBuilder().build();
    }

    static void verifyBufferOrEvent(
            InputGate inputGate,
            boolean expectedIsBuffer,
            int expectedChannelIndex,
            boolean expectedMoreAvailable)
            throws IOException, InterruptedException {

        final Optional<BufferOrEvent> bufferOrEvent = inputGate.getNext();
        assertThat(bufferOrEvent.isPresent()).isTrue();
        assertThat(bufferOrEvent.get().isBuffer()).isEqualTo(expectedIsBuffer);
        assertThat(bufferOrEvent.get().getChannelInfo())
                .isEqualTo(inputGate.getChannel(expectedChannelIndex).getChannelInfo());
        assertThat(bufferOrEvent.get().moreAvailable()).isEqualTo(expectedMoreAvailable);
        if (!expectedMoreAvailable) {
            assertThat(inputGate.pollNext().isPresent()).isFalse();
        }
    }

    private SingleInputGate createInputGate(NettyShuffleEnvironment environment) {
        SingleInputGate inputGate = createInputGate(environment, 3, ResultPartitionType.PIPELINED);
        InputChannel remoteChannel =
                new InputChannelBuilder().setChannelIndex(0).buildRemoteRecoveredChannel(inputGate);
        InputChannel localChannel =
                new InputChannelBuilder().setChannelIndex(1).buildLocalRecoveredChannel(inputGate);
        InputChannel unknownChannel =
                new InputChannelBuilder().setChannelIndex(2).buildUnknownChannel(inputGate);
        inputGate.setInputChannels(remoteChannel, localChannel, unknownChannel);
        return inputGate;
    }

    /**
     * A testing implementation of {@link ResultPartitionManager} which counts the number of {@link
     * ResultSubpartitionView} created.
     */
    public static class TestingResultPartitionManager extends ResultPartitionManager {
        private int counter = 0;
        private final ResultSubpartitionView subpartitionView;

        public TestingResultPartitionManager(ResultSubpartitionView subpartitionView) {
            this.subpartitionView = subpartitionView;
        }

        @Override
        public ResultSubpartitionView createSubpartitionView(
                ResultPartitionID partitionId,
                int subpartitionIndex,
                BufferAvailabilityListener availabilityListener)
                throws IOException {
            ++counter;
            return subpartitionView;
        }
    }

    /**
     * A testing implementation of {@link TaskEventPublisher} which counts the number of publish
     * times.
     */
    private static class TestingTaskEventPublisher implements TaskEventPublisher {
        private int counter = 0;

        @Override
        public boolean publish(ResultPartitionID partitionId, TaskEvent event) {
            ++counter;
            return true;
        }
    }
}
