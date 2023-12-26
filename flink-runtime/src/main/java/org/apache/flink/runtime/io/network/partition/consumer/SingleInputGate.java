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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p>Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p>As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p>When deploying such a program in parallel, the intermediate result will be partitioned over
 * its producing parallel subtasks; each of these partitions is furthermore partitioned into one or
 * more subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p>In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask.
 */
public class SingleInputGate extends IndexedInputGate {

    private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

    /** Lock object to guard partition requests and runtime channel updates. */
    private final Object requestLock = new Object();

    /** The name of the owning task, for logging purposes. */
    private final String owningTaskName;

    private final int gateIndex;

    /**
     * The ID of the consumed intermediate result. Each input gate consumes partitions of the
     * intermediate result specified by this ID. This ID also identifies the input gate at the
     * consuming task.
     */
    private final IntermediateDataSetID consumedResultId;

    /** The type of the partition the input gate is consuming. */
    private final ResultPartitionType consumedPartitionType;

    /**
     * Range of the index of the consumed subpartition of each consumed partition. This index
     * depends on the {@link DistributionPattern} and the subtask indices of the producing and
     * consuming task. The range is inclusive.
     */
    private final IndexRange subpartitionIndexRange;

    /** The number of input channels (equivalent to the number of consumed partitions). */
    private final int numberOfInputChannels;

    /**
     * Input channels. There is one input channel for each consumed subpartition. We store this in a
     * map for runtime updates of single channels.
     */
    private final Map<SubpartitionInfo, InputChannel> inputChannels;

    @GuardedBy("requestLock")
    private final InputChannel[] channels;

    /** Channels, which notified this input gate about available data. */
    private final PrioritizedDeque<InputChannel> inputChannelsWithData = new PrioritizedDeque<>();

    /**
     * Field guaranteeing uniqueness for inputChannelsWithData queue. Both of those fields should be
     * unified onto one.
     */
    @GuardedBy("inputChannelsWithData")
    private final BitSet enqueuedInputChannelsWithData;

    @GuardedBy("inputChannelsWithData")
    private final BitSet channelsWithEndOfPartitionEvents;

    @GuardedBy("inputChannelsWithData")
    private final BitSet channelsWithEndOfUserRecords;

    @GuardedBy("inputChannelsWithData")
    private int[] lastPrioritySequenceNumber;

    /** The partition producer state listener. */
    private final PartitionProducerStateProvider partitionProducerStateProvider;

    /**
     * Buffer pool for incoming buffers. Incoming data from remote channels is copied to buffers
     * from this pool.
     */
    private BufferPool bufferPool;

    private boolean hasReceivedAllEndOfPartitionEvents;

    private boolean hasReceivedEndOfData;

    /** Flag indicating whether partitions have been requested. */
    private boolean requestedPartitionsFlag;

    private final List<TaskEvent> pendingEvents = new ArrayList<>();

    private int numberOfUninitializedChannels;

    /** A timer to retrigger local partition requests. Only initialized if actually needed. */
    private Timer retriggerLocalRequestTimer;

    private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    private final CompletableFuture<Void> closeFuture;

    @Nullable private final BufferDecompressor bufferDecompressor;

    private final MemorySegmentProvider memorySegmentProvider;

    /**
     * The segment to read data from file region of bounded blocking partition by local input
     * channel.
     */
    private final MemorySegment unpooledSegment;

    private final ThroughputCalculator throughputCalculator;
    private final BufferDebloater bufferDebloater;
    private boolean shouldDrainOnEndOfData = true;

    // The consumer client will be null if the tiered storage is not enabled.
    @Nullable private final TieredStorageConsumerClient tieredStorageConsumerClient;

    // The consumer specs in tiered storage will be null if the tiered storage is not enabled.
    @Nullable private final List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs;

    // The availability notifier will be null if the tiered storage is not enabled.
    @Nullable private final AvailabilityNotifier availabilityNotifier;

    public SingleInputGate(
            String owningTaskName,
            int gateIndex,
            IntermediateDataSetID consumedResultId,
            final ResultPartitionType consumedPartitionType,
            IndexRange subpartitionIndexRange,
            int numberOfInputChannels,
            PartitionProducerStateProvider partitionProducerStateProvider,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            @Nullable BufferDecompressor bufferDecompressor,
            MemorySegmentProvider memorySegmentProvider,
            int segmentSize,
            ThroughputCalculator throughputCalculator,
            @Nullable BufferDebloater bufferDebloater,
            @Nullable TieredStorageConsumerClient tieredStorageConsumerClient,
            @Nullable TieredStorageNettyServiceImpl nettyService,
            @Nullable List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs) {

        this.owningTaskName = checkNotNull(owningTaskName);
        Preconditions.checkArgument(0 <= gateIndex, "The gate index must be positive.");
        this.gateIndex = gateIndex;

        this.consumedResultId = checkNotNull(consumedResultId);
        this.consumedPartitionType = checkNotNull(consumedPartitionType);
        this.bufferPoolFactory = checkNotNull(bufferPoolFactory);

        this.subpartitionIndexRange = checkNotNull(subpartitionIndexRange);

        checkArgument(numberOfInputChannels > 0);
        this.numberOfInputChannels = numberOfInputChannels;

        this.inputChannels = CollectionUtil.newHashMapWithExpectedSize(numberOfInputChannels);
        this.channels = new InputChannel[numberOfInputChannels];
        this.channelsWithEndOfPartitionEvents = new BitSet(numberOfInputChannels);
        this.channelsWithEndOfUserRecords = new BitSet(numberOfInputChannels);
        this.enqueuedInputChannelsWithData = new BitSet(numberOfInputChannels);
        this.lastPrioritySequenceNumber = new int[numberOfInputChannels];
        Arrays.fill(lastPrioritySequenceNumber, Integer.MIN_VALUE);

        this.partitionProducerStateProvider = checkNotNull(partitionProducerStateProvider);

        this.bufferDecompressor = bufferDecompressor;
        this.memorySegmentProvider = checkNotNull(memorySegmentProvider);

        this.closeFuture = new CompletableFuture<>();

        this.unpooledSegment = MemorySegmentFactory.allocateUnpooledSegment(segmentSize);
        this.bufferDebloater = bufferDebloater;
        this.throughputCalculator = checkNotNull(throughputCalculator);

        this.tieredStorageConsumerClient = tieredStorageConsumerClient;
        this.tieredStorageConsumerSpecs = tieredStorageConsumerSpecs;
        if (enabledTieredStorage()) {
            this.availabilityNotifier = new AvailabilityNotifierImpl();
            setupTieredStorageNettyService(nettyService, tieredStorageConsumerSpecs);
            tieredStorageConsumerClient.registerAvailabilityNotifier(availabilityNotifier);
        } else {
            this.availabilityNotifier = null;
        }
    }

    protected PrioritizedDeque<InputChannel> getInputChannelsWithData() {
        return inputChannelsWithData;
    }

    @Override
    public void setup() throws IOException {
        checkState(
                this.bufferPool == null,
                "Bug in input gate setup logic: Already registered buffer pool.");

        BufferPool bufferPool = bufferPoolFactory.get();
        setBufferPool(bufferPool);

        setupChannels();
    }

    @Override
    public CompletableFuture<Void> getStateConsumedFuture() {
        synchronized (requestLock) {
            List<CompletableFuture<?>> futures = new ArrayList<>(inputChannels.size());
            for (InputChannel inputChannel : inputChannels.values()) {
                if (inputChannel instanceof RecoveredInputChannel) {
                    futures.add(((RecoveredInputChannel) inputChannel).getStateConsumedFuture());
                }
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        }
    }

    @Override
    public void requestPartitions() {
        synchronized (requestLock) {
            if (!requestedPartitionsFlag) {
                if (closeFuture.isDone()) {
                    throw new IllegalStateException("Already released.");
                }

                // Sanity checks
                if (numberOfInputChannels != inputChannels.size()) {
                    throw new IllegalStateException(
                            String.format(
                                    "Bug in input gate setup logic: mismatch between "
                                            + "number of total input channels [%s] and the currently set number of input "
                                            + "channels [%s].",
                                    inputChannels.size(), numberOfInputChannels));
                }

                convertRecoveredInputChannels();
                internalRequestPartitions();
            }

            requestedPartitionsFlag = true;
            // Start the reader only when all InputChannels have been converted to either
            // LocalInputChannel or RemoteInputChannel, as this will prevent RecoveredInputChannels
            // from being queued again.
            if (enabledTieredStorage()) {
                tieredStorageConsumerClient.start();
            }
        }
    }

    @VisibleForTesting
    public void convertRecoveredInputChannels() {
        LOG.debug("Converting recovered input channels ({} channels)", getNumberOfInputChannels());
        for (Map.Entry<SubpartitionInfo, InputChannel> entry : inputChannels.entrySet()) {
            InputChannel inputChannel = entry.getValue();
            if (inputChannel instanceof RecoveredInputChannel) {
                try {
                    InputChannel realInputChannel =
                            ((RecoveredInputChannel) inputChannel).toInputChannel();
                    inputChannel.releaseAllResources();
                    entry.setValue(realInputChannel);
                    channels[inputChannel.getChannelIndex()] = realInputChannel;
                } catch (Throwable t) {
                    inputChannel.setError(t);
                    return;
                }
            }
        }
    }

    private void internalRequestPartitions() {
        for (InputChannel inputChannel : inputChannels.values()) {
            try {
                inputChannel.requestSubpartition();
            } catch (Throwable t) {
                inputChannel.setError(t);
                return;
            }
        }
    }

    @Override
    public void finishReadRecoveredState() throws IOException {
        for (final InputChannel channel : channels) {
            if (channel instanceof RecoveredInputChannel) {
                ((RecoveredInputChannel) channel).finishReadRecoveredState();
            }
        }
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    @Override
    public int getNumberOfInputChannels() {
        return numberOfInputChannels;
    }

    @Override
    public int getGateIndex() {
        return gateIndex;
    }

    @Override
    public List<InputChannelInfo> getUnfinishedChannels() {
        List<InputChannelInfo> unfinishedChannels =
                new ArrayList<>(
                        numberOfInputChannels - channelsWithEndOfPartitionEvents.cardinality());
        synchronized (inputChannelsWithData) {
            for (int i = channelsWithEndOfPartitionEvents.nextClearBit(0);
                    i < numberOfInputChannels;
                    i = channelsWithEndOfPartitionEvents.nextClearBit(i + 1)) {
                unfinishedChannels.add(getChannel(i).getChannelInfo());
            }
        }

        return unfinishedChannels;
    }

    @VisibleForTesting
    int getBuffersInUseCount() {
        int total = 0;
        for (InputChannel channel : channels) {
            total += channel.getBuffersInUseCount();
        }
        return total;
    }

    @VisibleForTesting
    public void announceBufferSize(int newBufferSize) {
        for (InputChannel channel : channels) {
            if (!channel.isReleased()) {
                channel.announceBufferSize(newBufferSize);
            }
        }
    }

    @Override
    public void triggerDebloating() {
        if (isFinished() || closeFuture.isDone()) {
            return;
        }

        checkState(bufferDebloater != null, "Buffer debloater should not be null");
        final long currentThroughput = throughputCalculator.calculateThroughput();
        bufferDebloater
                .recalculateBufferSize(currentThroughput, getBuffersInUseCount())
                .ifPresent(this::announceBufferSize);
    }

    public Duration getLastEstimatedTimeToConsume() {
        return bufferDebloater.getLastEstimatedTimeToConsumeBuffers();
    }

    /**
     * Returns the type of this input channel's consumed result partition.
     *
     * @return consumed result partition type
     */
    public ResultPartitionType getConsumedPartitionType() {
        return consumedPartitionType;
    }

    BufferProvider getBufferProvider() {
        return bufferPool;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    MemorySegmentProvider getMemorySegmentProvider() {
        return memorySegmentProvider;
    }

    public String getOwningTaskName() {
        return owningTaskName;
    }

    public int getNumberOfQueuedBuffers() {
        // re-try 3 times, if fails, return 0 for "unknown"
        for (int retry = 0; retry < 3; retry++) {
            try {
                int totalBuffers = 0;

                for (InputChannel channel : inputChannels.values()) {
                    totalBuffers += channel.unsynchronizedGetNumberOfQueuedBuffers();
                }

                return totalBuffers;
            } catch (Exception ex) {
                LOG.debug("Fail to get number of queued buffers :", ex);
            }
        }

        return 0;
    }

    public long getSizeOfQueuedBuffers() {
        // re-try 3 times, if fails, return 0 for "unknown"
        for (int retry = 0; retry < 3; retry++) {
            try {
                long totalSize = 0;

                for (InputChannel channel : inputChannels.values()) {
                    totalSize += channel.unsynchronizedGetSizeOfQueuedBuffers();
                }

                return totalSize;
            } catch (Exception ex) {
                LOG.debug("Fail to get size of queued buffers :", ex);
            }
        }

        return 0;
    }

    public CompletableFuture<Void> getCloseFuture() {
        return closeFuture;
    }

    @Override
    public InputChannel getChannel(int channelIndex) {
        return channels[channelIndex];
    }

    // ------------------------------------------------------------------------
    // Setup/Life-cycle
    // ------------------------------------------------------------------------

    public void setBufferPool(BufferPool bufferPool) {
        checkState(
                this.bufferPool == null,
                "Bug in input gate setup logic: buffer pool has"
                        + "already been set for this input gate.");

        this.bufferPool = checkNotNull(bufferPool);
    }

    /** Assign the exclusive buffers to all remote input channels directly for credit-based mode. */
    @VisibleForTesting
    public void setupChannels() throws IOException {
        // Allocate enough exclusive and floating buffers to guarantee that job can make progress.
        // Note: An exception will be thrown if there is no buffer available in the given timeout.

        // First allocate a single floating buffer to avoid potential deadlock when the exclusive
        // buffer is 0. See FLINK-24035 for more information.
        bufferPool.reserveSegments(1);

        // Next allocate the exclusive buffers per channel when the number of exclusive buffer is
        // larger than 0.
        synchronized (requestLock) {
            for (InputChannel inputChannel : inputChannels.values()) {
                inputChannel.setup();
            }
        }
    }

    public void setInputChannels(InputChannel... channels) {
        if (channels.length != numberOfInputChannels) {
            throw new IllegalArgumentException(
                    "Expected "
                            + numberOfInputChannels
                            + " channels, "
                            + "but got "
                            + channels.length);
        }
        synchronized (requestLock) {
            System.arraycopy(channels, 0, this.channels, 0, numberOfInputChannels);
            for (InputChannel inputChannel : channels) {
                IntermediateResultPartitionID partitionId =
                        inputChannel.getPartitionId().getPartitionId();
                int subpartitionIndex = inputChannel.getConsumedSubpartitionIndex();
                if (inputChannels.put(
                                        new SubpartitionInfo(partitionId, subpartitionIndex),
                                        inputChannel)
                                == null
                        && inputChannel instanceof UnknownInputChannel) {

                    numberOfUninitializedChannels++;
                }
            }
        }
    }

    public void updateInputChannel(
            ResourceID localLocation, NettyShuffleDescriptor shuffleDescriptor)
            throws IOException, InterruptedException {
        synchronized (requestLock) {
            if (closeFuture.isDone()) {
                // There was a race with a task failure/cancel
                return;
            }

            IntermediateResultPartitionID partitionId =
                    shuffleDescriptor.getResultPartitionID().getPartitionId();

            for (int subpartitionIndex = subpartitionIndexRange.getStartIndex();
                    subpartitionIndex <= subpartitionIndexRange.getEndIndex();
                    ++subpartitionIndex) {
                SubpartitionInfo subpartitionInfo =
                        new SubpartitionInfo(partitionId, subpartitionIndex);
                InputChannel current = inputChannels.get(subpartitionInfo);

                if (current instanceof UnknownInputChannel) {
                    UnknownInputChannel unknownChannel = (UnknownInputChannel) current;
                    boolean isLocal = shuffleDescriptor.isLocalTo(localLocation);
                    InputChannel newChannel;
                    if (isLocal) {
                        newChannel =
                                unknownChannel.toLocalInputChannel(
                                        shuffleDescriptor.getResultPartitionID());
                    } else {
                        RemoteInputChannel remoteInputChannel =
                                unknownChannel.toRemoteInputChannel(
                                        shuffleDescriptor.getConnectionId());
                        remoteInputChannel.setup();
                        newChannel = remoteInputChannel;
                    }
                    LOG.debug(
                            "{}: Updated unknown input channel to {}.", owningTaskName, newChannel);

                    inputChannels.put(subpartitionInfo, newChannel);
                    channels[current.getChannelIndex()] = newChannel;

                    if (requestedPartitionsFlag) {
                        newChannel.requestSubpartition();
                    }

                    for (TaskEvent event : pendingEvents) {
                        newChannel.sendTaskEvent(event);
                    }

                    if (--numberOfUninitializedChannels == 0) {
                        pendingEvents.clear();
                    }
                }
            }
        }
    }

    /** Retriggers a partition request. */
    public void retriggerPartitionRequest(
            IntermediateResultPartitionID partitionId, int subpartitionIndex) throws IOException {
        synchronized (requestLock) {
            if (!closeFuture.isDone()) {
                final InputChannel ch =
                        inputChannels.get(new SubpartitionInfo(partitionId, subpartitionIndex));

                checkNotNull(ch, "Unknown input channel with ID " + partitionId);

                LOG.debug(
                        "{}: Retriggering partition request {}:{}.",
                        owningTaskName,
                        ch.partitionId,
                        ch.getConsumedSubpartitionIndex());

                if (ch.getClass() == RemoteInputChannel.class) {
                    final RemoteInputChannel rch = (RemoteInputChannel) ch;
                    rch.retriggerSubpartitionRequest();
                } else if (ch.getClass() == LocalInputChannel.class) {
                    final LocalInputChannel ich = (LocalInputChannel) ch;

                    if (retriggerLocalRequestTimer == null) {
                        retriggerLocalRequestTimer = new Timer(true);
                    }

                    ich.retriggerSubpartitionRequest(retriggerLocalRequestTimer);
                } else {
                    throw new IllegalStateException(
                            "Unexpected type of channel to retrigger partition: " + ch.getClass());
                }
            }
        }
    }

    @VisibleForTesting
    Timer getRetriggerLocalRequestTimer() {
        return retriggerLocalRequestTimer;
    }

    MemorySegment getUnpooledSegment() {
        return unpooledSegment;
    }

    @Override
    public void close() throws IOException {
        boolean released = false;
        synchronized (requestLock) {
            if (!closeFuture.isDone()) {
                try {
                    LOG.debug("{}: Releasing {}.", owningTaskName, this);

                    if (retriggerLocalRequestTimer != null) {
                        retriggerLocalRequestTimer.cancel();
                    }

                    for (InputChannel inputChannel : inputChannels.values()) {
                        try {
                            inputChannel.releaseAllResources();
                        } catch (IOException e) {
                            LOG.warn(
                                    "{}: Error during release of channel resources: {}.",
                                    owningTaskName,
                                    e.getMessage(),
                                    e);
                        }
                    }

                    // The buffer pool can actually be destroyed immediately after the
                    // reader received all of the data from the input channels.
                    if (bufferPool != null) {
                        bufferPool.lazyDestroy();
                    }
                } finally {
                    released = true;
                    closeFuture.complete(null);
                }
            }
        }

        if (released) {
            synchronized (inputChannelsWithData) {
                inputChannelsWithData.notifyAll();
            }
            if (enabledTieredStorage()) {
                tieredStorageConsumerClient.close();
            }
        }
    }

    @Override
    public boolean isFinished() {
        return hasReceivedAllEndOfPartitionEvents;
    }

    @Override
    public EndOfDataStatus hasReceivedEndOfData() {
        if (!hasReceivedEndOfData) {
            return EndOfDataStatus.NOT_END_OF_DATA;
        } else if (shouldDrainOnEndOfData) {
            return EndOfDataStatus.DRAINED;
        } else {
            return EndOfDataStatus.STOPPED;
        }
    }

    @Override
    public String toString() {
        return "SingleInputGate{"
                + "owningTaskName='"
                + owningTaskName
                + '\''
                + ", gateIndex="
                + gateIndex
                + '}';
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    @Override
    public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
        return getNextBufferOrEvent(true);
    }

    @Override
    public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
        return getNextBufferOrEvent(false);
    }

    private Optional<BufferOrEvent> getNextBufferOrEvent(boolean blocking)
            throws IOException, InterruptedException {
        if (hasReceivedAllEndOfPartitionEvents) {
            return Optional.empty();
        }

        if (closeFuture.isDone()) {
            throw new CancelTaskException("Input gate is already closed.");
        }
        Optional<InputWithData<InputChannel, Buffer>> next = waitAndGetNextData(blocking);
        if (!next.isPresent()) {
            throughputCalculator.pauseMeasurement();
            return Optional.empty();
        }

        throughputCalculator.resumeMeasurement();

        InputWithData<InputChannel, Buffer> inputWithData = next.get();
        final BufferOrEvent bufferOrEvent =
                transformToBufferOrEvent(
                        inputWithData.data,
                        inputWithData.moreAvailable,
                        inputWithData.input,
                        inputWithData.morePriorityEvents);
        throughputCalculator.incomingDataSize(bufferOrEvent.getSize());
        return Optional.of(bufferOrEvent);
    }

    private Optional<InputWithData<InputChannel, Buffer>> waitAndGetNextData(boolean blocking)
            throws IOException, InterruptedException {
        while (true) {
            synchronized (inputChannelsWithData) {
                Optional<InputChannel> inputChannelOpt = getChannel(blocking);
                if (!inputChannelOpt.isPresent()) {
                    return Optional.empty();
                }

                final InputChannel inputChannel = inputChannelOpt.get();
                Optional<Buffer> buffer;
                if (enabledTieredStorage()) {
                    buffer = readBufferFromTieredStore(inputChannel);
                } else {
                    buffer = readBufferFromInputChannel(inputChannel);
                }
                if (!buffer.isPresent()) {
                    checkUnavailability();
                    continue;
                }

                final boolean morePriorityEvents =
                        inputChannelsWithData.getNumPriorityElements() > 0;
                if (buffer.get().getDataType().hasPriority()) {
                    if (!morePriorityEvents) {
                        priorityAvailabilityHelper.resetUnavailable();
                    }
                }
                checkUnavailability();
                return Optional.of(
                        new InputWithData<>(
                                inputChannel,
                                buffer.get(),
                                !inputChannelsWithData.isEmpty(),
                                morePriorityEvents));
            }
        }
    }

    private Optional<Buffer> readBufferFromInputChannel(InputChannel inputChannel)
            throws IOException, InterruptedException {
        Optional<BufferAndAvailability> bufferAndAvailabilityOpt = inputChannel.getNextBuffer();
        if (!bufferAndAvailabilityOpt.isPresent()) {
            return Optional.empty();
        }
        final BufferAndAvailability bufferAndAvailability = bufferAndAvailabilityOpt.get();
        if (bufferAndAvailability.moreAvailable()) {
            // enqueue the inputChannel at the end to avoid starvation
            queueChannelUnsafe(inputChannel, bufferAndAvailability.morePriorityEvents());
        }
        if (bufferAndAvailability.hasPriority()) {
            lastPrioritySequenceNumber[inputChannel.getChannelIndex()] =
                    bufferAndAvailability.getSequenceNumber();
        }
        return Optional.of(bufferAndAvailability.buffer());
    }

    private Optional<Buffer> readBufferFromTieredStore(InputChannel inputChannel) {
        TieredStorageConsumerSpec tieredStorageConsumerSpec =
                checkNotNull(tieredStorageConsumerSpecs).get(inputChannel.getChannelIndex());
        // If the data is available in the specific partition and subpartition, read buffer through
        // consumer client.
        Optional<Buffer> buffer =
                checkNotNull(tieredStorageConsumerClient)
                        .getNextBuffer(
                                tieredStorageConsumerSpec.getPartitionId(),
                                tieredStorageConsumerSpec.getSubpartitionId());
        // Continue to read buffer from consumer client until the specific partition and
        // subpartition is unavailable because an empty buffer is read.
        buffer.ifPresent(result -> queueChannel(checkNotNull(inputChannel), null, false));
        return buffer;
    }

    private boolean enabledTieredStorage() {
        return tieredStorageConsumerClient != null;
    }

    private void checkUnavailability() {
        assert Thread.holdsLock(inputChannelsWithData);

        if (inputChannelsWithData.isEmpty()) {
            availabilityHelper.resetUnavailable();
        }
    }

    private BufferOrEvent transformToBufferOrEvent(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents)
            throws IOException, InterruptedException {
        if (buffer.isBuffer()) {
            return transformBuffer(buffer, moreAvailable, currentChannel, morePriorityEvents);
        } else {
            return transformEvent(buffer, moreAvailable, currentChannel, morePriorityEvents);
        }
    }

    private BufferOrEvent transformBuffer(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents) {
        return new BufferOrEvent(
                decompressBufferIfNeeded(buffer),
                currentChannel.getChannelInfo(),
                moreAvailable,
                morePriorityEvents);
    }

    private BufferOrEvent transformEvent(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents)
            throws IOException, InterruptedException {
        final AbstractEvent event;
        try {
            event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
        } finally {
            buffer.recycleBuffer();
        }

        if (event.getClass() == EndOfPartitionEvent.class) {
            synchronized (inputChannelsWithData) {
                checkState(!channelsWithEndOfPartitionEvents.get(currentChannel.getChannelIndex()));
                channelsWithEndOfPartitionEvents.set(currentChannel.getChannelIndex());
                hasReceivedAllEndOfPartitionEvents =
                        channelsWithEndOfPartitionEvents.cardinality() == numberOfInputChannels;

                enqueuedInputChannelsWithData.clear(currentChannel.getChannelIndex());
                if (inputChannelsWithData.contains(currentChannel)) {
                    inputChannelsWithData.getAndRemove(channel -> channel == currentChannel);
                }
            }
            if (hasReceivedAllEndOfPartitionEvents) {
                // Because of race condition between:
                // 1. releasing inputChannelsWithData lock in this method and reaching this place
                // 2. empty data notification that re-enqueues a channel we can end up with
                // moreAvailable flag set to true, while we expect no more data.
                checkState(!moreAvailable || !pollNext().isPresent());
                moreAvailable = false;
                markAvailable();
            }

            currentChannel.releaseAllResources();
        } else if (event.getClass() == EndOfData.class) {
            synchronized (inputChannelsWithData) {
                checkState(!channelsWithEndOfUserRecords.get(currentChannel.getChannelIndex()));
                channelsWithEndOfUserRecords.set(currentChannel.getChannelIndex());
                hasReceivedEndOfData =
                        channelsWithEndOfUserRecords.cardinality() == numberOfInputChannels;
                shouldDrainOnEndOfData &= ((EndOfData) event).getStopMode() == StopMode.DRAIN;
            }
        }

        return new BufferOrEvent(
                event,
                buffer.getDataType().hasPriority(),
                currentChannel.getChannelInfo(),
                moreAvailable,
                buffer.getSize(),
                morePriorityEvents);
    }

    private Buffer decompressBufferIfNeeded(Buffer buffer) {
        if (buffer.isCompressed()) {
            try {
                checkNotNull(bufferDecompressor, "Buffer decompressor not set.");
                return bufferDecompressor.decompressToIntermediateBuffer(buffer);
            } finally {
                buffer.recycleBuffer();
            }
        }
        return buffer;
    }

    private void markAvailable() {
        CompletableFuture<?> toNotify;
        synchronized (inputChannelsWithData) {
            toNotify = availabilityHelper.getUnavailableToResetAvailable();
        }
        toNotify.complete(null);
    }

    @Override
    public void sendTaskEvent(TaskEvent event) throws IOException {
        synchronized (requestLock) {
            for (InputChannel inputChannel : inputChannels.values()) {
                inputChannel.sendTaskEvent(event);
            }

            if (numberOfUninitializedChannels > 0) {
                pendingEvents.add(event);
            }
        }
    }

    @Override
    public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {
        checkState(!isFinished(), "InputGate already finished.");
        // BEWARE: consumption resumption only happens for streaming jobs in which all slots
        // are allocated together so there should be no UnknownInputChannel. As a result, it
        // is safe to not synchronize the requestLock here. We will refactor the code to not
        // rely on this assumption in the future.
        channels[channelInfo.getInputChannelIdx()].resumeConsumption();
    }

    @Override
    public void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo) throws IOException {
        checkState(!isFinished(), "InputGate already finished.");
        if (!enabledTieredStorage()) {
            channels[channelInfo.getInputChannelIdx()].acknowledgeAllRecordsProcessed();
        }
    }

    // ------------------------------------------------------------------------
    // Channel notifications
    // ------------------------------------------------------------------------

    void notifyChannelNonEmpty(InputChannel channel) {
        if (enabledTieredStorage()) {
            TieredStorageConsumerSpec tieredStorageConsumerSpec =
                    checkNotNull(tieredStorageConsumerSpecs).get(channel.getChannelIndex());
            checkNotNull(availabilityNotifier)
                    .notifyAvailable(
                            tieredStorageConsumerSpec.getPartitionId(),
                            tieredStorageConsumerSpec.getSubpartitionId());
        } else {
            queueChannel(checkNotNull(channel), null, false);
        }
    }

    /**
     * Notifies that the respective channel has a priority event at the head for the given buffer
     * number.
     *
     * <p>The buffer number limits the notification to the respective buffer and voids the whole
     * notification in case that the buffer has been polled in the meantime. That is, if task thread
     * polls the enqueued priority buffer before this notification occurs (notification is not
     * performed under lock), this buffer number allows {@link #queueChannel(InputChannel, Integer,
     * boolean)} to avoid spurious priority wake-ups.
     */
    void notifyPriorityEvent(InputChannel inputChannel, int prioritySequenceNumber) {
        queueChannel(checkNotNull(inputChannel), prioritySequenceNumber, false);
    }

    void notifyPriorityEventForce(InputChannel inputChannel) {
        queueChannel(checkNotNull(inputChannel), null, true);
    }

    void triggerPartitionStateCheck(ResultPartitionID partitionId, int subpartitionIndex) {
        partitionProducerStateProvider.requestPartitionProducerState(
                consumedResultId,
                partitionId,
                ((PartitionProducerStateProvider.ResponseHandle responseHandle) -> {
                    boolean isProducingState =
                            new RemoteChannelStateChecker(partitionId, owningTaskName)
                                    .isProducerReadyOrAbortConsumption(responseHandle);
                    if (isProducingState) {
                        try {
                            retriggerPartitionRequest(
                                    partitionId.getPartitionId(), subpartitionIndex);
                        } catch (IOException t) {
                            responseHandle.failConsumption(t);
                        }
                    }
                }));
    }

    private void queueChannel(
            InputChannel channel, @Nullable Integer prioritySequenceNumber, boolean forcePriority) {
        try (GateNotificationHelper notification =
                new GateNotificationHelper(this, inputChannelsWithData)) {
            synchronized (inputChannelsWithData) {
                boolean priority = prioritySequenceNumber != null || forcePriority;

                if (!forcePriority
                        && priority
                        && isOutdated(
                                prioritySequenceNumber,
                                lastPrioritySequenceNumber[channel.getChannelIndex()])) {
                    // priority event at the given offset already polled (notification is not atomic
                    // in respect to
                    // buffer enqueuing), so just ignore the notification
                    return;
                }

                if (!queueChannelUnsafe(channel, priority)) {
                    return;
                }

                if (priority && inputChannelsWithData.getNumPriorityElements() == 1) {
                    notification.notifyPriority();
                }
                if (inputChannelsWithData.size() == 1) {
                    notification.notifyDataAvailable();
                }
            }
        }
    }

    private boolean isOutdated(int sequenceNumber, int lastSequenceNumber) {
        if ((lastSequenceNumber < 0) != (sequenceNumber < 0)
                && Math.max(lastSequenceNumber, sequenceNumber) > Integer.MAX_VALUE / 2) {
            // probably overflow of one of the two numbers, the negative one is greater then
            return lastSequenceNumber < 0;
        }
        return lastSequenceNumber >= sequenceNumber;
    }

    /**
     * Queues the channel if not already enqueued and not received EndOfPartition, potentially
     * raising the priority.
     *
     * @return true iff it has been enqueued/prioritized = some change to {@link
     *     #inputChannelsWithData} happened
     */
    private boolean queueChannelUnsafe(InputChannel channel, boolean priority) {
        assert Thread.holdsLock(inputChannelsWithData);
        if (channelsWithEndOfPartitionEvents.get(channel.getChannelIndex())) {
            return false;
        }

        final boolean alreadyEnqueued =
                enqueuedInputChannelsWithData.get(channel.getChannelIndex());
        if (alreadyEnqueued
                && (!priority || inputChannelsWithData.containsPriorityElement(channel))) {
            // already notified / prioritized (double notification), ignore
            return false;
        }

        inputChannelsWithData.add(channel, priority, alreadyEnqueued);
        if (!alreadyEnqueued) {
            enqueuedInputChannelsWithData.set(channel.getChannelIndex());
        }
        return true;
    }

    private Optional<InputChannel> getChannel(boolean blocking) throws InterruptedException {
        assert Thread.holdsLock(inputChannelsWithData);

        while (inputChannelsWithData.isEmpty()) {
            if (closeFuture.isDone()) {
                throw new IllegalStateException("Released");
            }

            if (blocking) {
                inputChannelsWithData.wait();
            } else {
                availabilityHelper.resetUnavailable();
                return Optional.empty();
            }
        }

        InputChannel inputChannel = inputChannelsWithData.poll();
        enqueuedInputChannelsWithData.clear(inputChannel.getChannelIndex());

        return Optional.of(inputChannel);
    }

    private void setupTieredStorageNettyService(
            TieredStorageNettyServiceImpl nettyService,
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs) {
        List<Supplier<InputChannel>> channelSuppliers = new ArrayList<>();
        for (int index = 0; index < channels.length; ++index) {
            int channelIndex = index;
            channelSuppliers.add(() -> channels[channelIndex]);
        }
        nettyService.setupInputChannels(tieredStorageConsumerSpecs, channelSuppliers);
    }

    /** The default implementation of {@link AvailabilityNotifier}. */
    private class AvailabilityNotifierImpl implements AvailabilityNotifier {

        private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
                channelIndexes;

        private AvailabilityNotifierImpl() {
            this.channelIndexes = new HashMap<>();
            for (int index = 0; index < checkNotNull(tieredStorageConsumerSpecs).size(); index++) {
                TieredStorageConsumerSpec spec = tieredStorageConsumerSpecs.get(index);
                channelIndexes
                        .computeIfAbsent(spec.getPartitionId(), ignore -> new HashMap<>())
                        .put(spec.getSubpartitionId(), index);
            }
        }

        public void notifyAvailable(
                TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
            queueChannel(
                    channels[channelIndexes.get(partitionId).get(subpartitionId)], null, false);
        }
    }

    // ------------------------------------------------------------------------

    public Map<SubpartitionInfo, InputChannel> getInputChannels() {
        return inputChannels;
    }

    static class SubpartitionInfo {
        private final IntermediateResultPartitionID partitionID;
        private final int subpartitionIndex;

        SubpartitionInfo(IntermediateResultPartitionID partitionID, int subpartitionIndex) {
            this.partitionID = checkNotNull(partitionID);
            checkArgument(subpartitionIndex >= 0);
            this.subpartitionIndex = subpartitionIndex;
        }

        @Override
        public int hashCode() {
            return partitionID.hashCode() ^ subpartitionIndex;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            } else if (obj != null && obj.getClass() == getClass()) {
                SubpartitionInfo that = (SubpartitionInfo) obj;
                return that.partitionID.equals(this.partitionID)
                        && that.subpartitionIndex == this.subpartitionIndex;
            } else {
                return false;
            }
        }
    }
}
