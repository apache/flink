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
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

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
 * <p>When deploying such a program in parallel, the intermediate result will be partitioned over its
 * producing parallel subtasks; each of these partitions is furthermore partitioned into one or more
 * subpartitions.
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
	 * The index of the consumed subpartition of each consumed partition. This index depends on the
	 * {@link DistributionPattern} and the subtask indices of the producing and consuming task.
	 */
	private final int consumedSubpartitionIndex;

	/** The number of input channels (equivalent to the number of consumed partitions). */
	private final int numberOfInputChannels;

	/**
	 * Input channels. There is a one input channel for each consumed intermediate result partition.
	 * We store this in a map for runtime updates of single channels.
	 */
	private final Map<IntermediateResultPartitionID, InputChannel> inputChannels;

	@GuardedBy("requestLock")
	private final InputChannel[] channels;

	/** Channels, which notified this input gate about available data. */
	private final ArrayDeque<InputChannel> inputChannelsWithData = new ArrayDeque<>();

	/**
	 * Field guaranteeing uniqueness for inputChannelsWithData queue. Both of those fields should be unified
	 * onto one.
	 */
	private final BitSet enqueuedInputChannelsWithData;

	private final BitSet channelsWithEndOfPartitionEvents;

	/** The partition producer state listener. */
	private final PartitionProducerStateProvider partitionProducerStateProvider;

	/**
	 * Buffer pool for incoming buffers. Incoming data from remote channels is copied to buffers
	 * from this pool.
	 */
	private BufferPool bufferPool;

	private boolean hasReceivedAllEndOfPartitionEvents;

	/** Flag indicating whether partitions have been requested. */
	private boolean requestedPartitionsFlag;

	private final List<TaskEvent> pendingEvents = new ArrayList<>();

	private int numberOfUninitializedChannels;

	/** A timer to retrigger local partition requests. Only initialized if actually needed. */
	private Timer retriggerLocalRequestTimer;

	private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

	private final CompletableFuture<Void> closeFuture;

	@Nullable
	private volatile BufferReceivedListener bufferReceivedListener;

	@Nullable
	private final BufferDecompressor bufferDecompressor;

	private final MemorySegmentProvider memorySegmentProvider;

	public SingleInputGate(
		String owningTaskName,
		int gateIndex,
		IntermediateDataSetID consumedResultId,
		final ResultPartitionType consumedPartitionType,
		int consumedSubpartitionIndex,
		int numberOfInputChannels,
		PartitionProducerStateProvider partitionProducerStateProvider,
		SupplierWithException<BufferPool, IOException> bufferPoolFactory,
		@Nullable BufferDecompressor bufferDecompressor,
		MemorySegmentProvider memorySegmentProvider) {

		this.owningTaskName = checkNotNull(owningTaskName);
		Preconditions.checkArgument(0 <= gateIndex, "The gate index must be positive.");
		this.gateIndex = gateIndex;

		this.consumedResultId = checkNotNull(consumedResultId);
		this.consumedPartitionType = checkNotNull(consumedPartitionType);
		this.bufferPoolFactory = checkNotNull(bufferPoolFactory);

		checkArgument(consumedSubpartitionIndex >= 0);
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;

		checkArgument(numberOfInputChannels > 0);
		this.numberOfInputChannels = numberOfInputChannels;

		this.inputChannels = new HashMap<>(numberOfInputChannels);
		this.channels = new InputChannel[numberOfInputChannels];
		this.channelsWithEndOfPartitionEvents = new BitSet(numberOfInputChannels);
		this.enqueuedInputChannelsWithData = new BitSet(numberOfInputChannels);

		this.partitionProducerStateProvider = checkNotNull(partitionProducerStateProvider);

		this.bufferDecompressor = bufferDecompressor;
		this.memorySegmentProvider = checkNotNull(memorySegmentProvider);

		this.closeFuture = new CompletableFuture<>();
	}

	@Override
	public void setup() throws IOException {
		checkState(this.bufferPool == null, "Bug in input gate setup logic: Already registered buffer pool.");
		// assign exclusive buffers to input channels directly and use the rest for floating buffers
		assignExclusiveSegments();

		BufferPool bufferPool = bufferPoolFactory.get();
		setBufferPool(bufferPool);
	}

	@Override
	public CompletableFuture<?> readRecoveredState(ExecutorService executor, ChannelStateReader reader) {
		List<CompletableFuture<?>> futures = getStateConsumedFuture();

		executor.submit(() -> {
			Collection<InputChannel> channels;
			synchronized (requestLock) {
				channels = inputChannels.values();
			}
			internalReadRecoveredState(reader, channels);
		});

		return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
	}

	private List<CompletableFuture<?>> getStateConsumedFuture() {
		synchronized (requestLock) {
			List<CompletableFuture<?>> futures = new ArrayList<>(inputChannels.size());
			for (InputChannel inputChannel : inputChannels.values()) {
				if (inputChannel instanceof RecoveredInputChannel) {
					futures.add(((RecoveredInputChannel) inputChannel).getStateConsumedFuture());
				}
			}
			return futures;
		}
	}

	private void internalReadRecoveredState(ChannelStateReader reader, Collection<InputChannel> inputChannels) {
		for (InputChannel inputChannel : inputChannels) {
			try {
				if (inputChannel instanceof RecoveredInputChannel) {
					((RecoveredInputChannel) inputChannel).readRecoveredState(reader);
				}
			} catch (Throwable t) {
				inputChannel.setError(t);
				return;
			}
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
					throw new IllegalStateException(String.format(
						"Bug in input gate setup logic: mismatch between " +
						"number of total input channels [%s] and the currently set number of input " +
						"channels [%s].",
						inputChannels.size(),
						numberOfInputChannels));
				}

				convertRecoveredInputChannels();
				internalRequestPartitions();
			}

			requestedPartitionsFlag = true;
		}
	}

	@VisibleForTesting
	void convertRecoveredInputChannels() {
		for (Map.Entry<IntermediateResultPartitionID, InputChannel> entry : inputChannels.entrySet()) {
			InputChannel inputChannel = entry.getValue();
			if (inputChannel instanceof RecoveredInputChannel) {
				try {
					InputChannel realInputChannel = ((RecoveredInputChannel) inputChannel).toInputChannel();
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
				inputChannel.requestSubpartition(consumedSubpartitionIndex);
			} catch (Throwable t) {
				inputChannel.setError(t);
				return;
			}
		}
	}

	@Override
	public void registerBufferReceivedListener(BufferReceivedListener bufferReceivedListener) {
		checkState(this.bufferReceivedListener == null, "Trying to overwrite the buffer received listener");
		this.bufferReceivedListener = checkNotNull(bufferReceivedListener);
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

	@Nullable
	BufferReceivedListener getBufferReceivedListener() {
		return bufferReceivedListener;
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

				return  totalBuffers;
			}
			catch (Exception ignored) {}
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
		checkState(this.bufferPool == null, "Bug in input gate setup logic: buffer pool has" +
			"already been set for this input gate.");

		this.bufferPool = checkNotNull(bufferPool);
	}

	/**
	 * Assign the exclusive buffers to all remote input channels directly for credit-based mode.
	 */
	@VisibleForTesting
	public void assignExclusiveSegments() throws IOException {
		synchronized (requestLock) {
			for (InputChannel inputChannel : inputChannels.values()) {
				// Note that although the initial channel would not be RemoteInputChannel at the moment,
				// we might change to generate different type channels based on config future.
				if (inputChannel instanceof RemoteInputChannel) {
					((RemoteInputChannel) inputChannel).assignExclusiveSegments();
				}
				else if (inputChannel instanceof RemoteRecoveredInputChannel) {
					((RemoteRecoveredInputChannel) inputChannel).assignExclusiveSegments();
				}
			}
		}
	}

	public void setInputChannels(InputChannel... channels) {
		if (channels.length != numberOfInputChannels) {
			throw new IllegalArgumentException("Expected " + numberOfInputChannels + " channels, " +
				"but got " + channels.length);
		}
		synchronized (requestLock) {
			System.arraycopy(channels, 0, this.channels, 0, numberOfInputChannels);
			for (InputChannel inputChannel : channels) {
				IntermediateResultPartitionID partitionId = inputChannel.getPartitionId().getPartitionId();
				if (inputChannels.put(partitionId, inputChannel) == null
					&& inputChannel instanceof UnknownInputChannel) {

					numberOfUninitializedChannels++;
				}
			}
		}
	}

	public void updateInputChannel(
			ResourceID localLocation,
			NettyShuffleDescriptor shuffleDescriptor) throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (closeFuture.isDone()) {
				// There was a race with a task failure/cancel
				return;
			}

			IntermediateResultPartitionID partitionId = shuffleDescriptor.getResultPartitionID().getPartitionId();

			InputChannel current = inputChannels.get(partitionId);

			if (current instanceof UnknownInputChannel) {
				UnknownInputChannel unknownChannel = (UnknownInputChannel) current;
				boolean isLocal = shuffleDescriptor.isLocalTo(localLocation);
				InputChannel newChannel;
				if (isLocal) {
					newChannel = unknownChannel.toLocalInputChannel();
				} else {
					RemoteInputChannel remoteInputChannel =
						unknownChannel.toRemoteInputChannel(shuffleDescriptor.getConnectionId());
					remoteInputChannel.assignExclusiveSegments();
					newChannel = remoteInputChannel;
				}
				LOG.debug("{}: Updated unknown input channel to {}.", owningTaskName, newChannel);

				inputChannels.put(partitionId, newChannel);
				channels[current.getChannelIndex()] = newChannel;

				if (requestedPartitionsFlag) {
					newChannel.requestSubpartition(consumedSubpartitionIndex);
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

	/**
	 * Retriggers a partition request.
	 */
	public void retriggerPartitionRequest(IntermediateResultPartitionID partitionId) throws IOException {
		synchronized (requestLock) {
			if (!closeFuture.isDone()) {
				final InputChannel ch = inputChannels.get(partitionId);

				checkNotNull(ch, "Unknown input channel with ID " + partitionId);

				LOG.debug("{}: Retriggering partition request {}:{}.", owningTaskName, ch.partitionId, consumedSubpartitionIndex);

				if (ch.getClass() == RemoteInputChannel.class) {
					final RemoteInputChannel rch = (RemoteInputChannel) ch;
					rch.retriggerSubpartitionRequest(consumedSubpartitionIndex);
				}
				else if (ch.getClass() == LocalInputChannel.class) {
					final LocalInputChannel ich = (LocalInputChannel) ch;

					if (retriggerLocalRequestTimer == null) {
						retriggerLocalRequestTimer = new Timer(true);
					}

					ich.retriggerSubpartitionRequest(retriggerLocalRequestTimer, consumedSubpartitionIndex);
				}
				else {
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
						}
						catch (IOException e) {
							LOG.warn("{}: Error during release of channel resources: {}.",
								owningTaskName, e.getMessage(), e);
						}
					}

					// The buffer pool can actually be destroyed immediately after the
					// reader received all of the data from the input channels.
					if (bufferPool != null) {
						bufferPool.lazyDestroy();
					}
				}
				finally {
					released = true;
					closeFuture.complete(null);
				}
			}
		}

		if (released) {
			synchronized (inputChannelsWithData) {
				inputChannelsWithData.notifyAll();
			}
		}
	}

	@Override
	public boolean isFinished() {
		return hasReceivedAllEndOfPartitionEvents;
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

	private Optional<BufferOrEvent> getNextBufferOrEvent(boolean blocking) throws IOException, InterruptedException {
		if (hasReceivedAllEndOfPartitionEvents) {
			return Optional.empty();
		}

		if (closeFuture.isDone()) {
			throw new CancelTaskException("Input gate is already closed.");
		}

		Optional<InputWithData<InputChannel, BufferAndAvailability>> next = waitAndGetNextData(blocking);
		if (!next.isPresent()) {
			return Optional.empty();
		}

		InputWithData<InputChannel, BufferAndAvailability> inputWithData = next.get();
		return Optional.of(transformToBufferOrEvent(
			inputWithData.data.buffer(),
			inputWithData.moreAvailable,
			inputWithData.input));
	}

	private Optional<InputWithData<InputChannel, BufferAndAvailability>> waitAndGetNextData(boolean blocking)
			throws IOException, InterruptedException {
		while (true) {
			Optional<InputChannel> inputChannel = getChannel(blocking);
			if (!inputChannel.isPresent()) {
				return Optional.empty();
			}

			// Do not query inputChannel under the lock, to avoid potential deadlocks coming from
			// notifications.
			Optional<BufferAndAvailability> result = inputChannel.get().getNextBuffer();

			synchronized (inputChannelsWithData) {
				if (result.isPresent() && result.get().moreAvailable()) {
					// enqueue the inputChannel at the end to avoid starvation
					inputChannelsWithData.add(inputChannel.get());
					enqueuedInputChannelsWithData.set(inputChannel.get().getChannelIndex());
				}

				if (inputChannelsWithData.isEmpty()) {
					availabilityHelper.resetUnavailable();
				}

				if (result.isPresent()) {
					return Optional.of(new InputWithData<>(
						inputChannel.get(),
						result.get(),
						!inputChannelsWithData.isEmpty()));
				}
			}
		}
	}

	private BufferOrEvent transformToBufferOrEvent(
			Buffer buffer,
			boolean moreAvailable,
			InputChannel currentChannel) throws IOException, InterruptedException {
		if (buffer.isBuffer()) {
			return transformBuffer(buffer, moreAvailable, currentChannel);
		} else {
			return transformEvent(buffer, moreAvailable, currentChannel);
		}
	}

	private BufferOrEvent transformBuffer(Buffer buffer, boolean moreAvailable, InputChannel currentChannel) {
		return new BufferOrEvent(decompressBufferIfNeeded(buffer), currentChannel.getChannelIndex(), moreAvailable);
	}

	private BufferOrEvent transformEvent(
			Buffer buffer,
			boolean moreAvailable,
			InputChannel currentChannel) throws IOException, InterruptedException {
		final AbstractEvent event;
		try {
			event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
		} finally {
			buffer.recycleBuffer();
		}

		if (event.getClass() == EndOfPartitionEvent.class) {
			channelsWithEndOfPartitionEvents.set(currentChannel.getChannelIndex());

			if (channelsWithEndOfPartitionEvents.cardinality() == numberOfInputChannels) {
				// Because of race condition between:
				// 1. releasing inputChannelsWithData lock in this method and reaching this place
				// 2. empty data notification that re-enqueues a channel
				// we can end up with moreAvailable flag set to true, while we expect no more data.
				checkState(!moreAvailable || !pollNext().isPresent());
				moreAvailable = false;
				hasReceivedAllEndOfPartitionEvents = true;
				markAvailable();
			}

			currentChannel.releaseAllResources();
		}

		return new BufferOrEvent(event, currentChannel.getChannelIndex(), moreAvailable, buffer.getSize());
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
	public void resumeConsumption(int channelIndex) {
		// BEWARE: consumption resumption only happens for streaming jobs in which all slots
		// are allocated together so there should be no UnknownInputChannel. As a result, it
		// is safe to not synchronize the requestLock here. We will refactor the code to not
		// rely on this assumption in the future.
		channels[channelIndex].resumeConsumption();
	}

	// ------------------------------------------------------------------------
	// Channel notifications
	// ------------------------------------------------------------------------

	void notifyChannelNonEmpty(InputChannel channel) {
		queueChannel(checkNotNull(channel));
	}

	void triggerPartitionStateCheck(ResultPartitionID partitionId) {
		partitionProducerStateProvider.requestPartitionProducerState(
			consumedResultId,
			partitionId,
			((PartitionProducerStateProvider.ResponseHandle responseHandle) -> {
				boolean isProducingState = new RemoteChannelStateChecker(partitionId, owningTaskName)
					.isProducerReadyOrAbortConsumption(responseHandle);
				if (isProducingState) {
					try {
						retriggerPartitionRequest(partitionId.getPartitionId());
					} catch (IOException t) {
						responseHandle.failConsumption(t);
					}
				}
			}));
	}

	private void queueChannel(InputChannel channel) {
		int availableChannels;

		CompletableFuture<?> toNotify = null;

		synchronized (inputChannelsWithData) {
			if (enqueuedInputChannelsWithData.get(channel.getChannelIndex())) {
				return;
			}
			availableChannels = inputChannelsWithData.size();

			inputChannelsWithData.add(channel);
			enqueuedInputChannelsWithData.set(channel.getChannelIndex());

			if (availableChannels == 0) {
				inputChannelsWithData.notifyAll();
				toNotify = availabilityHelper.getUnavailableToResetAvailable();
			}
		}

		if (toNotify != null) {
			toNotify.complete(null);
		}
	}

	private Optional<InputChannel> getChannel(boolean blocking) throws InterruptedException {
		synchronized (inputChannelsWithData) {
			while (inputChannelsWithData.size() == 0) {
				if (closeFuture.isDone()) {
					throw new IllegalStateException("Released");
				}

				if (blocking) {
					inputChannelsWithData.wait();
				}
				else {
					availabilityHelper.resetUnavailable();
					return Optional.empty();
				}
			}

			InputChannel inputChannel = inputChannelsWithData.remove();
			enqueuedInputChannelsWithData.clear(inputChannel.getChannelIndex());
			return Optional.of(inputChannel);
		}
	}

	// ------------------------------------------------------------------------

	public Map<IntermediateResultPartitionID, InputChannel> getInputChannels() {
		return inputChannels;
	}
}
