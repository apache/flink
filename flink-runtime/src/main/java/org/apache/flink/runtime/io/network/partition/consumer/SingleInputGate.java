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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;

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
public class SingleInputGate implements InputGate {

	private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

	/** Lock object to guard partition requests and runtime channel updates. */
	private final Object requestLock = new Object();

	/** The name of the owning task, for logging purposes. */
	private final String owningTaskName;

	/** The job ID of the owning task. */
	private final JobID jobId;

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

	/** Channels, which notified this input gate about available data. */
	private final ArrayDeque<InputChannel> inputChannelsWithData = new ArrayDeque<>();

	/**
	 * Field guaranteeing uniqueness for inputChannelsWithData queue. Both of those fields should be unified
	 * onto one.
	 */
	private final BitSet enqueuedInputChannelsWithData;

	private final BitSet channelsWithEndOfPartitionEvents;

	/** The partition state listener listening to failed partition requests. */
	private final TaskActions taskActions;

	/**
	 * Buffer pool for incoming buffers. Incoming data from remote channels is copied to buffers
	 * from this pool.
	 */
	private BufferPool bufferPool;

	/** Global network buffer pool to request and recycle exclusive buffers (only for credit-based). */
	private NetworkBufferPool networkBufferPool;

	private final boolean isCreditBased;

	private boolean hasReceivedAllEndOfPartitionEvents;

	/** Flag indicating whether partitions have been requested. */
	private boolean requestedPartitionsFlag;

	/** Flag indicating whether all resources have been released. */
	private volatile boolean isReleased;

	/** Registered listener to forward buffer notifications to. */
	private volatile InputGateListener inputGateListener;

	private final List<TaskEvent> pendingEvents = new ArrayList<>();

	private int numberOfUninitializedChannels;

	/** Number of network buffers to use for each remote input channel. */
	private int networkBuffersPerChannel;

	/** A timer to retrigger local partition requests. Only initialized if actually needed. */
	private Timer retriggerLocalRequestTimer;

	public SingleInputGate(
		String owningTaskName,
		JobID jobId,
		IntermediateDataSetID consumedResultId,
		final ResultPartitionType consumedPartitionType,
		int consumedSubpartitionIndex,
		int numberOfInputChannels,
		TaskActions taskActions,
		TaskIOMetricGroup metrics,
		boolean isCreditBased) {

		this.owningTaskName = checkNotNull(owningTaskName);
		this.jobId = checkNotNull(jobId);

		this.consumedResultId = checkNotNull(consumedResultId);
		this.consumedPartitionType = checkNotNull(consumedPartitionType);

		checkArgument(consumedSubpartitionIndex >= 0);
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;

		checkArgument(numberOfInputChannels > 0);
		this.numberOfInputChannels = numberOfInputChannels;

		this.inputChannels = new HashMap<>(numberOfInputChannels);
		this.channelsWithEndOfPartitionEvents = new BitSet(numberOfInputChannels);
		this.enqueuedInputChannelsWithData = new BitSet(numberOfInputChannels);

		this.taskActions = checkNotNull(taskActions);
		this.isCreditBased = isCreditBased;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	@Override
	public int getNumberOfInputChannels() {
		return numberOfInputChannels;
	}

	public IntermediateDataSetID getConsumedResultId() {
		return consumedResultId;
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

	@Override
	public int getPageSize() {
		if (bufferPool != null) {
			return bufferPool.getMemorySegmentSize();
		}
		else {
			throw new IllegalStateException("Input gate has not been initialized with buffers.");
		}
	}

	public int getNumberOfQueuedBuffers() {
		// re-try 3 times, if fails, return 0 for "unknown"
		for (int retry = 0; retry < 3; retry++) {
			try {
				int totalBuffers = 0;

				for (InputChannel channel : inputChannels.values()) {
					if (channel instanceof RemoteInputChannel) {
						totalBuffers += ((RemoteInputChannel) channel).getNumberOfQueuedBuffers();
					}
				}

				return  totalBuffers;
			}
			catch (Exception ignored) {}
		}

		return 0;
	}

	@Override
	public String getOwningTaskName() {
		return owningTaskName;
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
	 *
	 * @param networkBufferPool The global pool to request and recycle exclusive buffers
	 * @param networkBuffersPerChannel The number of exclusive buffers for each channel
	 */
	public void assignExclusiveSegments(NetworkBufferPool networkBufferPool, int networkBuffersPerChannel) throws IOException {
		checkState(this.isCreditBased, "Bug in input gate setup logic: exclusive buffers only exist with credit-based flow control.");
		checkState(this.networkBufferPool == null, "Bug in input gate setup logic: global buffer pool has" +
			"already been set for this input gate.");

		this.networkBufferPool = checkNotNull(networkBufferPool);
		this.networkBuffersPerChannel = networkBuffersPerChannel;

		synchronized (requestLock) {
			for (InputChannel inputChannel : inputChannels.values()) {
				if (inputChannel instanceof RemoteInputChannel) {
					((RemoteInputChannel) inputChannel).assignExclusiveSegments(
						networkBufferPool.requestMemorySegments(networkBuffersPerChannel));
				}
			}
		}
	}

	/**
	 * The exclusive segments are recycled to network buffer pool directly when input channel is released.
	 *
	 * @param segments The exclusive segments need to be recycled
	 */
	public void returnExclusiveSegments(List<MemorySegment> segments) throws IOException {
		networkBufferPool.recycleMemorySegments(segments);
	}

	public void setInputChannel(IntermediateResultPartitionID partitionId, InputChannel inputChannel) {
		synchronized (requestLock) {
			if (inputChannels.put(checkNotNull(partitionId), checkNotNull(inputChannel)) == null
					&& inputChannel instanceof UnknownInputChannel) {

				numberOfUninitializedChannels++;
			}
		}
	}

	public void updateInputChannel(InputChannelDeploymentDescriptor icdd) throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (isReleased) {
				// There was a race with a task failure/cancel
				return;
			}

			final IntermediateResultPartitionID partitionId = icdd.getConsumedPartitionId().getPartitionId();

			InputChannel current = inputChannels.get(partitionId);

			if (current instanceof UnknownInputChannel) {

				UnknownInputChannel unknownChannel = (UnknownInputChannel) current;

				InputChannel newChannel;

				ResultPartitionLocation partitionLocation = icdd.getConsumedPartitionLocation();

				if (partitionLocation.isLocal()) {
					newChannel = unknownChannel.toLocalInputChannel();
				}
				else if (partitionLocation.isRemote()) {
					newChannel = unknownChannel.toRemoteInputChannel(partitionLocation.getConnectionId());

					if (this.isCreditBased) {
						checkState(this.networkBufferPool != null, "Bug in input gate setup logic: " +
							"global buffer pool has not been set for this input gate.");
						((RemoteInputChannel) newChannel).assignExclusiveSegments(
							networkBufferPool.requestMemorySegments(networkBuffersPerChannel));
					}
				}
				else {
					throw new IllegalStateException("Tried to update unknown channel with unknown channel.");
				}

				LOG.debug("{}: Updated unknown input channel to {}.", owningTaskName, newChannel);

				inputChannels.put(partitionId, newChannel);

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
	public void retriggerPartitionRequest(IntermediateResultPartitionID partitionId) throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (!isReleased) {
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

	public void releaseAllResources() throws IOException {
		boolean released = false;
		synchronized (requestLock) {
			if (!isReleased) {
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
					isReleased = true;
					released = true;
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
		synchronized (requestLock) {
			for (InputChannel inputChannel : inputChannels.values()) {
				if (!inputChannel.isReleased()) {
					return false;
				}
			}
		}

		return true;
	}

	@Override
	public void requestPartitions() throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (!requestedPartitionsFlag) {
				if (isReleased) {
					throw new IllegalStateException("Already released.");
				}

				// Sanity checks
				if (numberOfInputChannels != inputChannels.size()) {
					throw new IllegalStateException("Bug in input gate setup logic: mismatch between" +
							"number of total input channels and the currently set number of input " +
							"channels.");
				}

				for (InputChannel inputChannel : inputChannels.values()) {
					inputChannel.requestSubpartition(consumedSubpartitionIndex);
				}
			}

			requestedPartitionsFlag = true;
		}
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	@Override
	public Optional<BufferOrEvent> getNextBufferOrEvent() throws IOException, InterruptedException {
		return getNextBufferOrEvent(true);
	}

	@Override
	public Optional<BufferOrEvent> pollNextBufferOrEvent() throws IOException, InterruptedException {
		return getNextBufferOrEvent(false);
	}

	private Optional<BufferOrEvent> getNextBufferOrEvent(boolean blocking) throws IOException, InterruptedException {
		if (hasReceivedAllEndOfPartitionEvents) {
			return Optional.empty();
		}

		if (isReleased) {
			throw new IllegalStateException("Released");
		}

		requestPartitions();

		InputChannel currentChannel;
		boolean moreAvailable;
		Optional<BufferAndAvailability> result = Optional.empty();

		do {
			synchronized (inputChannelsWithData) {
				while (inputChannelsWithData.size() == 0) {
					if (isReleased) {
						throw new IllegalStateException("Released");
					}

					if (blocking) {
						inputChannelsWithData.wait();
					}
					else {
						return Optional.empty();
					}
				}

				currentChannel = inputChannelsWithData.remove();
				enqueuedInputChannelsWithData.clear(currentChannel.getChannelIndex());
				moreAvailable = !inputChannelsWithData.isEmpty();
			}

			result = currentChannel.getNextBuffer();
		} while (!result.isPresent());

		// this channel was now removed from the non-empty channels queue
		// we re-add it in case it has more data, because in that case no "non-empty" notification
		// will come for that channel
		if (result.get().moreAvailable()) {
			queueChannel(currentChannel);
			moreAvailable = true;
		}

		final Buffer buffer = result.get().buffer();
		if (buffer.isBuffer()) {
			return Optional.of(new BufferOrEvent(buffer, currentChannel.getChannelIndex(), moreAvailable));
		}
		else {
			final AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());

			if (event.getClass() == EndOfPartitionEvent.class) {
				channelsWithEndOfPartitionEvents.set(currentChannel.getChannelIndex());

				if (channelsWithEndOfPartitionEvents.cardinality() == numberOfInputChannels) {
					// Because of race condition between:
					// 1. releasing inputChannelsWithData lock in this method and reaching this place
					// 2. empty data notification that re-enqueues a channel
					// we can end up with moreAvailable flag set to true, while we expect no more data.
					checkState(!moreAvailable || !pollNextBufferOrEvent().isPresent());
					moreAvailable = false;
					hasReceivedAllEndOfPartitionEvents = true;
				}

				currentChannel.notifySubpartitionConsumed();

				currentChannel.releaseAllResources();
			}

			return Optional.of(new BufferOrEvent(event, currentChannel.getChannelIndex(), moreAvailable));
		}
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

	// ------------------------------------------------------------------------
	// Channel notifications
	// ------------------------------------------------------------------------

	@Override
	public void registerListener(InputGateListener inputGateListener) {
		if (this.inputGateListener == null) {
			this.inputGateListener = inputGateListener;
		} else {
			throw new IllegalStateException("Multiple listeners");
		}
	}

	void notifyChannelNonEmpty(InputChannel channel) {
		queueChannel(checkNotNull(channel));
	}

	void triggerPartitionStateCheck(ResultPartitionID partitionId) {
		taskActions.triggerPartitionProducerStateCheck(jobId, consumedResultId, partitionId);
	}

	private void queueChannel(InputChannel channel) {
		int availableChannels;

		synchronized (inputChannelsWithData) {
			if (enqueuedInputChannelsWithData.get(channel.getChannelIndex())) {
				return;
			}
			availableChannels = inputChannelsWithData.size();

			inputChannelsWithData.add(channel);
			enqueuedInputChannelsWithData.set(channel.getChannelIndex());

			if (availableChannels == 0) {
				inputChannelsWithData.notifyAll();
			}
		}

		if (availableChannels == 0) {
			InputGateListener listener = inputGateListener;
			if (listener != null) {
				listener.notifyInputGateNonEmpty(this);
			}
		}
	}

	// ------------------------------------------------------------------------

	Map<IntermediateResultPartitionID, InputChannel> getInputChannels() {
		return inputChannels;
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates an input gate and all of its input channels.
	 */
	public static SingleInputGate create(
		String owningTaskName,
		JobID jobId,
		ExecutionAttemptID executionId,
		InputGateDeploymentDescriptor igdd,
		NetworkEnvironment networkEnvironment,
		TaskActions taskActions,
		TaskIOMetricGroup metrics) {

		final IntermediateDataSetID consumedResultId = checkNotNull(igdd.getConsumedResultId());
		final ResultPartitionType consumedPartitionType = checkNotNull(igdd.getConsumedPartitionType());

		final int consumedSubpartitionIndex = igdd.getConsumedSubpartitionIndex();
		checkArgument(consumedSubpartitionIndex >= 0);

		final InputChannelDeploymentDescriptor[] icdd = checkNotNull(igdd.getInputChannelDeploymentDescriptors());

		final SingleInputGate inputGate = new SingleInputGate(
			owningTaskName, jobId, consumedResultId, consumedPartitionType, consumedSubpartitionIndex,
			icdd.length, taskActions, metrics, networkEnvironment.isCreditBased());

		// Create the input channels. There is one input channel for each consumed partition.
		final InputChannel[] inputChannels = new InputChannel[icdd.length];

		int numLocalChannels = 0;
		int numRemoteChannels = 0;
		int numUnknownChannels = 0;

		for (int i = 0; i < inputChannels.length; i++) {
			final ResultPartitionID partitionId = icdd[i].getConsumedPartitionId();
			final ResultPartitionLocation partitionLocation = icdd[i].getConsumedPartitionLocation();

			if (partitionLocation.isLocal()) {
				inputChannels[i] = new LocalInputChannel(inputGate, i, partitionId,
					networkEnvironment.getResultPartitionManager(),
					networkEnvironment.getTaskEventDispatcher(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numLocalChannels++;
			}
			else if (partitionLocation.isRemote()) {
				inputChannels[i] = new RemoteInputChannel(inputGate, i, partitionId,
					partitionLocation.getConnectionId(),
					networkEnvironment.getConnectionManager(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numRemoteChannels++;
			}
			else if (partitionLocation.isUnknown()) {
				inputChannels[i] = new UnknownInputChannel(inputGate, i, partitionId,
					networkEnvironment.getResultPartitionManager(),
					networkEnvironment.getTaskEventDispatcher(),
					networkEnvironment.getConnectionManager(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numUnknownChannels++;
			}
			else {
				throw new IllegalStateException("Unexpected partition location.");
			}

			inputGate.setInputChannel(partitionId.getPartitionId(), inputChannels[i]);
		}

		LOG.debug("{}: Created {} input channels (local: {}, remote: {}, unknown: {}).",
			owningTaskName,
			inputChannels.length,
			numLocalChannels,
			numRemoteChannels,
			numUnknownChannels);

		return inputGate;
	}
}
