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

import com.google.common.collect.Maps;
import org.apache.flink.runtime.deployment.PartitionConsumerDeploymentDescriptor;
import org.apache.flink.runtime.deployment.PartitionInfo;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.RemoteAddress;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.util.event.EventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 * <p>
 * Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 * <p>
 * As an example, consider a map-reduce program, where the map operator produces data and the reduce
 * operator consumes the produced data.
 * <pre>
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * </pre>
 * When deploying such a program in parallel, the intermediate result will be partitioned over its
 * producing parallel subtasks; each of these partitions is furthermore partitioned into one or more
 * subpartitions.
 * <pre>
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
 * </pre>
 * In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is subpartitioned into two
 * subpartitions -- one for each parallel reduce subtask.
 * <p>
 */
public class SingleInputGate implements InputGate {

	private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

	/** Lock object to guard partition requests and runtime channel updates. */
	private final Object requestLock = new Object();

	/**
	 * The ID of the consumed intermediate result. Each input gate consumes partitions of the
	 * intermediate result specified by this ID. This ID also identifies the input gate at the
	 * consuming task.
	 */
	private final IntermediateDataSetID consumedResultId;

	/**
	 * The index of the consumed subpartition of each consumed partition. This index depends on the
	 * distribution pattern and both subtask indices of the producing and consuming task.
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
	private final BlockingQueue<InputChannel> inputChannelsWithData = new LinkedBlockingQueue<InputChannel>();

	/**
	 * Buffer pool for incoming buffers. Incoming data from remote channels is copied to buffers
	 * from this pool.
	 */
	private BufferPool bufferPool;

	/** Flag indicating whether partitions have been requested. */
	private boolean requestedPartitionsFlag;

	/** Flag indicating whether all resources have been released. */
	private boolean releasedResourcesFlag;

	/** Registered listener to forward buffer notifications to. */
	private final List<EventListener<InputGate>> registeredListeners = new CopyOnWriteArrayList<EventListener<InputGate>>();

	private final List<TaskEvent> pendingEvents = new ArrayList<TaskEvent>();

	private int numberOfUninitializedChannels;

	public SingleInputGate(IntermediateDataSetID consumedResultId, int consumedSubpartitionIndex, int numberOfInputChannels) {
		this.consumedResultId = checkNotNull(consumedResultId);

		checkArgument(consumedSubpartitionIndex >= 0);
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;

		checkArgument(numberOfInputChannels > 0);
		this.numberOfInputChannels = numberOfInputChannels;

		this.inputChannels = Maps.newHashMapWithExpectedSize(numberOfInputChannels);
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

	BufferProvider getBufferProvider() {
		return bufferPool;
	}

	// ------------------------------------------------------------------------
	// Setup/Life-cycle
	// ------------------------------------------------------------------------

	public void setBufferPool(BufferPool bufferPool) {
		// Sanity checks
		checkArgument(numberOfInputChannels == bufferPool.getNumberOfRequiredMemorySegments(),
				"Bug in input gate setup logic: buffer pool has not enough guaranteed buffers " +
						"for this input gate. Input gates require at least as many buffers as " +
						"there are input channels.");

		checkState(this.bufferPool == null, "Bug in input gate setup logic: buffer pool has" +
				"already been set for this input gate.");

		this.bufferPool = checkNotNull(bufferPool);
	}

	public void setInputChannel(IntermediateResultPartitionID partitionId, InputChannel inputChannel) {
		synchronized (requestLock) {
			if (inputChannels.put(checkNotNull(partitionId), checkNotNull(inputChannel)) == null &&
					inputChannel.getClass() == UnknownInputChannel.class) {
				numberOfUninitializedChannels++;
			}
		}
	}

	public void updateInputChannel(PartitionInfo partitionInfo) throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (releasedResourcesFlag) {
				// There was a race with a task failure/cancel
				return;
			}

			final IntermediateResultPartitionID partitionId = partitionInfo.getPartitionId();

			InputChannel current = inputChannels.get(partitionId);

			if (current.getClass() == UnknownInputChannel.class) {
				UnknownInputChannel unknownChannel = (UnknownInputChannel) current;

				InputChannel newChannel;

				if (partitionInfo.getProducerLocation() == PartitionInfo.PartitionLocation.REMOTE) {
					newChannel = unknownChannel.toRemoteInputChannel(partitionInfo.getProducerAddress());
				}
				else if (partitionInfo.getProducerLocation() == PartitionInfo.PartitionLocation.LOCAL) {
					newChannel = unknownChannel.toLocalInputChannel();
				}
				else {
					throw new IllegalStateException("Tried to update unknown channel with unknown channel.");
				}

				inputChannels.put(partitionId, newChannel);

				newChannel.requestIntermediateResultPartition(consumedSubpartitionIndex);

				for (TaskEvent event : pendingEvents) {
					newChannel.sendTaskEvent(event);
				}

				if (--numberOfUninitializedChannels == 0) {
					pendingEvents.clear();
				}
			}
		}
	}

	public void releaseAllResources() throws IOException {
		synchronized (requestLock) {
			if (!releasedResourcesFlag) {
				try {
					for (InputChannel inputChannel : inputChannels.values()) {
						try {
							inputChannel.releaseAllResources();
						}
						catch (IOException e) {
							LOG.warn("Error during release of channel resources: " + e.getMessage(), e);
						}
					}

					// The buffer pool can actually be destroyed immediately after the
					// reader received all of the data from the input channels.
					if (bufferPool != null) {
						bufferPool.destroy();
					}
				}
				finally {
					releasedResourcesFlag = true;
				}
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
		if (!requestedPartitionsFlag) {
			// Sanity check
			if (numberOfInputChannels != inputChannels.size()) {
				throw new IllegalStateException("Bug in input gate setup logic: mismatch between" +
						"number of total input channels and the currently set number of input " +
						"channels.");
			}

			synchronized (requestLock) {
				for (InputChannel inputChannel : inputChannels.values()) {
					inputChannel.requestIntermediateResultPartition(consumedSubpartitionIndex);
				}
			}

			requestedPartitionsFlag = true;
		}
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	@Override
	public BufferOrEvent getNextBufferOrEvent() throws IOException, InterruptedException {

		if (releasedResourcesFlag) {
			throw new IllegalStateException("The input has already been consumed. This indicates misuse of the input gate.");
		}

		requestPartitions();

		final InputChannel currentChannel = inputChannelsWithData.take();

		final Buffer buffer = currentChannel.getNextBuffer();

		// Sanity check that notifications only happen when data is available
		if (buffer == null) {
			throw new IllegalStateException("Bug in input gate/channel logic: input gate got" +
					"notified by channel about available data, but none was available.");
		}

		if (buffer.isBuffer()) {
			return new BufferOrEvent(buffer, currentChannel.getChannelIndex());
		}
		else {
			final AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());

			if (event.getClass() == EndOfPartitionEvent.class) {
				currentChannel.releaseAllResources();
			}

			return new BufferOrEvent(event, currentChannel.getChannelIndex());
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
	public void registerListener(EventListener<InputGate> listener) {
		registeredListeners.add(checkNotNull(listener));
	}

	public void onAvailableBuffer(InputChannel channel) {
		inputChannelsWithData.add(channel);

		for (int i = 0; i < registeredListeners.size(); i++) {
			registeredListeners.get(i).onEvent(this);
		}
	}

	// ------------------------------------------------------------------------

	public static SingleInputGate create(NetworkEnvironment networkEnvironment, PartitionConsumerDeploymentDescriptor desc) {
		// The consumed intermediate data set (all partitions are part of this data set)
		final IntermediateDataSetID resultId = desc.getResultId();
		// The queue to request from each consumed partition
		final int queueIndex = desc.getQueueIndex();
		// There is one input channel for each consumed partition
		final PartitionInfo[] partitions = desc.getPartitions();
		final int numberOfInputChannels = partitions.length;
		final SingleInputGate reader = new SingleInputGate(resultId, queueIndex, numberOfInputChannels);
		// Create input channels
		final InputChannel[] inputChannels = new InputChannel[numberOfInputChannels];
		int channelIndex = 0;
		for (PartitionInfo partition : partitions) {
			final ExecutionAttemptID producerExecutionId = partition.getProducerExecutionId();
			final IntermediateResultPartitionID partitionId = partition.getPartitionId();
			final PartitionInfo.PartitionLocation producerLocation = partition.getProducerLocation();
			switch (producerLocation) {
				case LOCAL:
					LOG.debug("Create LocalInputChannel for {}.", partition);

					inputChannels[channelIndex] = new LocalInputChannel(reader, channelIndex, producerExecutionId, partitionId,
							networkEnvironment.getPartitionManager(), networkEnvironment.getTaskEventDispatcher());

					break;
				case REMOTE:
					LOG.debug("Create RemoteInputChannel for {}.", partition);

					final RemoteAddress producerAddress = checkNotNull(partition.getProducerAddress(),
							"Missing producer address for remote intermediate result partition.");

					inputChannels[channelIndex] = new RemoteInputChannel(reader, channelIndex, producerExecutionId, partitionId,
							producerAddress, networkEnvironment.getConnectionManager());

					break;
				case UNKNOWN:
					LOG.debug("Create UnknownInputChannel for {}.", partition);

					inputChannels[channelIndex] = new UnknownInputChannel(reader, channelIndex, producerExecutionId, partitionId,
							networkEnvironment.getPartitionManager(), networkEnvironment.getTaskEventDispatcher(), networkEnvironment.getConnectionManager());

					break;
			}
			reader.setInputChannel(partitionId, inputChannels[channelIndex]);
			channelIndex++;
		}
		return reader;
	}
}
