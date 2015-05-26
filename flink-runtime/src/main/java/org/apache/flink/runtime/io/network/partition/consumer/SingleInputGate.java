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
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.util.event.EventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p> Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p> As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * </pre>
 *
 * <p> When deploying such a program in parallel, the intermediate result will be partitioned over its
 * producing parallel subtasks; each of these partitions is furthermore partitioned into one or more
 * subpartitions.
 *
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
 *
 * <p> In the above example, two map subtasks produce the intermediate result in parallel, resulting
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

	/** The execution attempt ID of the owning task. */
	private final ExecutionAttemptID executionId;

	/**
	 * The ID of the consumed intermediate result. Each input gate consumes partitions of the
	 * intermediate result specified by this ID. This ID also identifies the input gate at the
	 * consuming task.
	 */
	private final IntermediateDataSetID consumedResultId;

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
	private final BlockingQueue<InputChannel> inputChannelsWithData = new LinkedBlockingQueue<InputChannel>();

	private final BitSet channelsWithEndOfPartitionEvents;

	/** The partition state checker to use for failed partition requests. */
	private final PartitionStateChecker partitionStateChecker;

	/**
	 * Buffer pool for incoming buffers. Incoming data from remote channels is copied to buffers
	 * from this pool.
	 */
	private BufferPool bufferPool;

	private boolean hasReceivedAllEndOfPartitionEvents;

	/** Flag indicating whether partitions have been requested. */
	private boolean requestedPartitionsFlag;

	/** Flag indicating whether all resources have been released. */
	private volatile boolean isReleased;

	/** Registered listener to forward buffer notifications to. */
	private final List<EventListener<InputGate>> registeredListeners = new CopyOnWriteArrayList<EventListener<InputGate>>();

	private final List<TaskEvent> pendingEvents = new ArrayList<TaskEvent>();

	private int numberOfUninitializedChannels;

	/** A timer to retrigger local partition requests. Only initialized if actually needed. */
	private Timer retriggerLocalRequestTimer;

	public SingleInputGate(
			String owningTaskName,
			JobID jobId,
			ExecutionAttemptID executionId,
			IntermediateDataSetID consumedResultId,
			int consumedSubpartitionIndex,
			int numberOfInputChannels,
			PartitionStateChecker partitionStateChecker) {

		this.owningTaskName = checkNotNull(owningTaskName);
		this.jobId = checkNotNull(jobId);
		this.executionId = checkNotNull(executionId);

		this.consumedResultId = checkNotNull(consumedResultId);

		checkArgument(consumedSubpartitionIndex >= 0);
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;

		checkArgument(numberOfInputChannels > 0);
		this.numberOfInputChannels = numberOfInputChannels;

		this.inputChannels = Maps.newHashMapWithExpectedSize(numberOfInputChannels);
		this.channelsWithEndOfPartitionEvents = new BitSet(numberOfInputChannels);

		this.partitionStateChecker = checkNotNull(partitionStateChecker);
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
			if (inputChannels.put(checkNotNull(partitionId), checkNotNull(inputChannel)) == null
					&& inputChannel.getClass() == UnknownInputChannel.class) {

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

			if (current.getClass() == UnknownInputChannel.class) {

				UnknownInputChannel unknownChannel = (UnknownInputChannel) current;

				InputChannel newChannel;

				ResultPartitionLocation partitionLocation = icdd.getConsumedPartitionLocation();

				if (partitionLocation.isLocal()) {
					newChannel = unknownChannel.toLocalInputChannel();
				}
				else if (partitionLocation.isRemote()) {
					newChannel = unknownChannel.toRemoteInputChannel(partitionLocation.getConnectionId());
				}
				else {
					throw new IllegalStateException("Tried to update unknown channel with unknown channel.");
				}

				LOG.debug("Updated unknown input channel to {}.", newChannel);

				inputChannels.put(partitionId, newChannel);

				newChannel.requestSubpartition(consumedSubpartitionIndex);

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

				LOG.debug("Retriggering partition request {}:{}.", ch.partitionId, consumedSubpartitionIndex);

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
							LOG.warn("Error during release of channel resources: " + e.getMessage(), e);
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
		// Sanity check
		if (numberOfInputChannels != inputChannels.size()) {
			throw new IllegalStateException("Bug in input gate setup logic: mismatch between" +
					"number of total input channels and the currently set number of input " +
					"channels.");
		}

		if (!requestedPartitionsFlag) {
			synchronized (requestLock) {
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
	public BufferOrEvent getNextBufferOrEvent() throws IOException, InterruptedException {

		if (hasReceivedAllEndOfPartitionEvents) {
			return null;
		}

		if (isReleased) {
			throw new IllegalStateException("Already released.");
		}

		requestPartitions();

		InputChannel currentChannel = null;
		while (currentChannel == null) {
			currentChannel = inputChannelsWithData.poll(2, TimeUnit.SECONDS);
		}

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
				channelsWithEndOfPartitionEvents.set(currentChannel.getChannelIndex());

				if (channelsWithEndOfPartitionEvents.cardinality() == numberOfInputChannels) {
					hasReceivedAllEndOfPartitionEvents = true;
				}

				currentChannel.notifySubpartitionConsumed();

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

		for (EventListener<InputGate> registeredListener : registeredListeners) {
			registeredListener.onEvent(this);
		}
	}

	void triggerPartitionStateCheck(ResultPartitionID partitionId) {
		partitionStateChecker.triggerPartitionStateCheck(
				jobId,
				executionId,
				consumedResultId,
				partitionId);
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
			NetworkEnvironment networkEnvironment) {

		final IntermediateDataSetID consumedResultId = checkNotNull(igdd.getConsumedResultId());

		final int consumedSubpartitionIndex = igdd.getConsumedSubpartitionIndex();
		checkArgument(consumedSubpartitionIndex >= 0);

		final InputChannelDeploymentDescriptor[] icdd = checkNotNull(igdd.getInputChannelDeploymentDescriptors());

		final SingleInputGate inputGate = new SingleInputGate(
				owningTaskName, jobId, executionId, consumedResultId, consumedSubpartitionIndex, icdd.length, networkEnvironment.getPartitionStateChecker());

		// Create the input channels. There is one input channel for each consumed partition.
		final InputChannel[] inputChannels = new InputChannel[icdd.length];

		for (int i = 0; i < inputChannels.length; i++) {

			final ResultPartitionID partitionId = icdd[i].getConsumedPartitionId();
			final ResultPartitionLocation partitionLocation = icdd[i].getConsumedPartitionLocation();

			if (partitionLocation.isLocal()) {
				inputChannels[i] = new LocalInputChannel(inputGate, i, partitionId,
						networkEnvironment.getPartitionManager(),
						networkEnvironment.getTaskEventDispatcher(),
						networkEnvironment.getPartitionRequestInitialAndMaxBackoff());
			}
			else if (partitionLocation.isRemote()) {
				inputChannels[i] = new RemoteInputChannel(inputGate, i, partitionId,
						partitionLocation.getConnectionId(),
						networkEnvironment.getConnectionManager(),
						networkEnvironment.getPartitionRequestInitialAndMaxBackoff()
				);
			}
			else if (partitionLocation.isUnknown()) {
				inputChannels[i] = new UnknownInputChannel(inputGate, i, partitionId,
						networkEnvironment.getPartitionManager(),
						networkEnvironment.getTaskEventDispatcher(),
						networkEnvironment.getConnectionManager(),
						networkEnvironment.getPartitionRequestInitialAndMaxBackoff()
				);
			}
			else {
				throw new IllegalStateException("Unexpected partition location.");
			}

			inputGate.setInputChannel(partitionId.getPartitionId(), inputChannels[i]);
		}

		LOG.debug("Created input channels {} from {}.", Arrays.toString(inputChannels), igdd);

		return inputGate;
	}
}
