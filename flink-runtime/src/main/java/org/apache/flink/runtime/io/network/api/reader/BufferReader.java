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

package org.apache.flink.runtime.io.network.api.reader;

import com.google.common.collect.Maps;
import org.apache.flink.runtime.deployment.PartitionConsumerDeploymentDescriptor;
import org.apache.flink.runtime.deployment.PartitionInfo;
import org.apache.flink.runtime.deployment.PartitionInfo.PartitionLocation;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.execution.RuntimeEnvironment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.RemoteAddress;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.UnknownInputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.runtime.util.event.EventNotificationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class BufferReader implements BufferReaderBase {

	private static final Logger LOG = LoggerFactory.getLogger(BufferReader.class);

	private final Object requestLock = new Object();

	private final RuntimeEnvironment environment;

	private final NetworkEnvironment networkEnvironment;

	private final EventNotificationHandler<TaskEvent> taskEventHandler = new EventNotificationHandler<TaskEvent>();

	private final IntermediateDataSetID consumedResultId;

	private final int totalNumberOfInputChannels;

	private final int queueToRequest;

	private final Map<IntermediateResultPartitionID, InputChannel> inputChannels;

	private BufferPool bufferPool;

	private boolean isReleased;

	private boolean isTaskEvent;

	// ------------------------------------------------------------------------

	private final BlockingQueue<InputChannel> inputChannelsWithData = new LinkedBlockingQueue<InputChannel>();

	private final AtomicReference<EventListener<BufferReaderBase>> readerListener = new AtomicReference<EventListener<BufferReaderBase>>(null);

	// ------------------------------------------------------------------------

	private boolean isIterativeReader;

	private int currentNumEndOfSuperstepEvents;

	private int channelIndexOfLastReadBuffer = -1;

	private boolean hasRequestedPartitions = false;

	public BufferReader(RuntimeEnvironment environment, NetworkEnvironment networkEnvironment, IntermediateDataSetID consumedResultId, int numberOfInputChannels, int queueToRequest) {

		this.consumedResultId = checkNotNull(consumedResultId);
		// Note: the environment is not fully initialized yet
		this.environment = checkNotNull(environment);

		this.networkEnvironment = networkEnvironment;

		checkArgument(numberOfInputChannels >= 0);
		this.totalNumberOfInputChannels = numberOfInputChannels;

		checkArgument(queueToRequest >= 0);
		this.queueToRequest = queueToRequest;

		this.inputChannels = Maps.newHashMapWithExpectedSize(numberOfInputChannels);
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	public void setBufferPool(BufferPool bufferPool) {
		checkArgument(bufferPool.getNumberOfRequiredMemorySegments() == totalNumberOfInputChannels, "Buffer pool has not enough buffers for this reader.");
		checkState(this.bufferPool == null, "Buffer pool has already been set for reader.");

		this.bufferPool = checkNotNull(bufferPool);
	}

	public IntermediateDataSetID getConsumedResultId() {
		return consumedResultId;
	}

	public String getTaskNameWithSubtasks() {
		return environment.getTaskNameWithSubtasks();
	}

	public IntermediateResultPartitionProvider getIntermediateResultPartitionProvider() {
		return networkEnvironment.getPartitionManager();
	}

	public TaskEventDispatcher getTaskEventDispatcher() {
		return networkEnvironment.getTaskEventDispatcher();
	}

	public ConnectionManager getConnectionManager() {
		return networkEnvironment.getConnectionManager();
	}

	// TODO This is a work-around for the union reader
	boolean hasInputChannelWithData() {
		return !inputChannelsWithData.isEmpty();
	}

	/**
	 * Returns the total number of input channels for this reader.
	 * <p>
	 * Note: This number might be smaller the current number of input channels
	 * of the reader as channels are possibly updated during runtime.
	 */
	public int getNumberOfInputChannels() {
		return totalNumberOfInputChannels;
	}

	public BufferProvider getBufferProvider() {
		return bufferPool;
	}

	public void setInputChannel(IntermediateResultPartitionID partitionId, InputChannel inputChannel) {
		synchronized (requestLock) {
			inputChannels.put(checkNotNull(partitionId), checkNotNull(inputChannel));
		}
	}

	public void updateInputChannel(PartitionInfo partitionInfo) throws IOException {
		synchronized (requestLock) {
			if (isReleased) {
				// There was a race with a task failure/cancel
				return;
			}

			final IntermediateResultPartitionID partitionId = partitionInfo.getPartitionId();

			InputChannel current = inputChannels.get(partitionId);

			if (current.getClass() == UnknownInputChannel.class) {
				UnknownInputChannel unknownChannel = (UnknownInputChannel) current;

				InputChannel newChannel;

				if (partitionInfo.getProducerLocation() == PartitionLocation.REMOTE) {
					newChannel = unknownChannel.toRemoteInputChannel(partitionInfo.getProducerAddress());
				}
				else if (partitionInfo.getProducerLocation() == PartitionLocation.LOCAL) {
					newChannel = unknownChannel.toLocalInputChannel();
				}
				else {
					throw new IllegalStateException("Tried to update unknown channel with unknown channel.");
				}

				inputChannels.put(partitionId, newChannel);

				newChannel.requestIntermediateResultPartition(queueToRequest);
			}
		}
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	@Override
	public void requestPartitionsOnce() throws IOException {
		if (!hasRequestedPartitions) {
			// Sanity check
			if (totalNumberOfInputChannels != inputChannels.size()) {
				throw new IllegalStateException("Mismatch between number of total input channels and the currently number of set input channels.");
			}

			synchronized (requestLock) {
				for (InputChannel inputChannel : inputChannels.values()) {
					inputChannel.requestIntermediateResultPartition(queueToRequest);
				}
			}

			hasRequestedPartitions = true;
		}
	}

	@Override
	public Buffer getNextBufferBlocking() throws IOException, InterruptedException {
		requestPartitionsOnce();

		while (true) {
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			// Possibly block until data is available at one of the input channels
			InputChannel currentChannel = null;
			while (currentChannel == null) {
				currentChannel = inputChannelsWithData.poll(2000, TimeUnit.MILLISECONDS);
			}

			isTaskEvent = false;

			final Buffer buffer = currentChannel.getNextBuffer();

			if (buffer == null) {
				throw new IllegalStateException("Bug in reader logic: queried for a buffer although none was available.");
			}

			if (buffer.isBuffer()) {
				channelIndexOfLastReadBuffer = currentChannel.getChannelIndex();
				return buffer;
			}
			else {
				try {
					final AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());

					// ------------------------------------------------------------
					// Runtime events
					// ------------------------------------------------------------
					// Note: We can not assume that every channel will be finished
					// with an according event. In failure cases or iterations the
					// consumer task finishes earlier and has to release all
					// resources.
					// ------------------------------------------------------------
					if (event.getClass() == EndOfPartitionEvent.class) {
						currentChannel.releaseAllResources();

						return null;
					}
					else if (event.getClass() == EndOfSuperstepEvent.class) {
						incrementEndOfSuperstepEventAndCheck();

						return null;
					}
					// ------------------------------------------------------------
					// Task events (user)
					// ------------------------------------------------------------
					else if (event instanceof TaskEvent) {
						taskEventHandler.publish((TaskEvent) event);

						isTaskEvent = true;

						return null;
					}
					else {
						throw new IllegalStateException("Received unexpected event " + event + " from input channel " + currentChannel + ".");
					}
				}
				catch (Throwable t) {
					throw new IOException("Error while reading event: " + t.getMessage(), t);
				}
				finally {
					buffer.recycle();
				}
			}
		}
	}

	@Override
	public Buffer getNextBuffer(Buffer exchangeBuffer) {
		throw new UnsupportedOperationException("Buffer exchange when reading data is not yet supported.");
	}

	@Override
	public int getChannelIndexOfLastBuffer() {
		return channelIndexOfLastReadBuffer;
	}

	@Override
	public boolean isTaskEvent() {
		return isTaskEvent;
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

	public void releaseAllResources() throws IOException {
		synchronized (requestLock) {
			if (!isReleased) {
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
					isReleased = true;
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	// Channel notifications
	// ------------------------------------------------------------------------

	public void onAvailableInputChannel(InputChannel inputChannel) {
		inputChannelsWithData.add(inputChannel);

		if (readerListener.get() != null) {
			readerListener.get().onEvent(this);
		}
	}

	@Override
	public void subscribeToReader(EventListener<BufferReaderBase> listener) {
		if (!this.readerListener.compareAndSet(null, listener)) {
			throw new IllegalStateException(listener + " is already registered as a record availability listener");
		}
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException, InterruptedException {
		// This can be improved by just serializing the event once for all
		// remote input channels.
		synchronized (requestLock) {
			for (InputChannel inputChannel : inputChannels.values()) {
				inputChannel.sendTaskEvent(event);
			}
		}
	}

	@Override
	public void subscribeToTaskEvent(EventListener<TaskEvent> listener, Class<? extends TaskEvent> eventType) {
		taskEventHandler.subscribe(listener, eventType);
	}

	// ------------------------------------------------------------------------
	// Iteration end of superstep events
	// ------------------------------------------------------------------------

	@Override
	public void setIterativeReader() {
		isIterativeReader = true;
	}

	@Override
	public void startNextSuperstep() {
		checkState(isIterativeReader, "Tried to start next superstep in a non-iterative reader.");
		checkState(currentNumEndOfSuperstepEvents == totalNumberOfInputChannels,
				"Tried to start next superstep before reaching end of previous superstep.");

		currentNumEndOfSuperstepEvents = 0;
	}

	@Override
	public boolean hasReachedEndOfSuperstep() {
		return currentNumEndOfSuperstepEvents == totalNumberOfInputChannels;
	}

	private boolean incrementEndOfSuperstepEventAndCheck() {
		checkState(isIterativeReader, "Received end of superstep event in a non-iterative reader.");

		currentNumEndOfSuperstepEvents++;

		checkState(currentNumEndOfSuperstepEvents <= totalNumberOfInputChannels,
				"Received too many (" + currentNumEndOfSuperstepEvents + ") end of superstep events.");

		return currentNumEndOfSuperstepEvents == totalNumberOfInputChannels;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("BufferReader %s [task: %s, current/total number of input channels: %d/%d]",
				consumedResultId, getTaskNameWithSubtasks(), inputChannels.size(), totalNumberOfInputChannels);
	}

	public static BufferReader create(RuntimeEnvironment runtimeEnvironment, NetworkEnvironment networkEnvironment, PartitionConsumerDeploymentDescriptor desc) {
		// The consumed intermediate data set (all partitions are part of this data set)
		final IntermediateDataSetID resultId = desc.getResultId();

		// The queue to request from each consumed partition
		final int queueIndex = desc.getQueueIndex();

		// There is one input channel for each consumed partition
		final PartitionInfo[] partitions = desc.getPartitions();
		final int numberOfInputChannels = partitions.length;

		final BufferReader reader = new BufferReader(runtimeEnvironment, networkEnvironment, resultId, numberOfInputChannels, queueIndex);

		// Create input channels
		final InputChannel[] inputChannels = new InputChannel[numberOfInputChannels];

		int channelIndex = 0;

		for (PartitionInfo partition : partitions) {
			final ExecutionAttemptID producerExecutionId = partition.getProducerExecutionId();
			final IntermediateResultPartitionID partitionId = partition.getPartitionId();

			final PartitionLocation producerLocation = partition.getProducerLocation();

			switch (producerLocation) {
				case LOCAL:
					inputChannels[channelIndex] = new LocalInputChannel(channelIndex, producerExecutionId, partitionId, reader);
					break;

				case REMOTE:
					final RemoteAddress producerAddress = checkNotNull(partition.getProducerAddress(), "Missing producer address for remote intermediate result partition.");

					inputChannels[channelIndex] = new RemoteInputChannel(channelIndex, producerExecutionId, partitionId, reader, producerAddress);
					break;

				case UNKNOWN:
					inputChannels[channelIndex] = new UnknownInputChannel(channelIndex, producerExecutionId, partitionId, reader);
					break;
			}

			reader.setInputChannel(partitionId, inputChannels[channelIndex]);

			channelIndex++;
		}

		return reader;
	}
}
