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

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.queue.IntermediateResultPartitionQueue;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;

/**
 * An input channel is the consumer of a single queue of an {@link IntermediateResultPartitionQueue}.
 * <p>
 * For each channel, the consumption life cycle is as follows:
 * <ol>
 * <li>{@link #requestIntermediateResultPartition(int)}</li>
 * <li>{@link #getNextBuffer()} until {@link #isReleased()}</li>
 * <li>{@link #releaseAllResources()}</li>
 * </ol>
 */
public abstract class InputChannel {

	protected final int channelIndex;

	protected final ExecutionAttemptID producerExecutionId;

	protected final IntermediateResultPartitionID partitionId;

	protected final BufferReader reader;

	protected InputChannel(int channelIndex, ExecutionAttemptID producerExecutionId, IntermediateResultPartitionID partitionId, BufferReader reader) {
		this.channelIndex = channelIndex;
		this.producerExecutionId = producerExecutionId;
		this.partitionId = partitionId;
		this.reader = reader;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	public int getChannelIndex() {
		return channelIndex;
	}

	public ExecutionAttemptID getProducerExecutionId() {
		return producerExecutionId;
	}

	public IntermediateResultPartitionID getPartitionId() {
		return partitionId;
	}

	@Override
	public String toString() {
		return String.format("[%s:%s]", producerExecutionId, partitionId);
	}

	/**
	 * Notifies the {@link BufferReader}, which consumes this input channel
	 * about an available {@link Buffer} instance.
	 */
	protected void notifyReaderAboutAvailableBuffer() {
		reader.onAvailableInputChannel(this);
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	/**
	 * Requests the queue with the specified index of the source intermediate
	 * result partition.
	 * <p>
	 * The queue index to request depends on which sub task the channel belongs
	 * to and is specified by the consumer of this channel.
	 */
	public abstract void requestIntermediateResultPartition(int queueIndex) throws IOException;

	/**
	 * Returns the next buffer from the consumed queue.
	 */
	public abstract Buffer getNextBuffer() throws IOException;

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	/**
	 * Sends a {@link TaskEvent} back to the partition producer.
	 * <p>
	 * <strong>Important</strong>: This only works if the producer task is
	 * running at the same time.
	 */
	public abstract void sendTaskEvent(TaskEvent event) throws IOException;

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	public abstract boolean isReleased();

	/**
	 * Releases all resources of the channel.
	 */
	public abstract void releaseAllResources() throws IOException;

}
