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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import java.io.IOException;

/**
 * An input channel consumes a single {@link ResultSubpartitionView}.
 * <p>
 * For each channel, the consumption life cycle is as follows:
 * <ol>
 * <li>{@link #requestSubpartition(int)}</li>
 * <li>{@link #getNextBuffer()}</li>
 * <li>{@link #releaseAllResources()}</li>
 * </ol>
 */
public abstract class InputChannel {

	protected final int channelIndex;

	protected final ResultPartitionID partitionId;

	protected final SingleInputGate inputGate;

	protected InputChannel(SingleInputGate inputGate, int channelIndex, ResultPartitionID partitionId) {
		this.inputGate = inputGate;
		this.channelIndex = channelIndex;
		this.partitionId = partitionId;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	int getChannelIndex() {
		return channelIndex;
	}

	/**
	 * Notifies the owning {@link SingleInputGate} about an available {@link Buffer} instance.
	 */
	protected void notifyAvailableBuffer() {
		inputGate.onAvailableBuffer(this);
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
	abstract void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException;

	/**
	 * Returns the next buffer from the consumed subpartition.
	 */
	abstract Buffer getNextBuffer() throws IOException, InterruptedException;

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	/**
	 * Sends a {@link TaskEvent} back to the task producing the consumed result partition.
	 * <p>
	 * <strong>Important</strong>: The producing task has to be running to receive backwards events.
	 * This means that the result type needs to be pipelined and the task logic has to ensure that
	 * the producer will wait for all backwards events. Otherwise, this will lead to an Exception
	 * at runtime.
	 */
	abstract void sendTaskEvent(TaskEvent event) throws IOException;

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	abstract boolean isReleased();

	abstract void notifySubpartitionConsumed() throws IOException;

	/**
	 * Releases all resources of the channel.
	 */
	abstract void releaseAllResources() throws IOException;

}
