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
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import scala.Tuple2;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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

	// - Asynchronous error notification --------------------------------------

	private final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();

	// - Partition request backoff --------------------------------------------

	/** The initial backoff (in ms). */
	private final int initialBackoff;

	/** The maximum backoff (in ms). */
	private final int maxBackoff;

	/** The current backoff (in ms) */
	private int currentBackoff;

	protected InputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			Tuple2<Integer, Integer> initialAndMaxBackoff) {

		checkArgument(channelIndex >= 0);

		int initial = initialAndMaxBackoff._1();
		int max = initialAndMaxBackoff._2();

		checkArgument(initial >= 0 && initial <= max);

		this.inputGate = checkNotNull(inputGate);
		this.channelIndex = channelIndex;
		this.partitionId = checkNotNull(partitionId);

		this.initialBackoff = initial;
		this.maxBackoff = max;
		this.currentBackoff = initial == 0 ? -1 : 0;
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

	// ------------------------------------------------------------------------
	// Error notification
	// ------------------------------------------------------------------------

	/**
	 * Checks for an error and rethrows it if one was reported.
	 */
	protected void checkError() throws IOException {
		final Throwable t = cause.get();

		if (t != null) {
			if (t instanceof CancelTaskException) {
				throw (CancelTaskException) t;
			}
			if (t instanceof IOException) {
				throw (IOException) t;
			}
			else {
				throw new IOException(t);
			}
		}
	}

	/**
	 * Atomically sets an error for this channel and notifies the input gate about available data to
	 * trigger querying this channel by the task thread.
	 */
	protected void setError(Throwable cause) {
		if (this.cause.compareAndSet(null, checkNotNull(cause))) {
			// Notify the input gate.
			notifyAvailableBuffer();
		}
	}

	// ------------------------------------------------------------------------
	// Partition request exponential backoff
	// ------------------------------------------------------------------------

	/**
	 * Returns the current backoff in ms.
	 */
	protected int getCurrentBackoff() {
		return currentBackoff <= 0 ? 0 : currentBackoff;
	}

	/**
	 * Increases the current backoff and returns whether the operation was successful.
	 *
	 * @return <code>true</code>, iff the operation was successful. Otherwise, <code>false</code>.
	 */
	protected boolean increaseBackoff() {
		// Backoff is disabled
		if (currentBackoff < 0) {
			return false;
		}

		// This is the first time backing off
		if (currentBackoff == 0) {
			currentBackoff = initialBackoff;

			return true;
		}

		// Continue backing off
		else if (currentBackoff < maxBackoff) {
			currentBackoff = Math.min(currentBackoff * 2, maxBackoff);

			return true;
		}

		// Reached maximum backoff
		return false;
	}
}
