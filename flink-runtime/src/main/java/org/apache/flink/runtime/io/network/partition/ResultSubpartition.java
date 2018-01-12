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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * A single subpartition of a {@link ResultPartition} instance.
 */
public abstract class ResultSubpartition {

	/** The index of the subpartition at the parent partition. */
	protected final int index;

	/** The parent partition this subpartition belongs to. */
	protected final ResultPartition parent;

	// - Statistics ----------------------------------------------------------

	/** The total number of buffers (both data and event buffers) */
	private long totalNumberOfBuffers;

	/** The total number of bytes (both data and event buffers) */
	private long totalNumberOfBytes;

	public ResultSubpartition(int index, ResultPartition parent) {
		this.index = index;
		this.parent = parent;
	}

	protected void updateStatistics(Buffer buffer) {
		totalNumberOfBuffers++;
		totalNumberOfBytes += buffer.getSize();
	}

	protected long getTotalNumberOfBuffers() {
		return totalNumberOfBuffers;
	}

	protected long getTotalNumberOfBytes() {
		return totalNumberOfBytes;
	}

	/**
	 * Notifies the parent partition about a consumed {@link ResultSubpartitionView}.
	 */
	protected void onConsumedSubpartition() {
		parent.onConsumedSubpartition(index);
	}

	protected Throwable getFailureCause() {
		return parent.getFailureCause();
	}

	/**
	 * Adds the given buffer.
	 *
	 * <p>The request may be executed synchronously, or asynchronously, depending on the
	 * implementation.
	 *
	 * @param buffer
	 * 		the buffer to add (transferring ownership to this writer)
	 *
	 * @throws IOException
	 * 		thrown in case of errors while adding the buffer
	 */
	abstract public boolean add(Buffer buffer) throws IOException;

	abstract public void finish() throws IOException;

	abstract public void release() throws IOException;

	abstract public ResultSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException;

	abstract int releaseMemory() throws IOException;

	abstract public boolean isReleased();

	/**
	 * Makes a best effort to get the current size of the queue.
	 * This method must not acquire locks or interfere with the task and network threads in
	 * any way.
	 */
	abstract public int unsynchronizedGetNumberOfQueuedBuffers();

}
