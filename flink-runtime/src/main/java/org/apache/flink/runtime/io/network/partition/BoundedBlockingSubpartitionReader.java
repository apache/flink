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

import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.partition.BoundedData.BoundedPartitionData;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The reader (read view) of a BoundedBlockingSubpartition.
 */
final class BoundedBlockingSubpartitionReader implements ResultSubpartitionView {

	/** The result subpartition that we read. */
	private final BoundedBlockingSubpartition parent;

	/** The reader/decoder to the memory mapped region with the data we currently read from.
	 * Null once the reader empty or disposed.*/
	private final BoundedData.Reader dataReader;

	/** The remaining number of data buffers (not events) in the result. */
	private int dataBufferBacklog;

	/** The remaining number of data buffers and events in the result. */
	private int numBuffersAndEvents;

	/** Flag whether this reader is released. Atomic, to avoid double release. */
	private boolean isReleased;

	private int sequenceNumber;

	/**
	 * Convenience constructor that takes a single buffer.
	 */
	BoundedBlockingSubpartitionReader(
			BoundedBlockingSubpartition parent,
			BoundedData data,
			int numDataBuffers,
			int numBuffersAndEvents) throws IOException {

		this.parent = checkNotNull(parent);

		checkNotNull(data);
		this.dataReader = data.createReader(this);

		checkArgument(numDataBuffers >= 0);
		this.dataBufferBacklog = numDataBuffers;

		checkArgument(numBuffersAndEvents >= 0);
		this.numBuffersAndEvents = numBuffersAndEvents;
	}

	@Nullable
	@Override
	public PartitionData getNextData() throws IOException {
		if (isReleased) {
			return null;
		}

		BoundedPartitionData current = dataReader.nextData();
		if (current == null) {
			// as per contract, we must return null when the reader is empty,
			// but also in case the reader is disposed (rather than throwing an exception)
			return null;
		}
		if (current.isBuffer()) {
			dataBufferBacklog--;
		}
		numBuffersAndEvents--;

		// We can simplify assume all the data are non-events here to avoid trouble in pre-fetching the next header
		DataType nextDataType = numBuffersAndEvents > 0 ? DataType.DATA_BUFFER : DataType.NONE;

		return current.build(nextDataType, dataBufferBacklog, sequenceNumber++);
	}

	@Override
	public void notifyDataAvailable() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public void releaseAllResources() throws IOException {
		// it is not a problem if this method executes multiple times
		isReleased = true;

		IOUtils.closeQuietly(dataReader);

		// Notify the parent that this one is released. This allows the parent to
		// eventually release all resources (when all readers are done and the
		// parent is disposed).
		parent.releaseReaderReference(this);
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public void resumeConsumption() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public boolean isAvailable(int numCreditsAvailable) {
		// Since the blocking partition does not have checkpoint barrier, and it should only have
		// end of partition event ATM, then we can simplify assume all the data are non-events.
		return numCreditsAvailable > 0 && numBuffersAndEvents > 0;
	}

	@Override
	public Throwable getFailureCause() {
		// we can never throw an error after this was created
		return null;
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return parent.unsynchronizedGetNumberOfQueuedBuffers();
	}

	@Override
	public String toString() {
		return String.format("Blocking Subpartition Reader: ID=%s, index=%d",
				parent.parent.getPartitionId(),
				parent.getSubPartitionIndex());
	}
}
