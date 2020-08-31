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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link ResultPartition} which writes buffers directly to {@link ResultSubpartition}s. This
 * is in contrast to implementations where records are written to a joint structure, from which
 * the subpartitions draw the data after the write phase is finished, for example the sort-based
 * partitioning.
 *
 * <p>To avoid confusion: On the read side, all subpartitions return buffers (and backlog) to be
 * transported through the network.
 */
public class BufferWritingResultPartition extends ResultPartition {

	/** The subpartitions of this partition. At least one. */
	protected final ResultSubpartition[] subpartitions;

	public BufferWritingResultPartition(
		String owningTaskName,
		int partitionIndex,
		ResultPartitionID partitionId,
		ResultPartitionType partitionType,
		ResultSubpartition[] subpartitions,
		int numTargetKeyGroups,
		ResultPartitionManager partitionManager,
		@Nullable BufferCompressor bufferCompressor,
		FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {

		super(
			owningTaskName,
			partitionIndex,
			partitionId,
			partitionType,
			subpartitions.length,
			numTargetKeyGroups,
			partitionManager,
			bufferCompressor,
			bufferPoolFactory);

		this.subpartitions = checkNotNull(subpartitions);
	}

	public int getNumberOfQueuedBuffers() {
		int totalBuffers = 0;

		for (ResultSubpartition subpartition : subpartitions) {
			totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
		}

		return totalBuffers;
	}

	public int getNumberOfQueuedBuffers(int targetSubpartition) {
		checkArgument(targetSubpartition >= 0 && targetSubpartition < numSubpartitions);
		return subpartitions[targetSubpartition].unsynchronizedGetNumberOfQueuedBuffers();
	}

	@Override
	public BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		checkInProduceState();

		return bufferPool.requestBufferBuilderBlocking(targetChannel);
	}

	@Override
	public BufferBuilder tryGetBufferBuilder(int targetChannel) throws IOException {
		return bufferPool.requestBufferBuilder(targetChannel);
	}

	@Override
	public boolean addBufferConsumer(
			BufferConsumer bufferConsumer,
			int subpartitionIndex) throws IOException {
		checkNotNull(bufferConsumer);

		ResultSubpartition subpartition;
		try {
			checkInProduceState();
			subpartition = subpartitions[subpartitionIndex];
		}
		catch (Exception ex) {
			bufferConsumer.close();
			throw ex;
		}

		return subpartition.add(bufferConsumer);
	}

	@Override
	public void flushAll() {
		for (ResultSubpartition subpartition : subpartitions) {
			subpartition.flush();
		}
	}

	@Override
	public void flush(int targetSubpartition) {
		subpartitions[targetSubpartition].flush();
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(
			int subpartitionIndex,
			BufferAvailabilityListener availabilityListener) throws IOException {
		checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
		checkState(!isReleased(), "Partition released.");

		ResultSubpartition subpartition = subpartitions[subpartitionIndex];
		ResultSubpartitionView readView = subpartition.createReadView(availabilityListener);

		LOG.debug("Created {}", readView);

		return readView;
	}

	@Override
	public void finish() throws IOException {
		for (ResultSubpartition subpartition : subpartitions) {
			subpartition.finish();
		}
		super.finish();
	}

	@Override
	protected void releaseInternal() {
		// Release all subpartitions
		for (ResultSubpartition subpartition : subpartitions) {
			try {
				subpartition.release();
			}
			// Catch this in order to ensure that release is called on all subpartitions
			catch (Throwable t) {
				LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
			}
		}
	}

	@VisibleForTesting
	public ResultSubpartition[] getAllPartitions() {
		return subpartitions;
	}
}
