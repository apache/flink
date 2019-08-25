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

import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A result partition for data produced by a single task.
 *
 * <p>This class is the runtime part of a logical {@link IntermediateResultPartition}. Essentially,
 * a result partition is a collection of {@link Buffer} instances. The buffers are organized in one
 * or more {@link ResultSubpartition} instances, which further partition the data depending on the
 * number of consuming tasks and the data {@link DistributionPattern}.
 *
 * <p>Tasks, which consume a result partition have to request one of its subpartitions. The request
 * happens either remotely (see {@link RemoteInputChannel}) or locally (see {@link LocalInputChannel})
 *
 * <h2>Life-cycle</h2>
 *
 * <p>The life-cycle of each result partition has three (possibly overlapping) phases:
 * <ol>
 * <li><strong>Produce</strong>: </li>
 * <li><strong>Consume</strong>: </li>
 * <li><strong>Release</strong>: </li>
 * </ol>
 *
 * <h2>Buffer management</h2>
 *
 * <h2>State management</h2>
 */
public class ResultPartition implements ResultPartitionWriter, BufferPoolOwner {

	protected static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);

	private final String owningTaskName;

	protected final ResultPartitionID partitionId;

	/** Type of this partition. Defines the concrete subpartition implementation to use. */
	protected final ResultPartitionType partitionType;

	/** The subpartitions of this partition. At least one. */
	protected final ResultSubpartition[] subpartitions;

	protected final ResultPartitionManager partitionManager;

	public final int numTargetKeyGroups;

	// - Runtime state --------------------------------------------------------

	private final AtomicBoolean isReleased = new AtomicBoolean();

	private BufferPool bufferPool;

	private boolean isFinished;

	private volatile Throwable cause;

	private final FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory;

	public ResultPartition(
		String owningTaskName,
		ResultPartitionID partitionId,
		ResultPartitionType partitionType,
		ResultSubpartition[] subpartitions,
		int numTargetKeyGroups,
		ResultPartitionManager partitionManager,
		FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {

		this.owningTaskName = checkNotNull(owningTaskName);
		this.partitionId = checkNotNull(partitionId);
		this.partitionType = checkNotNull(partitionType);
		this.subpartitions = checkNotNull(subpartitions);
		this.numTargetKeyGroups = numTargetKeyGroups;
		this.partitionManager = checkNotNull(partitionManager);
		this.bufferPoolFactory = bufferPoolFactory;
	}

	/**
	 * Registers a buffer pool with this result partition.
	 *
	 * <p>There is one pool for each result partition, which is shared by all its sub partitions.
	 *
	 * <p>The pool is registered with the partition *after* it as been constructed in order to conform
	 * to the life-cycle of task registrations in the {@link TaskExecutor}.
	 */
	@Override
	public void setup() throws IOException {
		checkState(this.bufferPool == null, "Bug in result partition setup logic: Already registered buffer pool.");

		BufferPool bufferPool = checkNotNull(bufferPoolFactory.apply(this));
		checkArgument(bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
			"Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for this result partition.");

		this.bufferPool = bufferPool;
		partitionManager.registerResultPartition(this);
	}

	public String getOwningTaskName() {
		return owningTaskName;
	}

	public ResultPartitionID getPartitionId() {
		return partitionId;
	}

	@Override
	public int getNumberOfSubpartitions() {
		return subpartitions.length;
	}

	public BufferPool getBufferPool() {
		return bufferPool;
	}

	public int getNumberOfQueuedBuffers() {
		int totalBuffers = 0;

		for (ResultSubpartition subpartition : subpartitions) {
			totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
		}

		return totalBuffers;
	}

	/**
	 * Returns the type of this result partition.
	 *
	 * @return result partition type
	 */
	public ResultPartitionType getPartitionType() {
		return partitionType;
	}

	// ------------------------------------------------------------------------

	@Override
	public BufferBuilder getBufferBuilder() throws IOException, InterruptedException {
		checkInProduceState();

		return bufferPool.requestBufferBuilderBlocking();
	}

	@Override
	public boolean addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex) throws IOException {
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
	public void flush(int subpartitionIndex) {
		subpartitions[subpartitionIndex].flush();
	}

	/**
	 * Finishes the result partition.
	 *
	 * <p>After this operation, it is not possible to add further data to the result partition.
	 *
	 * <p>For BLOCKING results, this will trigger the deployment of consuming tasks.
	 */
	@Override
	public void finish() throws IOException {
		checkInProduceState();

		for (ResultSubpartition subpartition : subpartitions) {
			subpartition.finish();
		}

		isFinished = true;
	}

	public void release() {
		release(null);
	}

	/**
	 * Releases the result partition.
	 */
	public void release(Throwable cause) {
		if (isReleased.compareAndSet(false, true)) {
			LOG.debug("{}: Releasing {}.", owningTaskName, this);

			// Set the error cause
			if (cause != null) {
				this.cause = cause;
			}

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
	}

	@Override
	public void close() {
		if (bufferPool != null) {
			bufferPool.lazyDestroy();
		}
	}

	@Override
	public void fail(@Nullable Throwable throwable) {
		partitionManager.releasePartition(partitionId, throwable);
	}

	/**
	 * Returns the requested subpartition.
	 */
	public ResultSubpartitionView createSubpartitionView(int index, BufferAvailabilityListener availabilityListener) throws IOException {
		checkElementIndex(index, subpartitions.length, "Subpartition not found.");
		checkState(!isReleased.get(), "Partition released.");

		ResultSubpartitionView readView = subpartitions[index].createReadView(availabilityListener);

		LOG.debug("Created {}", readView);

		return readView;
	}

	public Throwable getFailureCause() {
		return cause;
	}

	@Override
	public int getNumTargetKeyGroups() {
		return numTargetKeyGroups;
	}

	/**
	 * Releases buffers held by this result partition.
	 *
	 * <p>This is a callback from the buffer pool, which is registered for result partitions, which
	 * are back pressure-free.
	 */
	@Override
	public void releaseMemory(int toRelease) throws IOException {
		checkArgument(toRelease > 0);

		for (ResultSubpartition subpartition : subpartitions) {
			toRelease -= subpartition.releaseMemory();

			// Only release as much memory as needed
			if (toRelease <= 0) {
				break;
			}
		}
	}

	/**
	 * Whether this partition is released.
	 *
	 * <p>A partition is released when each subpartition is either consumed and communication is closed by consumer
	 * or failed. A partition is also released if task is cancelled.
	 */
	public boolean isReleased() {
		return isReleased.get();
	}

	@Override
	public String toString() {
		return "ResultPartition " + partitionId.toString() + " [" + partitionType + ", "
				+ subpartitions.length + " subpartitions]";
	}

	// ------------------------------------------------------------------------

	/**
	 * Notification when a subpartition is released.
	 */
	void onConsumedSubpartition(int subpartitionIndex) {

		if (isReleased.get()) {
			return;
		}

		LOG.debug("{}: Received release notification for subpartition {}.",
				this, subpartitionIndex);
	}

	public ResultSubpartition[] getAllPartitions() {
		return subpartitions;
	}

	// ------------------------------------------------------------------------

	private void checkInProduceState() throws IllegalStateException {
		checkState(!isFinished, "Partition already finished.");
	}
}
