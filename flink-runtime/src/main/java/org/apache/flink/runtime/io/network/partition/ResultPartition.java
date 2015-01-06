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

import akka.actor.ActorRef;
import com.google.common.base.Optional;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.ResultPartitionID;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A result partition for data produced by a single task. There is a single producing task for each
 * result partition, but each producing task can produce more than one partition.
 * <p>
 * Essentially, a result partition is a collection of {@link Buffer}. The buffers are organized in
 * one or more partitions, which allow to further partition the data depending on the number of
 * consuming task.
 * <p>
 * These partitions have different characteristics depending on the {@link ResultPartitionType}.
 * Tasks, which consume a result partition have to request one of its partitions. The request
 * happens either remotely (see {@link RemoteInputChannel} and {@link PartitionRequestClient}) or
 * locally (see {@link LocalInputChannel} and {@link ResultPartitionManager}).
 *
 * <h2>Life-cycle</h2>
 *
 * The life-cycle of each result partition has three (possibly overlapping) phases:
 * <ol>
 * <li><strong>Produce</strong>: </li>
 * <li><strong>Consume</strong>: </li>
 * <li><strong>Release</strong>: </li>
 * </ol>
 *
 * <h2>Lazy deployment and updates of consuming tasks</h2>
 *
 * Before a consuming task can request the result, it has to be deployed. The time of deployment
 * depends on the PIPELINED vs. BLOCKING characteristic of the result partition. With pipelined
 * results, receivers are deployed as soon as the first buffer is added to the result partition.
 * With blocking results on the other hand, receivers are deployed after the partition is finished.
 *
 * <h2>Buffer management</h2>
 *
 * <h2>State management</h2>
 */
public class ResultPartition implements BufferPoolOwner {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);

	/**
	 * Partition index corresponding to the index of the partition in
	 * {@link ExecutionVertex#resultPartitions}.
	 *
	 * TODO Use different identifier as the index is error-prone.
	 */
	private final int index;

	private final JobID jobId;

	/** Execution ID of the task producing this partition */
	private final ExecutionAttemptID executionId;

	/** ID of this partition */
	private final ResultPartitionID partitionId;

	/** Type of this partition. Defines the concrete subpartition implementation to use. */
	private final ResultPartitionType partitionType;

	/** The partitions of this partition. At least one. */
	private final ResultSubpartition[] partitions;

	/** The partition manager */
	private final ResultPartitionManager partitionManager;

	private final ActorRef jobManager;

	private final FiniteDuration jobManagerTimeout;

	// - Runtime state --------------------------------------------------------

	private BufferPool bufferPool;

	private volatile boolean notifiedConsumersFlag;

	private boolean finishedFlag;

	/**
	 * The total number of references to partitions of this result. The result partition can be
	 * safely released, iff the reference count is zero. A reference count of -1 denotes that the
	 * result partition has been released.
	 */
	private int referenceCount;

	// - Statistics ----------------------------------------------------------

	/** The total number of buffers (both data and event buffers) */
	private int totalNumberOfBuffers;

	/** The total number of bytes (both data and event buffers) */
	private long totalNumberOfBytes;

	public ResultPartition(int index, JobID jobId, ExecutionAttemptID executionId, ResultPartitionID partitionId, ResultPartitionType partitionType, int numberOfSubpartitions, NetworkEnvironment networkEnvironment, IOManager ioManager) {
		checkArgument(index >= 0);
		this.index = index;
		this.jobId = checkNotNull(jobId);
		this.executionId = checkNotNull(executionId);
		this.partitionId = checkNotNull(partitionId);
		this.partitionType = checkNotNull(partitionType);
		this.partitions = new ResultSubpartition[numberOfSubpartitions];
		this.partitionManager = checkNotNull(networkEnvironment.getPartitionManager());
		this.jobManager = checkNotNull(networkEnvironment.getJobManager());
		this.jobManagerTimeout = checkNotNull(networkEnvironment.getJobManagerTimeout());

		// Create the subpartitions. Initially, the partition needs to be consumed at least once
		// before it can be released.
		synchronized (partitions) {
			switch (partitionType) {
				case BLOCKING:
					for (int i = 0; i < partitions.length; i++) {
						partitions[i] = new SpillableSubpartition(i, this, ioManager);
					}

					break;

				case PIPELINED:
					for (int i = 0; i < partitions.length; i++) {
						partitions[i] = new PipelinedSubpartition(i, this);
					}

					break;

				default:
					throw new IllegalArgumentException("Unsupported result partition type.");
			}

			// Initially set the reference count to ensure that the partition has to be consumed
			// once before it can be released.
			this.referenceCount = partitions.length;
		}
	}

	/**
	 * Registers a buffer pool with this result partition.
	 * <p>
	 * There is one pool for each result partition, which is shared by all its sub partitions.
	 * <p>
	 * The pool is registered with the partition *after* it as been constructed in order to conform
	 * to the life-cycle of task registrations in the {@link TaskManager}.
	 */
	public void registerBufferPool(BufferPool bufferPool) {
		checkArgument(bufferPool.getNumberOfRequiredMemorySegments() == getNumberOfSubpartitions(), "Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for this result partition.");
		checkState(this.bufferPool == null, "Bug in result partition setup logic: Already registered buffer pool.");

		this.bufferPool = checkNotNull(bufferPool);

		// If the partition type has no back pressure, we register with the buffer pool for
		// callbacks to release memory.
		if (!partitionType.hasBackPressure()) {
			bufferPool.setBufferPoolOwner(this);
		}
	}

	public JobID getJobId() {
		return jobId;
	}

	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}

	public ResultPartitionID getPartitionId() {
		return partitionId;
	}

	public int getNumberOfSubpartitions() {
		return partitions.length;
	}

	public BufferProvider getBufferProvider() {
		return bufferPool;
	}

	public int getTotalNumberOfBuffers() {
		return totalNumberOfBuffers;
	}

	public long getTotalNumberOfBytes() {
		return totalNumberOfBytes;
	}

	// ------------------------------------------------------------------------

	/**
	 * Adds a buffer to the subpartition with the given index.
	 * <p>
	 * For PIPELINED results, this will trigger the deployment of consuming tasks after the first
	 * buffer has been added.
	 */
	public void add(Buffer buffer, int subpartitionIndex) throws IOException {
		boolean success = false;

		try {
			synchronized (partitions) {
				checkInProduceState();

				partitions[subpartitionIndex].add(buffer);

				// Update statistics
				totalNumberOfBuffers++;
				totalNumberOfBytes += buffer.getSize();

				success = true;
			}
		}
		finally {
			if (success) {
				if (partitionType.isPipelined()) {
					notifyConsumersOnce();
				}
			}
			else {
				buffer.recycle();
			}
		}
	}

	/**
	 * Finishes the result partition.
	 * <p>
	 * After this operation, it is not possible to add further data to the result partition.
	 * <p>
	 * For BLOCKING results, this will trigger the deployment of consuming tasks.
	 */
	public void finish() throws IOException {
		boolean success = false;

		try {
			synchronized (partitions) {
				checkInProduceState();

				for (ResultSubpartition partition : partitions) {
					partition.finish();
				}

				success = true;
			}
		}
		finally {
			if (success) {
				finishedFlag = true;

				notifyConsumersOnce();
			}
		}
	}

	/**
	 * Returns the requested subpartition.
	 */
	public ResultSubpartitionView getSubpartition(int index, Optional<BufferProvider> bufferProvider) throws IOException {
		synchronized (partitions) {
			checkState(!hasBeenReleased(), "Result partition already released.");
			checkState(referenceCount > 0, "Result partition needs to be pinned, before a subpartition can be consumed.");

			return partitions[index].getReadView(bufferProvider);
		}
	}

	/**
	 * Releases the result partition.
	 */
	public void release() {
		synchronized (partitions) {
			// Only release the partition if there is currently no reference to it.
			if (referenceCount == 0) {
				try {
					for (ResultSubpartition partition : partitions) {
						try {
							partition.release();
						}
						// Catch this in order to ensure that release is called on subpartitions
						catch (Throwable t) {
							LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
						}
					}
				}
				finally {
					referenceCount = -1;

					if (bufferPool != null) {
						bufferPool.lazyDestroy();
					}
				}
			}
			else if (referenceCount > 0) {
				throw new IllegalStateException("Bug in result partition release logic: pending references.");
			}
			else {
				throw new IllegalStateException("Bug in result partition release logic: already released.");
			}
		}
	}

	/**
	 * Releases buffers held by this result partition.
	 * <p>
	 * This is a callback from the buffer pool, which is registered for result partitions, which
	 * are back pressure-free.
	 */
	@Override
	public void releaseMemory(int toRelease) throws IOException {
		checkArgument(toRelease > 0);

		for (ResultSubpartition partition : partitions) {
			toRelease -= partition.releaseMemory();

			// Only release
			if (toRelease <= 0) {
				break;
			}
		}

		int numRecycledBuffers = 0;

		for (ResultSubpartition queue : partitions) {
			numRecycledBuffers += queue.releaseMemory();

			if (numRecycledBuffers >= toRelease) {
				break;
			}
		}
	}

	@Override
	public String toString() {
		return executionId + ":" + partitionId + ":" + index + "[" + partitionType + "]";
	}

	// ------------------------------------------------------------------------

	/**
	 * Pins the result partition.
	 * <p>
	 * The partition can only be released after each subpartition has been consumed once per pin
	 * operation.
	 */
	void pin() {
		synchronized (partitions) {
			referenceCount += partitions.length;
		}
	}

	/**
	 * Unpins the result partition.
	 * <p>
	 * The partition can be released after this operation (if there is no pinning in between).
	 */
	void unpin() {
		synchronized (partitions) {
			referenceCount = 0;
		}
	}

	/**
	 * Notification when a subpartition is released.
	 */
	void onConsumedSubpartition(int subpartitionIndex) {
		synchronized (partitions) {
			referenceCount--;

			if (referenceCount == 0) {
				partitionManager.onConsumedPartition(this);
			}
			else if (referenceCount < 0) {
				throw new IllegalStateException("Bug in result subpartition release logic: decremented the reference count below 0.");
			}

			if (LOG.isDebugEnabled() && referenceCount == 0) {
				LOG.debug("Reference count for result partition reached zero.");
			}
		}
	}

	boolean hasConsumer() {
		synchronized (partitions) {
			return referenceCount > 0;
		}
	}

	boolean hasBeenReleased() {
		synchronized (partitions) {
			return referenceCount < 0;
		}
	}

	// ------------------------------------------------------------------------

	//
	// IMPORTANT: Needs to be called from synchronized scope (guarded by "partitions")
	//
	private void checkInProduceState() {
		checkState(!finishedFlag, "Partition already finished.");
		checkState(referenceCount >= 0, "Partition already released.");
	}

	/**
	 * Notifies consumers of this result partition once.
	 *
	 * TODO Use Akka's asynchronous tell call. As long as this is a blocking call, watch out to NOT
	 * call this method in synchronized scope (guarded by "partitions"). Otherwise, this will
	 * lead to deadlocks.
	 */
	private void notifyConsumersOnce() throws IOException {
		if (!notifiedConsumersFlag) {
			while (true) {
				if (Thread.interrupted()) {
					return;
				}

				final JobManagerMessages.ConsumerNotificationResult result = AkkaUtils.ask(
						jobManager,
						new JobManagerMessages.ScheduleOrUpdateConsumers(jobId, executionId, index),
						jobManagerTimeout);

				if (result.success()) {
					notifiedConsumersFlag = true;
					return;
				}
				else {
					Option<Throwable> error = result.error();
					if (error.isDefined()) {
						throw new IOException(error.get().getMessage(), error.get());
					}
				}

				try {
					Thread.sleep(10);
				}
				catch (InterruptedException e) {
					throw new IOException("Unexpected interruption during consumer schedule or update.", e);
				}
			}
		}
	}
}
