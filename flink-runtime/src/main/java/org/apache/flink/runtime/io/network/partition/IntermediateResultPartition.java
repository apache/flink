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
import akka.dispatch.OnFailure;
import akka.pattern.Patterns;
import com.google.common.base.Optional;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.deployment.PartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.RuntimeEnvironment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.queue.IllegalQueueIteratorRequestException;
import org.apache.flink.runtime.io.network.partition.queue.IntermediateResultPartitionQueue;
import org.apache.flink.runtime.io.network.partition.queue.IntermediateResultPartitionQueueIterator;
import org.apache.flink.runtime.io.network.partition.queue.PipelinedPartitionQueue;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionType;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.messages.JobManagerMessages.ScheduleOrUpdateConsumers;
import org.apache.flink.runtime.messages.TaskManagerMessages.FailTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class IntermediateResultPartition implements BufferPoolOwner {

	private static final Logger LOG = LoggerFactory.getLogger(IntermediateResultPartition.class);

	private final RuntimeEnvironment environment;

	/**
	 * Note: This index needs to correspond to the index of the partition in
	 * {@link ExecutionVertex#resultPartitions}, which might be a little
	 * fragile as the data availability notifications use it.
	 */
	private final int partitionIndex;

	private final JobID jobId;

	private final ExecutionAttemptID producerExecutionId;

	private final IntermediateResultPartitionID partitionId;

	private final IntermediateResultPartitionType partitionType;

	private final NetworkEnvironment networkEnvironment;

	private final IntermediateResultPartitionQueue[] queues;

	private volatile boolean hasNotifiedConsumers;

	private volatile boolean isReleased;

	private boolean isFinished;

	private BufferPool bufferPool;

	public IntermediateResultPartition(RuntimeEnvironment environment, int partitionIndex, JobID jobId,
			ExecutionAttemptID executionId, IntermediateResultPartitionID partitionId, IntermediateResultPartitionType partitionType,
			IntermediateResultPartitionQueue[] partitionQueues, NetworkEnvironment networkEnvironment) {
		this.environment = environment;
		this.partitionIndex = partitionIndex;
		this.jobId = jobId;
		this.producerExecutionId = executionId;
		this.partitionId = partitionId;
		this.partitionType = partitionType;
		this.networkEnvironment = networkEnvironment;
		this.queues = partitionQueues;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	public void setBufferPool(BufferPool bufferPool) {
		checkArgument(bufferPool.getNumberOfRequiredMemorySegments() == getNumberOfQueues(),
				"Buffer pool has not enough buffers for this intermediate result.");
		checkState(this.bufferPool == null, "Buffer pool has already been set for intermediate result partition.");

		this.bufferPool = checkNotNull(bufferPool);
	}

	public ExecutionAttemptID getProducerExecutionId() {
		return producerExecutionId;
	}

	public IntermediateResultPartitionID getPartitionId() {
		return partitionId;
	}

	public JobID getJobId() {
		return jobId;
	}

	public int getNumberOfQueues() {
		return queues.length;
	}

	public BufferProvider getBufferProvider() {
		return bufferPool;
	}

	public boolean isFinished() {
		return isFinished;
	}

	// ------------------------------------------------------------------------
	// Produce
	// ------------------------------------------------------------------------

	public void add(Buffer buffer, int targetQueue) throws IOException {
		synchronized (queues) {
			if (isReleased) {
				buffer.recycle();
			}
			else {
				checkInProducePhase();
				queues[targetQueue].add(buffer);
			}
		}

		maybeNotifyConsumers(partitionType.isPipelined());
	}

	public void finish() throws IOException {
		boolean success = false;

		synchronized (queues) {
			checkInProducePhase();

			try {
				if (!isReleased) {
					for (IntermediateResultPartitionQueue queue : queues) {
						queue.finish();
					}

					success = true;
				}
			}
			finally {
				isFinished = true;
			}
		}

		if (success) {
			// Notify at this point in any case either because of the end
			// of a blocking result or an empty pipelined result.
			maybeNotifyConsumers(true);

			if (!partitionType.isPersistent() && bufferPool != null) {
				// If this partition is not persistent, immediately destroy
				// the buffer pool. For persistent intermediate results, the
				// partition manager needs to release the buffer pool.
				bufferPool.destroy();
			}
		}
	}

	public void releaseAllResources() throws IOException {
		synchronized (queues) {
			if (!isReleased) {
				try {
					for (IntermediateResultPartitionQueue queue : queues) {
						try {
							queue.discard();
						}
						catch (Throwable t) {
							LOG.error("Error while discarding queue: " + t.getMessage(), t);
						}
					}

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
	// Consume
	// ------------------------------------------------------------------------

	public IntermediateResultPartitionQueueIterator getQueueIterator(int queueIndex, Optional<BufferProvider> bufferProvider) throws IOException {
		synchronized (queues) {
			if (isReleased) {
				throw new IllegalQueueIteratorRequestException("Intermediate result partition has already been released.");
			}

			if (queueIndex < 0 || queueIndex >= queues.length) {
				throw new IllegalQueueIteratorRequestException("Illegal queue index: " + queueIndex + ", allowed: 0-" + (queues.length - 1));
			}

			return queues[queueIndex].getQueueIterator(bufferProvider);
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "Intermediate result partition " + partitionId + " [num queues: " + queues.length + ", " + (isFinished ? "finished" : "not finished") + "]";
	}

	private void checkInProducePhase() {
		checkState(!isReleased, "Partition has already been discarded.");
		checkState(!isFinished, "Partition has already been finished.");
	}

	/**
	 * Maybe notifies consumers of this result partition.
	 */
	private void maybeNotifyConsumers(boolean doNotify) throws IOException {
		if (doNotify && !hasNotifiedConsumers) {
			scheduleOrUpdateConsumers();
			hasNotifiedConsumers = true;
		}
	}

	private void scheduleOrUpdateConsumers() throws IOException {
		if(!isReleased){
			ScheduleOrUpdateConsumers msg = new ScheduleOrUpdateConsumers(jobId,
					producerExecutionId, partitionIndex);

			Future<Object> futureResponse = Patterns.ask(networkEnvironment.getJobManager(), msg,
					networkEnvironment.getJobManagerTimeout());

			futureResponse.onFailure(new OnFailure(){
				@Override
				public void onFailure(Throwable failure) throws Throwable {
					LOG.error("Could not schedule or update consumers at the JobManager.", failure);

					// Fail task at the TaskManager
					FailTask failMsg = new FailTask(producerExecutionId,
							new RuntimeException("Could not schedule or update consumers at " +
									"the JobManager.", failure));

					networkEnvironment.getTaskManager().tell(failMsg, ActorRef.noSender());
				}
			}, AkkaUtils.globalExecutionContext());
		}
	}

	// ------------------------------------------------------------------------
	// Buffer pool owner methods
	// ------------------------------------------------------------------------

	/**
	 * If this partition is registered as the {@link BufferPoolOwner} of a
	 * {@link BufferPool}, it will forward the requests to the queues.
	 */
	@Override
	public void recycleBuffers(int numBuffersToRecycle) throws IOException {
		int numRecycledBuffers = 0;

		for (IntermediateResultPartitionQueue queue : queues) {
			numRecycledBuffers += queue.recycleBuffers();

			if (numRecycledBuffers >= numBuffersToRecycle) {
				break;
			}
		}
	}

	// ------------------------------------------------------------------------

	public static IntermediateResultPartition create(RuntimeEnvironment environment, int partitionIndex, JobID jobId, ExecutionAttemptID executionId, NetworkEnvironment networkEnvironment, PartitionDeploymentDescriptor desc) {
		final IntermediateResultPartitionID partitionId = checkNotNull(desc.getPartitionId());
		final IntermediateResultPartitionType partitionType = checkNotNull(desc.getPartitionType());

		final IntermediateResultPartitionQueue[] partitionQueues = new IntermediateResultPartitionQueue[desc.getNumberOfQueues()];

		// TODO The queues need to be created depending on the result type
		for (int i = 0; i < partitionQueues.length; i++) {
			partitionQueues[i] = new PipelinedPartitionQueue();
		}

		return new IntermediateResultPartition(environment, partitionIndex, jobId, executionId, partitionId, partitionType, partitionQueues, networkEnvironment);
	}
}
