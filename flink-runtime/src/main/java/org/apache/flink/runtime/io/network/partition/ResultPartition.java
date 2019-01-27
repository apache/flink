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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A result partition for data produced by a single task.
 *
 * <p> This class is the runtime part of a logical {@link IntermediateResultPartition}.
 *
 * <p> Tasks, which consume a result partition have to request one of its subpartitions. The request
 * happens either remotely (see {@link RemoteInputChannel}) or locally (see {@link LocalInputChannel})
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
 */
public abstract class ResultPartition<T> implements ResultPartitionWriter<T> {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);

	private final String owningTaskName;

	protected final JobID jobId;

	protected final ResultPartitionID partitionId;

	/** Type of this partition. Defines the concrete subpartition implementation to use. */
	protected final ResultPartitionType partitionType;

	protected final int numberOfSubpartitions;

	public final int numTargetKeyGroups;

	protected Counter numBytesOut = new SimpleCounter();

	protected Counter numBuffersOut = new SimpleCounter();

	protected TypeSerializer typeSerializer;

	protected SerializationDelegate serializationDelegate;

	protected AbstractInvokable parentTask;

	// - Runtime state --------------------------------------------------------

	protected final AtomicBoolean isReleased = new AtomicBoolean();

	protected boolean isFinished;

	private volatile Throwable cause;

	public ResultPartition(
		String owningTaskName,
		JobID jobId,
		ResultPartitionID partitionId,
		ResultPartitionType partitionType,
		int numberOfSubpartitions,
		int numTargetKeyGroups) {

		this.owningTaskName = checkNotNull(owningTaskName);
		this.jobId = checkNotNull(jobId);
		this.partitionId = checkNotNull(partitionId);
		this.partitionType = checkNotNull(partitionType);
		this.numberOfSubpartitions = numberOfSubpartitions;
		this.numTargetKeyGroups = numTargetKeyGroups;
	}

	public JobID getJobId() {
		return jobId;
	}

	public ResultPartitionType getPartitionType() {
		return partitionType;
	}

	protected void checkInProduceState() {
		checkState(!isFinished, "Partition already finished.");
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

			releaseInternal();
		}
	}

	public Throwable getFailureCause() {
		return cause;
	}

	protected abstract void releaseInternal();

	/**
	 * Finishes the result partition.
	 *
	 * <p> After this operation, it is not possible to add further data to the result partition.
	 *
	 * <p> For BLOCKING results, this will trigger the deployment of consuming tasks.
	 */
	public abstract void finish() throws IOException;

	@Override
	public void setTypeSerializer(TypeSerializer typeSerializer) {
		this.typeSerializer = Preconditions.checkNotNull(typeSerializer);
	}

	@Override
	public void setParentTask(AbstractInvokable parentTask) {
		this.parentTask = parentTask;
	}

	@Override
	public ResultPartitionID getPartitionId() {
		return partitionId;
	}

	@Override
	public int getNumberOfSubpartitions() {
		return numberOfSubpartitions;
	}

	@Override
	public int getNumTargetKeyGroups() {
		return numTargetKeyGroups;
	}

	@Override
	public void setMetricGroup(TaskIOMetricGroup metrics, boolean enableTracingMetrics, int tracingMetricsInterval) {
		numBytesOut = metrics.getNumBytesOutCounter();
		numBuffersOut = metrics.getNumBuffersOutCounter();
	}

	@VisibleForTesting
	public Counter getNumBytesOut() {
		return numBytesOut;
	}

	@VisibleForTesting
	public Counter getNumBuffersOut() {
		return numBuffersOut;
	}
}
