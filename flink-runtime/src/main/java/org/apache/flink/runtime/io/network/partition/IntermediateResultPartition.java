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

import com.google.common.base.Optional;
import org.apache.flink.runtime.deployment.GateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.IntermediateResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.queue.ConsumableOnceInMemoryOnlyQueue;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobID;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class IntermediateResultPartition {

	private final JobID jobId;

	private final BufferPool bufferPool;

	private final IntermediateResultPartitionID partitionId;

	private IntermediateResultPartitionQueue[] queues;

	private boolean isFinished;

	public IntermediateResultPartition(JobID jobId, BufferPool bufferPool, IntermediateResultPartitionDeploymentDescriptor desc) {
		this.jobId = checkNotNull(jobId);
		this.bufferPool = checkNotNull(bufferPool);

		this.partitionId = checkNotNull(desc.getPartitionId());

		this.queues = new IntermediateResultPartitionQueue[desc.getNumQueues()];

		for (int i = 0; i < queues.length; i++) {
			// TODO This needs to be refactored to allow configuration
			// depending on the type of required intermediate result partition
			// in the deployment descriptor.
			queues[i] = new ConsumableOnceInMemoryOnlyQueue();
		}
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

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
	// Data processing
	// ------------------------------------------------------------------------

	public void add(BufferOrEvent bufferOrEvent, int targetQueue) throws IOException {
		queues[targetQueue].add(bufferOrEvent);
	}

	public void addToAllPartitions(BufferOrEvent bufferOrEvent) throws IOException {
		for (IntermediateResultPartitionQueue queue : queues) {
			queue.add(bufferOrEvent);
		}
	}

	public IntermediateResultPartitionQueueIterator getQueueIterator(int queueIndex, Optional<BufferProvider> bufferProvider) {
		return queues[queueIndex].getQueueIterator(bufferProvider);
	}

	public void finish() throws IOException {
		if (!isFinished) {
			try {
				for (IntermediateResultPartitionQueue queue : queues) {
					queue.finish();
				}
			}
			finally {
				isFinished = true;
			}
		}
	}

	public void discard() {
		// buffer pool destroy etc.
	}

	@Override
	public String toString() {
		return "Intermediate result partition [num queues: " + queues.length + ", " + (isFinished ? "finished" : "not finished") + "]";
	}
}
