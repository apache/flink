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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Shuffle descriptor for a specific partition which involves in all necessary information used
 * for generating {@link ShuffleDeploymentDescriptor}, {@link ResultPartitionDeploymentDescriptor},
 * {@link InputGateDeploymentDescriptor} and {@link InputChannelDeploymentDescriptor}.
 */
public class PartitionShuffleDescriptor implements Serializable {

	private static final long serialVersionUID = 6343547936086963705L;

	/** The resource ID to identify the container where the producer execution is deployed. */
	private final ResourceID producerResourceId;

	/** The connection to use to request the remote partition. */
	private final ConnectionID connectionId;

	/** The ID of the producer execution attempt. */
	private final ExecutionAttemptID producerExeccutionId;

	/** The ID of the result this partition belongs to. */
	private final IntermediateDataSetID resultId;

	/** The ID of the partition. */
	private final IntermediateResultPartitionID partitionId;

	/** The type of the partition. */
	private final ResultPartitionType partitionType;

	/** The number of subpartitions. */
	private final int numberOfSubpartitions;

	/** The maximum parallelism. */
	private final int maxParallelism;

	public PartitionShuffleDescriptor(
			ResourceID resourceId,
			ConnectionID connectionId,
			ExecutionAttemptID executionId,
			IntermediateDataSetID resultId,
			IntermediateResultPartitionID partitionId,
			ResultPartitionType partitionType,
			int numberOfSubpartitions,
			int maxParallelism) {

		this.producerResourceId = checkNotNull(resourceId);
		this.connectionId = checkNotNull(connectionId);
		this.producerExeccutionId = checkNotNull(executionId);
		this.resultId = checkNotNull(resultId);
		this.partitionId = checkNotNull(partitionId);
		this.partitionType = checkNotNull(partitionType);

		checkArgument(numberOfSubpartitions >= 1);
		this.numberOfSubpartitions = numberOfSubpartitions;
		KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
		this.maxParallelism = maxParallelism;
	}

	public ResourceID getProducerResourceId() {
		return producerResourceId;
	}

	public ConnectionID getConnectionId() {
		return connectionId;
	}

	public ExecutionAttemptID getProducerExecutionId() {
		return producerExeccutionId;
	}

	public IntermediateDataSetID getResultId() {
		return resultId;
	}

	public IntermediateResultPartitionID getPartitionId() {
		return partitionId;
	}

	public ResultPartitionType getPartitionType() {
		return partitionType;
	}

	public int getNumberOfSubpartitions() {
		return numberOfSubpartitions;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	@Override
	public String toString() {
		return String.format("PartitionShuffleDescriptor [result id: %s, "
				+ "partition id: %s, partition type: %s]",
			resultId, partitionId, partitionType);
	}

	// ------------------------------------------------------------------------

	public static PartitionShuffleDescriptor from(
			LogicalSlot slot,
			ExecutionAttemptID producerId,
			IntermediateResultPartition partition,
			int maxParallelism) {

		// If no consumers are known at this point, we use a single subpartition, otherwise we have
		// one for each consuming sub task.
		int numberOfSubpartitions = 1;
		if (!partition.getConsumers().isEmpty() && !partition.getConsumers().get(0).isEmpty()) {
			if (partition.getConsumers().size() > 1) {
				throw new IllegalStateException("Currently, only a single consumer group per partition is supported.");
			}
			numberOfSubpartitions = partition.getConsumers().get(0).size();
		}

		return new PartitionShuffleDescriptor(
			slot.getTaskManagerLocation().getResourceID(),
			new ConnectionID(slot.getTaskManagerLocation(), partition.getIntermediateResult().getConnectionIndex()),
			producerId,
			partition.getIntermediateResult().getId(),
			partition.getPartitionId(),
			partition.getIntermediateResult().getResultType(),
			numberOfSubpartitions,
			maxParallelism);
	}
}
