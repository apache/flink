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

import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deployment descriptor for a result partition.
 *
 * @see ResultPartition
 */
public class ResultPartitionDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = 6343547936086963705L;

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

	/** Flag whether the result partition should send scheduleOrUpdateConsumer messages. */
	private final boolean sendScheduleOrUpdateConsumersMessage;

	public ResultPartitionDeploymentDescriptor(
			IntermediateDataSetID resultId,
			IntermediateResultPartitionID partitionId,
			ResultPartitionType partitionType,
			int numberOfSubpartitions,
			int maxParallelism,
			boolean lazyScheduling) {

		this.resultId = checkNotNull(resultId);
		this.partitionId = checkNotNull(partitionId);
		this.partitionType = checkNotNull(partitionType);

		KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
		checkArgument(numberOfSubpartitions >= 1);
		this.numberOfSubpartitions = numberOfSubpartitions;
		this.maxParallelism = maxParallelism;
		this.sendScheduleOrUpdateConsumersMessage = lazyScheduling;
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

	public boolean sendScheduleOrUpdateConsumersMessage() {
		return sendScheduleOrUpdateConsumersMessage;
	}

	@Override
	public String toString() {
		return String.format("ResultPartitionDeploymentDescriptor [result id: %s, "
						+ "partition id: %s, partition type: %s]",
				resultId, partitionId, partitionType);
	}

	// ------------------------------------------------------------------------

	public static ResultPartitionDeploymentDescriptor from(
			IntermediateResultPartition partition, int maxParallelism, boolean lazyScheduling) {

		final IntermediateDataSetID resultId = partition.getIntermediateResult().getId();
		final IntermediateResultPartitionID partitionId = partition.getPartitionId();
		final ResultPartitionType partitionType = partition.getIntermediateResult().getResultType();

		// The produced data is partitioned among a number of subpartitions.
		//
		// If no consumers are known at this point, we use a single subpartition, otherwise we have
		// one for each consuming sub task.
		int numberOfSubpartitions = 1;

		if (!partition.getConsumers().isEmpty() && !partition.getConsumers().get(0).isEmpty()) {

			if (partition.getConsumers().size() > 1) {
				throw new IllegalStateException("Currently, only a single consumer group per partition is supported.");
			}

			numberOfSubpartitions = partition.getConsumers().get(0).size();
		}

		return new ResultPartitionDeploymentDescriptor(
				resultId, partitionId, partitionType, numberOfSubpartitions, maxParallelism, lazyScheduling);
	}
}
