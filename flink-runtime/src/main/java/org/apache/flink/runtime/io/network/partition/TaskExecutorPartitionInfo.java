/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Encapsulates meta-information the TaskExecutor requires to be kept for each partition.
 */
public final class TaskExecutorPartitionInfo {

	private final ResultPartitionID resultPartitionId;
	private final IntermediateDataSetID intermediateDataSetId;

	private final int numberOfPartitions;

	public TaskExecutorPartitionInfo(ResultPartitionID resultPartitionId, IntermediateDataSetID intermediateDataSetId, int numberOfPartitions) {
		this.resultPartitionId = checkNotNull(resultPartitionId);
		this.intermediateDataSetId = checkNotNull(intermediateDataSetId);
		checkArgument(numberOfPartitions > 0);
		this.numberOfPartitions = numberOfPartitions;
	}

	public IntermediateDataSetID getIntermediateDataSetId() {
		return intermediateDataSetId;
	}

	public ResultPartitionID getResultPartitionId() {
		return resultPartitionId;
	}

	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TaskExecutorPartitionInfo that = (TaskExecutorPartitionInfo) o;
		// only use the dataset ID here, so we can use this as an efficient place for meta data
		return Objects.equals(intermediateDataSetId, that.intermediateDataSetId);
	}

	@Override
	public int hashCode() {
		// only use the dataset ID here, so we can use this as an efficient place for meta data
		return Objects.hash(intermediateDataSetId);
	}

	public static TaskExecutorPartitionInfo from(ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
		return new TaskExecutorPartitionInfo(
			resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID(),
			resultPartitionDeploymentDescriptor.getResultId(),
			resultPartitionDeploymentDescriptor.getTotalNumberOfPartitions());
	}
}
