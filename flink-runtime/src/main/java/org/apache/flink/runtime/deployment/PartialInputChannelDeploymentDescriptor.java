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

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Partial deployment descriptor for a single input channel instance.
 *
 * <p>This deployment descriptor is created in {@link Execution#scheduleOrUpdateConsumers(java.util.List)},
 * if the consumer instance is not yet clear. Once the instance on which the consumer runs is known,
 * the deployment descriptor is updated by completing the partition location.
 */
public class PartialInputChannelDeploymentDescriptor {

	/** The intermediate result. */
	private final IntermediateResult intermediateResult;

	/** The partition ID identifies the input channel to update. */
	private final ResultPartitionID partitionID;

	/** The partition connection info. */
	private final TaskManagerLocation partitionTaskManagerLocation;

	public PartialInputChannelDeploymentDescriptor(
			IntermediateResult intermediateResult,
			ResultPartitionID partitionID,
			TaskManagerLocation partitionTaskManagerLocation) {

		this.intermediateResult = checkNotNull(intermediateResult);
		this.partitionID = checkNotNull(partitionID);
		this.partitionTaskManagerLocation = checkNotNull(partitionTaskManagerLocation);
	}

	/**
	 * Creates a channel deployment descriptor by completing the partition location.
	 *
	 * @see InputChannelDeploymentDescriptor
	 */
	public InputChannelDeploymentDescriptor createInputChannelDeploymentDescriptor(
		Execution consumerExecution,
		ResultPartitionLocationTrackerProxy resultPartitionLocationTrackerProxy) {
		checkNotNull(consumerExecution, "consumerExecution");

		TaskManagerLocation consumerLocation = consumerExecution.getAssignedResourceLocation();
		checkNotNull(consumerLocation, "Consumer connection info null");

		final ResultPartitionLocation partitionLocation =
			resultPartitionLocationTrackerProxy.getResultPartitionLocation(
			partitionTaskManagerLocation, consumerLocation, intermediateResult);

		return new InputChannelDeploymentDescriptor(partitionID, partitionLocation);
	}

	public IntermediateDataSetID getResultId() {
		return intermediateResult.getId();
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates a partial input channel for the given partition and producing task.
	 */
	public static PartialInputChannelDeploymentDescriptor fromEdge(
			IntermediateResultPartition partition,
			Execution producer) {

		final ResultPartitionID partitionId = new ResultPartitionID(
				partition.getPartitionId(), producer.getAttemptId());

		final IntermediateResult result = partition.getIntermediateResult();

		final TaskManagerLocation partitionConnectionInfo = producer.getAssignedResourceLocation();

		return new PartialInputChannelDeploymentDescriptor(
				result, partitionId, partitionConnectionInfo);
	}
}
