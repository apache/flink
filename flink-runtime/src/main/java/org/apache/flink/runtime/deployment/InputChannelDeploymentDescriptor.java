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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deployment descriptor for a single input channel instance.
 *
 * <p>Each input channel consumes a single subpartition. The index of the subpartition to consume
 * is part of the {@link InputGateDeploymentDescriptor} as it is the same for each input channel of
 * the respective input gate.
 *
 * @see InputChannel
 * @see SingleInputGate
 */
public class InputChannelDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = 373711381640454080L;

	/** The ID of the partition the input channel is going to consume. */
	private final ResultPartitionID consumedPartitionId;

	/** The location of the partition the input channel is going to consume. */
	private final ResultPartitionLocation consumedPartitionLocation;

	public InputChannelDeploymentDescriptor(
			ResultPartitionID consumedPartitionId,
			ResultPartitionLocation consumedPartitionLocation) {

		this.consumedPartitionId = checkNotNull(consumedPartitionId);
		this.consumedPartitionLocation = checkNotNull(consumedPartitionLocation);
	}

	public ResultPartitionID getConsumedPartitionId() {
		return consumedPartitionId;
	}

	public ResultPartitionLocation getConsumedPartitionLocation() {
		return consumedPartitionLocation;
	}

	@Override
	public String toString() {
		return String.format("InputChannelDeploymentDescriptor [consumed partition id: %s, " +
						"consumed partition location: %s]",
				consumedPartitionId, consumedPartitionLocation);
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates an input channel deployment descriptor for each partition.
	 */
	public static List<InputChannelDeploymentDescriptor> fromEdges(
			List<ExecutionEdge> edges,
			boolean allowLazyDeployment) {
		return edges.stream().map(edge -> fromEdgeAndValidate(allowLazyDeployment, edge)).collect(Collectors.toList());
	}

	@Nonnull
	private static InputChannelDeploymentDescriptor fromEdgeAndValidate(boolean allowLazyDeployment, ExecutionEdge edge) {
		final InputChannelDeploymentDescriptor inputChannelDeploymentDescriptor = fromEdge(edge);

		if (!allowLazyDeployment && inputChannelDeploymentDescriptor.getConsumedPartitionLocation().isUnknown()) {
			final IntermediateResultPartition consumedPartition = edge.getSource();
			final Execution producer = consumedPartition.getProducer().getCurrentExecutionAttempt();
			final ExecutionState producerState = producer.getState();

			if (producerState == ExecutionState.CANCELING
				|| producerState == ExecutionState.CANCELED
				|| producerState == ExecutionState.FAILED) {
				String msg = "Trying to schedule a task whose inputs were canceled or failed. " +
					"The producer is in state " + producerState + '.';
				throw new IllegalStateException(msg);
			} else {
				final LogicalSlot producerSlot = producer.getAssignedResource();

				String msg = String.format("Trying to eagerly schedule a task whose inputs " +
						"are not ready (result type: %s, partition consumable: %s, producer state: %s, producer slot: %s).",
					consumedPartition.getResultType(),
					consumedPartition.isConsumable(),
					producerState,
					producerSlot);
				throw new IllegalStateException(msg);
			}
		}

		return inputChannelDeploymentDescriptor;
	}

	@Nonnull
	public static InputChannelDeploymentDescriptor fromEdge(ExecutionEdge edge) {
		final IntermediateResultPartition consumedPartition = edge.getSource();
		final Execution producer = consumedPartition.getProducer().getCurrentExecutionAttempt();

		final ExecutionState producerState = producer.getState();
		final LogicalSlot producerSlot = producer.getAssignedResource();

		final ResultPartitionLocation partitionLocation;

		// The producing task needs to be RUNNING or already FINISHED
		if ((consumedPartition.getResultType().isPipelined() || consumedPartition.isConsumable()) &&
			producerSlot != null &&
				(producerState == ExecutionState.RUNNING ||
					producerState == ExecutionState.FINISHED ||
					producerState == ExecutionState.SCHEDULED ||
					producerState == ExecutionState.DEPLOYING)) {

			final TaskManagerLocation partitionTaskManagerLocation = producerSlot.getTaskManagerLocation();
			final LogicalSlot consumerSlot = edge.getTarget().getCurrentAssignedResource();

			if (consumerSlot != null) {
				partitionLocation = createKnownResultPartitionLocation(consumerSlot.getTaskManagerLocation().getResourceID(), consumedPartition, partitionTaskManagerLocation);
			} else {
				throw new IllegalStateException("Cannot create an input channel descriptor for a consumer which has no slot assigned.");
			}
		}
		else {
			// The producing task might not have registered the partition yet
			partitionLocation = ResultPartitionLocation.createUnknown();
		}

		final ResultPartitionID consumedPartitionId = new ResultPartitionID(
				consumedPartition.getPartitionId(), producer.getAttemptId());

		return new InputChannelDeploymentDescriptor(consumedPartitionId, partitionLocation);
	}

	@Nonnull
	private static ResultPartitionLocation createKnownResultPartitionLocation(
			ResourceID consumerResourceId,
			IntermediateResultPartition consumedPartition,
			TaskManagerLocation partitionTaskManagerLocation) {
		ResultPartitionLocation partitionLocation;
		final ResourceID partitionTaskManager = partitionTaskManagerLocation.getResourceID();

		if (partitionTaskManager.equals(consumerResourceId)) {
			// Consuming task is deployed to the same TaskManager as the partition => local
			partitionLocation = ResultPartitionLocation.createLocal();
		}
		else {
			// Different instances => remote
			final ConnectionID connectionId = new ConnectionID(
					partitionTaskManagerLocation,
					consumedPartition.getIntermediateResult().getConnectionIndex());

			partitionLocation = ResultPartitionLocation.createRemote(connectionId);
		}
		return partitionLocation;
	}
}
