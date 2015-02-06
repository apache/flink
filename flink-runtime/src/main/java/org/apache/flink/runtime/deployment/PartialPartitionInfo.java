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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.io.network.RemoteAddress;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

/**
 * This class contains the partial partition info which is created if the consumer instance is not
 * yet clear. Once the instance on which the consumer runs is known, the complete partition info
 * can be computed.
 */
public class PartialPartitionInfo {
	private final IntermediateDataSetID intermediateDataSetID;

	private final IntermediateResultPartitionID partitionID;

	private final ExecutionAttemptID producerExecutionID;

	private final InstanceConnectionInfo producerInstanceConnectionInfo;

	private final int partitionConnectionIndex;

	public PartialPartitionInfo(IntermediateDataSetID intermediateDataSetID,
								IntermediateResultPartitionID partitionID,
								ExecutionAttemptID executionID,
								InstanceConnectionInfo producerInstanceConnectionInfo,
								int partitionConnectionIndex) {
		this.intermediateDataSetID = intermediateDataSetID;
		this.partitionID = partitionID;
		this.producerExecutionID = executionID;
		this.producerInstanceConnectionInfo = producerInstanceConnectionInfo;
		this.partitionConnectionIndex = partitionConnectionIndex;
	}

	public PartitionInfo createPartitionInfo(Execution consumerExecution) throws IllegalStateException {
		if(consumerExecution != null){
			PartitionInfo.PartitionLocation producerLocation;

			RemoteAddress resolvedProducerAddress;

			if(consumerExecution.getAssignedResourceLocation().equals(
					producerInstanceConnectionInfo)) {
				resolvedProducerAddress = null;
				producerLocation = PartitionInfo.PartitionLocation.LOCAL;
			} else {
				resolvedProducerAddress = new RemoteAddress(producerInstanceConnectionInfo,
						partitionConnectionIndex);

				producerLocation = PartitionInfo.PartitionLocation.REMOTE;
			}

			return new PartitionInfo(partitionID, producerExecutionID, producerLocation,
					resolvedProducerAddress);

		} else {
			throw new RuntimeException("Cannot create partition info, because consumer execution " +
					"is null.");
		}
	}

	public IntermediateDataSetID getIntermediateDataSetID() {
		return intermediateDataSetID;
	}

	public static PartialPartitionInfo fromEdge(final ExecutionEdge edge){
		IntermediateResultPartition partition = edge.getSource();
		IntermediateResultPartitionID partitionID = edge.getSource().getPartitionId();

		IntermediateDataSetID intermediateDataSetID = partition.getIntermediateResult().getId();

		Execution producer = partition.getProducer().getCurrentExecutionAttempt();
		ExecutionAttemptID producerExecutionID = producer.getAttemptId();

		return new PartialPartitionInfo(intermediateDataSetID, partitionID, producerExecutionID,
				producer.getAssignedResourceLocation(),
				partition.getIntermediateResult().getConnectionIndex());
	}

}
