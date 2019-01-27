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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.ArrayList;
import java.util.List;

public class IntermediateResultPartition {

	private final IntermediateResult totalResult;

	private final ExecutionVertex producer;

	private final int partitionNumber;

	private IntermediateResultPartitionID partitionId;

	private List<List<ExecutionEdge>> consumers;

	/**
	 * Whether this partition has data produced. For pipelined results only.
	 */
	private boolean dataProduced = false;

	/**
	 * Whether this partition is finished.
	 */
	private boolean isFinished = false;

	public IntermediateResultPartition(IntermediateResult totalResult, ExecutionVertex producer, int partitionNumber) {
		this.totalResult = totalResult;
		this.producer = producer;
		this.partitionNumber = partitionNumber;
		this.consumers = new ArrayList<List<ExecutionEdge>>(0);
		this.partitionId = new IntermediateResultPartitionID();
	}

	public ExecutionVertex getProducer() {
		return producer;
	}

	public int getPartitionNumber() {
		return partitionNumber;
	}

	public IntermediateResult getIntermediateResult() {
		return totalResult;
	}

	/**
	 * Set partition id to the one reported by task executor during jm failover.
	 */
	public void setPartitionId(IntermediateResultPartitionID id) {
		this.partitionId = id;
	}

	public IntermediateResultPartitionID getPartitionId() {
		return partitionId;
	}

	public ResultPartitionType getResultType() {
		return totalResult.getResultType();
	}

	public List<List<ExecutionEdge>> getConsumers() {
		return consumers;
	}

	public void resetForNewExecution() {
		if (isConsumable()) {
			getIntermediateResult().decrementNumberOfConsumablePartitions();
		}

		isFinished = false;
		dataProduced = false;
	}

	public void markDataProduced() {
		if (!isConsumable()) {
			getIntermediateResult().incrementNumberOfConsumablePartitions();
		}
		dataProduced = true;
	}

	public boolean hasDataProduced() {
		return dataProduced;
	}

	public boolean isConsumable() {
		if (getResultType().isPipelined()) {
			return dataProduced;
		} else {
			return isFinished;
		}
	}

	int addConsumerGroup() {
		int pos = consumers.size();

		// NOTE: currently we support only one consumer per result!!!
		if (pos != 0) {
			throw new RuntimeException("Currently, each intermediate result can only have one consumer.");
		}

		consumers.add(new ArrayList<ExecutionEdge>());
		return pos;
	}

	void addConsumer(ExecutionEdge edge, int consumerNumber) {
		consumers.get(consumerNumber).add(edge);
	}

	public void markFinished() {
		if (getResultType().isBlocking() && !isConsumable()) {
			getIntermediateResult().incrementNumberOfConsumablePartitions();
		}
		isFinished = true;
	}
}
