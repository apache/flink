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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import java.io.IOException;
import java.io.Serializable;

/**
 * A partition deployment descriptor combines information for a produced intermediate result
 * partition.
 */
public class PartitionDeploymentDescriptor implements IOReadableWritable, Serializable {

	private final IntermediateDataSetID resultId;

	private final ResultPartitionID partitionId;

	private ResultPartitionType partitionType;

	private int numberOfQueues;

	public PartitionDeploymentDescriptor() {
		this.resultId = new IntermediateDataSetID();
		this.partitionId = new ResultPartitionID();
		this.numberOfQueues = -1;
	}

	public PartitionDeploymentDescriptor(IntermediateDataSetID resultId, ResultPartitionID partitionId, ResultPartitionType partitionType, int numberOfQueues) {
		this.resultId = resultId;
		this.partitionId = partitionId;
		this.partitionType = partitionType;
		this.numberOfQueues = numberOfQueues;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	public IntermediateDataSetID getResultId() {
		return resultId;
	}

	public ResultPartitionID getPartitionId() {
		return partitionId;
	}

	public ResultPartitionType getPartitionType() {
		return partitionType;
	}

	public int getNumberOfSubpartitions() {
		return numberOfQueues;
	}

	// ------------------------------------------------------------------------
	// Serialization
	// ------------------------------------------------------------------------

	@Override
	public void write(DataOutputView out) throws IOException {
		resultId.write(out);
		partitionId.write(out);
		out.writeInt(partitionType.ordinal());
		out.writeInt(numberOfQueues);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		resultId.read(in);
		partitionId.read(in);
		partitionType = ResultPartitionType.values()[in.readInt()];
		numberOfQueues = in.readInt();
	}

	// ------------------------------------------------------------------------

	public static PartitionDeploymentDescriptor fromIntermediateResultPartition(IntermediateResultPartition partition) {

		ResultPartitionID partitionId = partition.getPartitionId();

		// The produced data is partitioned at runtime among a number of queues.
		// If no consumers are known at this point, we use a single queue,
		// otherwise we have a queue for each consumer sub task.
		int numberOfQueues = 1;

		if (!partition.getConsumers().isEmpty() && !partition.getConsumers().get(0).isEmpty()) {
			numberOfQueues = partition.getConsumers().get(0).size();
		}

		return new PartitionDeploymentDescriptor(partition.getIntermediateResult().getId(), partitionId, partition.getIntermediateResult().getRuntimeType(), numberOfQueues);
	}
}
