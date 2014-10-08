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
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultType;

import java.io.IOException;
import java.util.List;

public class IntermediateResultPartitionDeploymentDescriptor implements IOReadableWritable {

	private IntermediateDataSetID resultId;

	private IntermediateResultType resultType;

	private IntermediateResultPartitionID partitionId;

	private int partitionNumber;

	private int numQueues;

	private GateDeploymentDescriptor gdd;

	public IntermediateResultPartitionDeploymentDescriptor(
			IntermediateDataSetID resultId, IntermediateResultType resultType,
			IntermediateResultPartitionID partitionId, int partitionNumber, int numQueues,
			GateDeploymentDescriptor gdd) {

		this.resultType = resultType;
		this.resultId = resultId;
		this.partitionId = partitionId;
		this.partitionNumber = partitionNumber;
		this.numQueues = numQueues;
		this.gdd = gdd;
	}

	public IntermediateResultPartitionDeploymentDescriptor() {
		this.resultId = new IntermediateDataSetID();
		this.resultType = IntermediateResultType.PIPELINED;
		this.partitionId = new IntermediateResultPartitionID();
		this.partitionNumber = -1;
		this.numQueues = -1;
		this.gdd = new GateDeploymentDescriptor();
	}

	public IntermediateDataSetID getResultId() {
		return resultId;
	}

	public IntermediateResultType getResultType() {
		return resultType;
	}

	public IntermediateResultPartitionID getPartitionId() {
		return partitionId;
	}

	public int getPartitionNumber() {
		return partitionNumber;
	}

	public int getNumQueues() {
		return numQueues;
	}

	public GateDeploymentDescriptor getGdd() {
		return gdd;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		resultId.write(out);
		out.writeInt(resultType.ordinal());
		partitionId.write(out);
		out.writeInt(partitionNumber);
		out.writeInt(numQueues);
		gdd.write(out);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		resultId.read(in);
		resultType = IntermediateResultType.values()[in.readInt()];
		partitionId.read(in);
		partitionNumber = in.readInt();
		numQueues = in.readInt();
		gdd.read(in);
	}

	public static IntermediateResultPartitionDeploymentDescriptor fromIntermediateResultPartition(IntermediateResultPartition partition) {
		IntermediateDataSetID resultId = partition.getIntermediateResult().getId();
		// The result type is runtime information indicating which type of
		// queue to use.
		// TODO This needs to be set depending on the job graph (consumers, incremental roll out etc.)
		IntermediateResultType resultType = IntermediateResultType.PIPELINED;
		IntermediateResultPartitionID partitionId = partition.getPartitionId();
		int partitionNumber = partition.getPartitionNumber();

		// At runtime, the produced partition data is partitioned among a
		// number of queues for re-partitioning.
		int numQueues = 1;

		// We use the number of incoming execution edges as the number of
		// queues. Currently, we only support a single consumer and use the
		// the execution edges as the number of required queues.
		// TODO this will be refactored when the scheduler is updated for fault tolerance, etc.
		if (!partition.getConsumers().isEmpty() && !partition.getConsumers().get(0).isEmpty()) {
			numQueues = partition.getConsumers().get(0).size();
		}

		List<ExecutionEdge> channels = partition.getConsumers().get(0);
		GateDeploymentDescriptor gdd = GateDeploymentDescriptor.fromEdges(channels);

		return new IntermediateResultPartitionDeploymentDescriptor(resultId, resultType, partitionId, partitionNumber, numQueues, gdd);
	}
}
