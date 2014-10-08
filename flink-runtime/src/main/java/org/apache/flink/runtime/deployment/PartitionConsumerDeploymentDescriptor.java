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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.io.IOException;
import java.io.Serializable;

/**
 * A partition consumer deployment descriptor combines information of all partitions, which are
 * consumed by a single reader.
 */
public class PartitionConsumerDeploymentDescriptor implements IOReadableWritable, Serializable {

	private IntermediateDataSetID resultId;

	private PartitionInfo[] partitions;

	private int queueIndex;

	public PartitionConsumerDeploymentDescriptor() {
	}

	public PartitionConsumerDeploymentDescriptor(IntermediateDataSetID resultId, PartitionInfo[] partitions, int queueIndex) {
		this.resultId = resultId;
		this.partitions = partitions;
		this.queueIndex = queueIndex;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	public IntermediateDataSetID getResultId() {
		return resultId;
	}

	public PartitionInfo[] getPartitions() {
		return partitions;
	}

	public int getQueueIndex() {
		return queueIndex;
	}

	// ------------------------------------------------------------------------
	// Serialization
	// ------------------------------------------------------------------------

	@Override
	public void write(DataOutputView out) throws IOException {
		resultId.write(out);
		out.writeInt(partitions.length);
		for (PartitionInfo partition : partitions) {
			partition.write(out);
		}

		out.writeInt(queueIndex);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		resultId = new IntermediateDataSetID();
		resultId.read(in);

		partitions = new PartitionInfo[in.readInt()];
		for (int i = 0; i < partitions.length; i++) {
			partitions[i] = new PartitionInfo();
			partitions[i].read(in);
		}

		this.queueIndex = in.readInt();
	}
}
