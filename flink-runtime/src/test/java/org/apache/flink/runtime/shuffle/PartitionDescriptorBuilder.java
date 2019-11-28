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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

/**
 * Builder for {@link PartitionDescriptor} in tests.
 */
public class PartitionDescriptorBuilder {
	private IntermediateResultPartitionID partitionId;
	private ResultPartitionType partitionType;
	private int totalNumberOfPartitions = 1;

	private PartitionDescriptorBuilder() {
		this.partitionId = new IntermediateResultPartitionID();
		this.partitionType = ResultPartitionType.PIPELINED;
	}

	public PartitionDescriptorBuilder setPartitionId(IntermediateResultPartitionID partitionId) {
		this.partitionId = partitionId;
		return this;
	}

	public PartitionDescriptorBuilder setPartitionType(ResultPartitionType partitionType) {
		this.partitionType = partitionType;
		return this;
	}

	public PartitionDescriptorBuilder setTotalNumberOfPartitions(int totalNumberOfPartitions) {
		this.totalNumberOfPartitions = totalNumberOfPartitions;
		return this;
	}

	public PartitionDescriptor build() {
		return new PartitionDescriptor(new IntermediateDataSetID(), totalNumberOfPartitions, partitionId, partitionType, 1, 0);
	}

	public static PartitionDescriptorBuilder newBuilder() {
		return new PartitionDescriptorBuilder();
	}
}
