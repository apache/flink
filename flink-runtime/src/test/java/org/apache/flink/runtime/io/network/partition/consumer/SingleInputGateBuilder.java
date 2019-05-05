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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskmanager.NoOpTaskActions;
import org.apache.flink.runtime.taskmanager.TaskActions;

import java.util.function.Supplier;

/**
 * Utility class to encapsulate the logic of building a {@link SingleInputGate} instance.
 */
public class SingleInputGateBuilder {

	private String taskName = "Single Input Gate";

	private JobID jobId = new JobID();

	private IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();

	private ResultPartitionType partitionType = ResultPartitionType.PIPELINED;

	private int consumedSubpartitionIndex = 0;

	private int numberOfChannels = 1;

	private Supplier<BufferPool> bufferPoolToSetup = () -> null;

	private SingleInputGate.MemorySegmentProvider segmentProvider = null;

	private TaskActions taskActions = new NoOpTaskActions();

	private Counter numBytesInCounter = new SimpleCounter();

	private boolean isCreditBased = true;

	public SingleInputGateBuilder setResultPartitionType(ResultPartitionType partitionType) {
		this.partitionType = partitionType;
		return this;
	}

	public SingleInputGateBuilder setConsumedSubpartitionIndex(int consumedSubpartitionIndex) {
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;
		return this;
	}

	public SingleInputGateBuilder setNumberOfChannels(int numberOfChannels) {
		this.numberOfChannels = numberOfChannels;
		return this;
	}

	public SingleInputGateBuilder setBufferPoolSupplier(Supplier<BufferPool> bufferPoolToSetup) {
		this.bufferPoolToSetup = bufferPoolToSetup;
		return this;
	}

	public SingleInputGateBuilder setIsCreditBased(boolean isCreditBased) {
		this.isCreditBased = isCreditBased;
		return this;
	}

	public SingleInputGate build() {
		return new SingleInputGate(
			taskName,
			jobId,
			intermediateDataSetID,
			partitionType,
			consumedSubpartitionIndex,
			numberOfChannels,
			segmentProvider,
			bufferPoolToSetup,
			taskActions,
			numBytesInCounter,
			isCreditBased);
	}
}
