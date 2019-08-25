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

import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;

/**
 * Utility class to encapsulate the logic of building a {@link SingleInputGate} instance.
 */
public class SingleInputGateBuilder {

	public static final PartitionProducerStateProvider NO_OP_PRODUCER_CHECKER = (dsid, id, consumer) -> {};

	private final IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();

	private ResultPartitionType partitionType = ResultPartitionType.PIPELINED;

	private int consumedSubpartitionIndex = 0;

	private int numberOfChannels = 1;

	private PartitionProducerStateProvider partitionProducerStateProvider = NO_OP_PRODUCER_CHECKER;

	private boolean isCreditBased = true;

	private SupplierWithException<BufferPool, IOException> bufferPoolFactory = () -> {
		throw new UnsupportedOperationException();
	};

	public SingleInputGateBuilder setPartitionProducerStateProvider(
		PartitionProducerStateProvider partitionProducerStateProvider) {

		this.partitionProducerStateProvider = partitionProducerStateProvider;
		return this;
	}

	public SingleInputGateBuilder setResultPartitionType(ResultPartitionType partitionType) {
		this.partitionType = partitionType;
		return this;
	}

	SingleInputGateBuilder setConsumedSubpartitionIndex(int consumedSubpartitionIndex) {
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;
		return this;
	}

	public SingleInputGateBuilder setNumberOfChannels(int numberOfChannels) {
		this.numberOfChannels = numberOfChannels;
		return this;
	}

	public SingleInputGateBuilder setIsCreditBased(boolean isCreditBased) {
		this.isCreditBased = isCreditBased;
		return this;
	}

	public SingleInputGateBuilder setupBufferPoolFactory(NettyShuffleEnvironment environment) {
		NettyShuffleEnvironmentConfiguration config = environment.getConfiguration();
		this.bufferPoolFactory = SingleInputGateFactory.createBufferPoolFactory(
			environment.getNetworkBufferPool(),
			config.isCreditBased(),
			config.networkBuffersPerChannel(),
			config.floatingNetworkBuffersPerGate(),
			numberOfChannels,
			partitionType);
		return this;
	}

	public SingleInputGateBuilder setBufferPoolFactory(BufferPool bufferPool) {
		this.bufferPoolFactory = () -> bufferPool;
		return this;
	}

	public SingleInputGate build() {
		return new SingleInputGate(
			"Single Input Gate",
			intermediateDataSetID,
			partitionType,
			consumedSubpartitionIndex,
			numberOfChannels,
			partitionProducerStateProvider,
			isCreditBased,
			bufferPoolFactory);
	}
}
