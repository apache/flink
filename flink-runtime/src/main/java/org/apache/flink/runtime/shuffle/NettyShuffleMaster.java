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

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.LocalExecutionPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.NetworkPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.PartitionConnectionInfo;

import java.util.concurrent.CompletableFuture;

/**
 * Default {@link ShuffleMaster} for netty and local file based shuffle implementation.
 */
public enum NettyShuffleMaster implements ShuffleMaster<NettyShuffleDescriptor> {
	INSTANCE;

	@Override
	public CompletableFuture<NettyShuffleDescriptor> registerPartitionWithProducer(
			PartitionDescriptor partitionDescriptor,
			ProducerDescriptor producerDescriptor) {

		ResultPartitionID resultPartitionID = new ResultPartitionID(
			partitionDescriptor.getPartitionId(),
			producerDescriptor.getProducerExecutionId());

		NettyShuffleDescriptor shuffleDeploymentDescriptor = new NettyShuffleDescriptor(
			producerDescriptor.getProducerLocation(),
			createConnectionInfo(producerDescriptor, partitionDescriptor.getConnectionIndex()),
			resultPartitionID);

		return CompletableFuture.completedFuture(shuffleDeploymentDescriptor);
	}

	@Override
	public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
	}

	private static PartitionConnectionInfo createConnectionInfo(
			ProducerDescriptor producerDescriptor,
			int connectionIndex) {
		return producerDescriptor.getDataPort() >= 0 ?
			NetworkPartitionConnectionInfo.fromProducerDescriptor(producerDescriptor, connectionIndex) :
			LocalExecutionPartitionConnectionInfo.INSTANCE;
	}
}
