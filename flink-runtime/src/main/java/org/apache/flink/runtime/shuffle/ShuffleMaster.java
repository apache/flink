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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Intermediate result partition registry to use in {@link org.apache.flink.runtime.jobmaster.JobMaster}.
 *
 * @param <T> partition shuffle descriptor used for producer/consumer deployment and their data exchange.
 */
public interface ShuffleMaster<T extends ShuffleDescriptor> {

	/**
	 * Asynchronously register a partition and its producer with the shuffle service.
	 *
	 * <p>The returned shuffle descriptor is an internal handle
	 * which identifies the partition internally within the shuffle service.
	 * The descriptor should provide enough information to read from or write data to the partition.
	 *
	 * @param partitionDescriptor general job graph information about the partition
	 * @param producerDescriptor general producer information (location, execution id, connection info)
	 * @return future with the partition shuffle descriptor used for producer/consumer deployment and their data exchange.
	 */
	CompletableFuture<T> registerPartitionWithProducer(
		PartitionDescriptor partitionDescriptor,
		ProducerDescriptor producerDescriptor);

	/**
	 * Release any external resources occupied by the given partition.
	 *
	 * <p>This call triggers release of any resources which are occupied by the given partition in the external systems
	 * outside of the producer executor. This is mostly relevant for the batch jobs and blocking result partitions.
	 * The producer local resources are managed by {@link ShuffleDescriptor#storesLocalResourcesOn()} and
	 * {@link ShuffleEnvironment#releasePartitionsLocally(Collection)}.
	 *
	 * @param shuffleDescriptor shuffle descriptor of the result partition to release externally.
	 */
	void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor);
}
