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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.partition.BlockingShuffleType;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The proxy of ResultPartitionLocationTracker, decide which track should be used.
 */
public class ResultPartitionLocationTrackerProxy {
	private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionLocationTrackerProxy.class);

	/** Global configuration. */
	private final Configuration configuration;

	/**
	 * The internal result partition location tracker which tracks the result partitions served
	 * by taskmanager.
	 */
	private final InternalResultPartitionLocationTracker internalResultPartitionLocationTracker;

	/**
	 * The external result partition location tracker which tracks the result partitions served
	 * by external components, e.g. yarn shuffle service.
	 */
	private final ExternalResultPartitionLocationTracker externalResultPartitionLocationTracker;

	public ResultPartitionLocationTrackerProxy(
		Configuration configuration) {
		this.configuration = Preconditions.checkNotNull(configuration);

		this.internalResultPartitionLocationTracker = new InternalResultPartitionLocationTracker();
		this.externalResultPartitionLocationTracker = new ExternalResultPartitionLocationTracker(configuration);
	}

	/**
	 * Gets the result partition location. This method is used when generating {@link InputChannelDeploymentDescriptor}.
	 *
	 * @param producerLocation The partition producer location.
	 * @param consumerLocation The partition consumer location.
	 * @param intermediateResult The intermediate result to be consumed.
	 * @return The result partition location.
	 */
	public ResultPartitionLocation getResultPartitionLocation(
		TaskManagerLocation producerLocation,
		TaskManagerLocation consumerLocation,
		IntermediateResult intermediateResult) {

		BlockingShuffleType shuffleType =
			BlockingShuffleType.getBlockingShuffleTypeFromConfiguration(configuration, LOG);
		if (intermediateResult.getResultType() == ResultPartitionType.BLOCKING
			&& shuffleType == BlockingShuffleType.YARN) {
			// use the yarn shuffle service
			return externalResultPartitionLocationTracker.getResultPartitionLocation(
				producerLocation, consumerLocation, intermediateResult);
		} else {
			// use internal shuffle service
			return internalResultPartitionLocationTracker.getResultPartitionLocation(
				producerLocation, consumerLocation, intermediateResult);
		}
	}
}
