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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Partial deployment descriptor for a single input channel instance.
 *
 * <p>This deployment descriptor is created in {@link Execution#scheduleOrUpdateConsumers(java.util.List)},
 * if the consumer instance is not yet clear. Once the instance on which the consumer runs is known,
 * the deployment descriptor is updated by completing the partition location.
 */
public class PartialInputChannelDeploymentDescriptor {

	private final PartitionShuffleDescriptor psd;

	private final ShuffleDeploymentDescriptor sdd;

	public PartialInputChannelDeploymentDescriptor(PartitionShuffleDescriptor psd, ShuffleDeploymentDescriptor sdd) {
		this.psd = checkNotNull(psd);
		this.sdd = checkNotNull(sdd);
	}

	/**
	 * Creates a channel deployment descriptor by completing the partition location.
	 *
	 * @see InputChannelDeploymentDescriptor
	 */
	public InputChannelDeploymentDescriptor createInputChannelDeploymentDescriptor(ResourceID consumerResourceId) {
		final LocationType locationType = LocationType.getLocationType(psd.getProducerResourceId(), consumerResourceId);

		return new InputChannelDeploymentDescriptor(
			new ResultPartitionID(psd.getPartitionId(), psd.getProducerExecutionId()),
			locationType,
			Optional.of(sdd.getConnectionId()));
	}

	public IntermediateDataSetID getResultId() {
		return psd.getResultId();
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates a partial channel deployment descriptor based on cached partition and shuffle descriptors.
	 */
	public static PartialInputChannelDeploymentDescriptor fromShuffleDescriptor(
			PartitionShuffleDescriptor psd,
			ShuffleDeploymentDescriptor sdd) {
		return new PartialInputChannelDeploymentDescriptor(psd, sdd);
	}
}
