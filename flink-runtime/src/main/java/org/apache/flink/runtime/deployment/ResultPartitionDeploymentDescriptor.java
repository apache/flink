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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor.ReleaseType;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.io.Serializable;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deployment descriptor for a result partition.
 *
 * @see ResultPartition
 */
public class ResultPartitionDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = 6343547936086963705L;

	private final PartitionDescriptor partitionDescriptor;

	private final ShuffleDescriptor shuffleDescriptor;

	private final int maxParallelism;

	/** Flag whether the result partition should send scheduleOrUpdateConsumer messages. */
	private final boolean sendScheduleOrUpdateConsumersMessage;

	private final ReleaseType releaseType;

	@VisibleForTesting
	public ResultPartitionDeploymentDescriptor(
			PartitionDescriptor partitionDescriptor,
			ShuffleDescriptor shuffleDescriptor,
			int maxParallelism,
			boolean sendScheduleOrUpdateConsumersMessage) {
		this(
			checkNotNull(partitionDescriptor),
			shuffleDescriptor,
			maxParallelism,
			sendScheduleOrUpdateConsumersMessage,
			ReleaseType.AUTO);
	}

	public ResultPartitionDeploymentDescriptor(
			PartitionDescriptor partitionDescriptor,
			ShuffleDescriptor shuffleDescriptor,
			int maxParallelism,
			boolean sendScheduleOrUpdateConsumersMessage,
			ReleaseType releaseType) {
		checkReleaseOnConsumptionIsSupportedForPartition(shuffleDescriptor, releaseType);
		this.partitionDescriptor = checkNotNull(partitionDescriptor);
		this.shuffleDescriptor = checkNotNull(shuffleDescriptor);
		KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
		this.maxParallelism = maxParallelism;
		this.sendScheduleOrUpdateConsumersMessage = sendScheduleOrUpdateConsumersMessage;
		this.releaseType = releaseType;
	}

	private static void checkReleaseOnConsumptionIsSupportedForPartition(
			ShuffleDescriptor shuffleDescriptor,
			ReleaseType releaseType) {
		checkNotNull(shuffleDescriptor);
		checkArgument(
			shuffleDescriptor.getSupportedReleaseTypes().contains(releaseType),
			"Release type %s is not supported by the shuffle service for this partition" +
				"(id: %s), supported release types: %s",
			releaseType,
			shuffleDescriptor.getResultPartitionID(),
			shuffleDescriptor.getSupportedReleaseTypes());
	}

	public IntermediateDataSetID getResultId() {
		return partitionDescriptor.getResultId();
	}

	public IntermediateResultPartitionID getPartitionId() {
		return partitionDescriptor.getPartitionId();
	}

	public ResultPartitionType getPartitionType() {
		return partitionDescriptor.getPartitionType();
	}

	public int getNumberOfSubpartitions() {
		return partitionDescriptor.getNumberOfSubpartitions();
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	public ShuffleDescriptor getShuffleDescriptor() {
		return shuffleDescriptor;
	}

	public boolean sendScheduleOrUpdateConsumersMessage() {
		return sendScheduleOrUpdateConsumersMessage;
	}

	/**
	 * Returns whether to release the partition after having been fully consumed once.
	 *
	 * <p>Indicates whether the shuffle service should automatically release all partition resources after
	 * the first full consumption has been acknowledged. This kind of partition does not need to be explicitly released
	 * by {@link ShuffleMaster#releasePartitionExternally(ShuffleDescriptor)}
	 * and {@link ShuffleEnvironment#releasePartitionsLocally(Collection)}.
	 *
	 * <p>The partition has to support the corresponding {@link ReleaseType} in
	 * {@link ShuffleDescriptor#getSupportedReleaseTypes()}:
	 * {@link ReleaseType#AUTO} for {@code isReleasedOnConsumption()} to return {@code true} and
	 * {@link ReleaseType#MANUAL} for {@code isReleasedOnConsumption()} to return {@code false}.
	 *
	 * @return whether to release the partition after having been fully consumed once.
	 */
	public boolean isReleasedOnConsumption() {
		return releaseType == ReleaseType.AUTO;
	}

	@Override
	public String toString() {
		return String.format("ResultPartitionDeploymentDescriptor [PartitionDescriptor: %s, "
						+ "ShuffleDescriptor: %s]",
			partitionDescriptor, shuffleDescriptor);
	}
}
