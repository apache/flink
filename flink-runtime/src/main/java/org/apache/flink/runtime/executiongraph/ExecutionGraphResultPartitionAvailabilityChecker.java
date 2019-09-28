/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.io.network.partition.PartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ResultPartitionAvailabilityChecker} which decides the intermediate result partition availability
 * based on whether the corresponding result partition in the execution graph is tracked.
 */
public class ExecutionGraphResultPartitionAvailabilityChecker implements ResultPartitionAvailabilityChecker {

	/** The function maps an IntermediateResultPartitionID to a ResultPartitionID. */
	private final Function<IntermediateResultPartitionID, ResultPartitionID> partitionIDMapper;

	/** The tracker that tracks all available result partitions. */
	private final PartitionTracker partitionTracker;

	ExecutionGraphResultPartitionAvailabilityChecker(
			final Function<IntermediateResultPartitionID, ResultPartitionID> partitionIDMapper,
			final PartitionTracker partitionTracker) {

		this.partitionIDMapper = checkNotNull(partitionIDMapper);
		this.partitionTracker = checkNotNull(partitionTracker);
	}

	@Override
	public boolean isAvailable(final IntermediateResultPartitionID resultPartitionID) {
		return partitionTracker.isPartitionTracked(partitionIDMapper.apply(resultPartitionID));
	}
}
