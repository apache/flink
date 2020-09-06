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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;

/**
 * A wrapper class for {@link InputDependencyConstraint} checker.
 */
public class InputDependencyConstraintChecker {
	private final SchedulingIntermediateDataSetManager intermediateDataSetManager =
		new SchedulingIntermediateDataSetManager();

	public boolean check(final SchedulingExecutionVertex schedulingExecutionVertex) {
		if (Iterables.isEmpty(schedulingExecutionVertex.getConsumedResults())) {
			return true;
		}

		final InputDependencyConstraint inputConstraint = schedulingExecutionVertex.getInputDependencyConstraint();
		switch (inputConstraint) {
			case ANY:
				return checkAny(schedulingExecutionVertex);
			case ALL:
				return checkAll(schedulingExecutionVertex);
			default:
				throw new IllegalStateException("Unknown InputDependencyConstraint " + inputConstraint);
		}
	}

	List<SchedulingResultPartition> markSchedulingResultPartitionFinished(SchedulingResultPartition srp) {
		return intermediateDataSetManager.markSchedulingResultPartitionFinished(srp);
	}

	void resetSchedulingResultPartition(SchedulingResultPartition srp) {
		intermediateDataSetManager.resetSchedulingResultPartition(srp);
	}

	void addSchedulingResultPartition(SchedulingResultPartition srp) {
		intermediateDataSetManager.addSchedulingResultPartition(srp);
	}

	private boolean checkAll(final SchedulingExecutionVertex schedulingExecutionVertex) {
		for (SchedulingResultPartition consumedResultPartition : schedulingExecutionVertex.getConsumedResults()) {
			if (!partitionConsumable(consumedResultPartition)) {
				return false;
			}
		}
		return true;
	}

	private boolean checkAny(final SchedulingExecutionVertex schedulingExecutionVertex) {
		for (SchedulingResultPartition consumedResultPartition : schedulingExecutionVertex.getConsumedResults()) {
			if (partitionConsumable(consumedResultPartition)) {
				return true;
			}
		}
		return false;
	}

	private boolean partitionConsumable(SchedulingResultPartition partition) {
		if (BLOCKING.equals(partition.getResultType())) {
			return intermediateDataSetManager.allPartitionsFinished(partition);
		} else {
			final ResultPartitionState state = partition.getState();
			return ResultPartitionState.CONSUMABLE.equals(state);
		}
	}

	private static class SchedulingIntermediateDataSetManager {

		private final Map<IntermediateDataSetID, SchedulingIntermediateDataSet> intermediateDataSets = new HashMap<>();

		List<SchedulingResultPartition> markSchedulingResultPartitionFinished(SchedulingResultPartition srp) {
			SchedulingIntermediateDataSet intermediateDataSet = getSchedulingIntermediateDataSet(srp.getResultId());
			if (intermediateDataSet.markPartitionFinished(srp.getId())) {
				return intermediateDataSet.getSchedulingResultPartitions();
			}
			return Collections.emptyList();
		}

		void resetSchedulingResultPartition(SchedulingResultPartition srp) {
			SchedulingIntermediateDataSet sid = getSchedulingIntermediateDataSet(srp.getResultId());
			sid.resetPartition(srp.getId());
		}

		void addSchedulingResultPartition(SchedulingResultPartition srp) {
			SchedulingIntermediateDataSet sid = getOrCreateSchedulingIntermediateDataSetIfAbsent(srp.getResultId());
			sid.addSchedulingResultPartition(srp);
		}

		boolean allPartitionsFinished(SchedulingResultPartition srp) {
			SchedulingIntermediateDataSet sid = getSchedulingIntermediateDataSet(srp.getResultId());
			return sid.allPartitionsFinished();
		}

		private SchedulingIntermediateDataSet getSchedulingIntermediateDataSet(
				final IntermediateDataSetID intermediateDataSetId) {
			return getSchedulingIntermediateDataSetInternal(intermediateDataSetId, false);
		}

		private SchedulingIntermediateDataSet getOrCreateSchedulingIntermediateDataSetIfAbsent(
				final IntermediateDataSetID intermediateDataSetId) {
			return getSchedulingIntermediateDataSetInternal(intermediateDataSetId, true);
		}

		private SchedulingIntermediateDataSet getSchedulingIntermediateDataSetInternal(
				final IntermediateDataSetID intermediateDataSetId,
				boolean createIfAbsent) {

			return intermediateDataSets.computeIfAbsent(
				intermediateDataSetId,
				(key) -> {
					if (createIfAbsent) {
						return new SchedulingIntermediateDataSet();
					} else {
						throw new IllegalArgumentException("can not find data set for " + intermediateDataSetId);
					}
				});
		}
	}

	/**
	 * Representation of {@link IntermediateDataSet}.
	 */
	private static class SchedulingIntermediateDataSet {

		private final List<SchedulingResultPartition> partitions;

		private final Set<IntermediateResultPartitionID> producingPartitionIds;

		SchedulingIntermediateDataSet() {
			partitions = new ArrayList<>();
			producingPartitionIds = new HashSet<>();
		}

		boolean markPartitionFinished(IntermediateResultPartitionID partitionId) {
			producingPartitionIds.remove(partitionId);
			return producingPartitionIds.isEmpty();
		}

		void resetPartition(IntermediateResultPartitionID partitionId) {
			producingPartitionIds.add(partitionId);
		}

		boolean allPartitionsFinished() {
			return producingPartitionIds.isEmpty();
		}

		void addSchedulingResultPartition(SchedulingResultPartition partition) {
			partitions.add(partition);
			producingPartitionIds.add(partition.getId());
		}

		List<SchedulingResultPartition> getSchedulingResultPartitions() {
			return Collections.unmodifiableList(partitions);
		}
	}
}
