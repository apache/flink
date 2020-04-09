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

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

/**
 * A wrapper class for {@link InputDependencyConstraint} checker.
 */
public class InputDependencyConstraintChecker {

	public boolean check(final SchedulingExecutionVertex<?, ?> schedulingExecutionVertex) {
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

	private boolean checkAll(final SchedulingExecutionVertex<?, ?> schedulingExecutionVertex) {
		for (SchedulingResultPartition<?, ?> consumedResultPartition : schedulingExecutionVertex.getConsumedResults()) {
			if (!partitionConsumable(consumedResultPartition)) {
				return false;
			}
		}
		return true;
	}

	private boolean checkAny(final SchedulingExecutionVertex<?, ?> schedulingExecutionVertex) {
		for (SchedulingResultPartition<?, ?> consumedResultPartition : schedulingExecutionVertex.getConsumedResults()) {
			if (partitionConsumable(consumedResultPartition)) {
				return true;
			}
		}
		return false;
	}

	private boolean partitionConsumable(SchedulingResultPartition<?, ?> partition) {
		final ResultPartitionState state = partition.getState();
		return ResultPartitionState.CONSUMABLE.equals(state);
	}
}
