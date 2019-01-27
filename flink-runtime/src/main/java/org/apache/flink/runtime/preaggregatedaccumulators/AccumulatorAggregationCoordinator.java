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

package org.apache.flink.runtime.preaggregatedaccumulators;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Responds for collecting the partially aggregated accumulators from TaskExecutors and merging them together, and
 * answering the queries when the final values have been acquired.
 */
public class AccumulatorAggregationCoordinator {
	/** The partly committed or fully committed accumulators. */
	private final Map<String, GlobalAggregatedAccumulator> aggregatedAccumulators = new HashMap<>();

	/** The unfulfilled query futures, which will be fulfilled when the target accumulator finishes aggregating. */
	private final Map<String, List<CompletableFuture<Accumulator>>> unfulfilledQueryFutures = new HashMap<>();

	@SuppressWarnings("unchecked")
	public void commitPreAggregatedAccumulator(ExecutionGraph executionGraph, CommitAccumulator commitAccumulator) {
		ExecutionJobVertex jobVertex = executionGraph.getJobVertex(commitAccumulator.getJobVertexId());
		// JobVertex remains not changed even with failovers, therefore a valid job vertex should never be null.
		if (jobVertex == null) {
			throw new IllegalArgumentException("Commit contains an invalid job vertex id: " + commitAccumulator.getJobVertexId());
		}

		GlobalAggregatedAccumulator globalAggregatedAccumulator =
			aggregatedAccumulators.computeIfAbsent(commitAccumulator.getName(), k -> new GlobalAggregatedAccumulator(commitAccumulator.getJobVertexId()));

		if (globalAggregatedAccumulator.isAllCommitted()) {
			// May be due to failover or rescaling. For failover, we desert the repeat values.
			// TODO: Handle rescaling of jobVertex.
			return;
		}

		globalAggregatedAccumulator.commitForTasks(commitAccumulator.getJobVertexId(),
			commitAccumulator.getAccumulator(),
			commitAccumulator.getCommittedTasks());

		checkState(globalAggregatedAccumulator.getCommittedTasks().size() <= jobVertex.getParallelism(),
			"More tasks committed than the total number of tasks.");

		if (globalAggregatedAccumulator.getCommittedTasks().size() == jobVertex.getParallelism()) {
			globalAggregatedAccumulator.markAllCommitted();

			List<CompletableFuture<Accumulator>> queryFutures = unfulfilledQueryFutures.get(commitAccumulator.getName());
			if (queryFutures != null) {
				queryFutures.forEach(query -> {
					query.complete(globalAggregatedAccumulator.getAggregatedValue());
				});
				unfulfilledQueryFutures.remove(commitAccumulator.getName());
			}
		}
	}

	@SuppressWarnings("unchecked")
	public <V, A extends Serializable> CompletableFuture<Accumulator<V, A>> queryPreAggregatedAccumulator(String name) {
		CompletableFuture<Accumulator<V, A>> queryFuture = new CompletableFuture<>();

		GlobalAggregatedAccumulator globalAggregatedAccumulator = aggregatedAccumulators.get(name);
		if (globalAggregatedAccumulator == null || !globalAggregatedAccumulator.isAllCommitted()) {
			unfulfilledQueryFutures.computeIfAbsent(name, k -> new ArrayList<>()).add((CompletableFuture) queryFuture);
		} else {
			queryFuture.complete(globalAggregatedAccumulator.getAggregatedValue());
		}

		return queryFuture;
	}

	public void clear() {
		aggregatedAccumulators.clear();
		unfulfilledQueryFutures.clear();
	}

	@VisibleForTesting
	public Map<String, GlobalAggregatedAccumulator> getAggregatedAccumulators() {
		return aggregatedAccumulators;
	}

	public Map<String, List<CompletableFuture<Accumulator>>> getUnfulfilledQueryFutures() {
		return unfulfilledQueryFutures;
	}

	/**
	 * Wrapper class for managing the committed status of a specific accumulator.
	 */
	static class GlobalAggregatedAccumulator {
		/** The job vertex ID of the tasks that commit the values. */
		private final JobVertexID jobVertexId;

		/** The index of the tasks who have committed. */
		private final Set<Integer> committedTasks = new HashSet<>();

		/** The partially aggregated value. */
		private Accumulator aggregatedValue;

		/** Whether all the tasks have committed. */
		private boolean allCommitted;

		GlobalAggregatedAccumulator(JobVertexID jobVertexId) {
			this.jobVertexId = jobVertexId;
		}

		@SuppressWarnings("unchecked")
		void commitForTasks(JobVertexID jobVertexId, Accumulator value, Set<Integer> committedTasks) {
			checkArgument(this.jobVertexId.equals(jobVertexId), "The registered task belongs to different JobVertex with previous registered ones");

			if (aggregatedValue == null) {
				aggregatedValue = value.clone();
			} else {
				aggregatedValue.merge(value);
			}

			this.committedTasks.addAll(committedTasks);
		}

		boolean isAllCommitted() {
			return allCommitted;
		}

		void markAllCommitted() {
			this.allCommitted = true;
		}

		Accumulator getAggregatedValue() {
			return aggregatedValue;
		}

		Set<Integer> getCommittedTasks() {
			return committedTasks;
		}
	}
}
