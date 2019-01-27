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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskexecutor.JobManagerConnection;
import org.apache.flink.runtime.taskexecutor.JobManagerTable;

import javax.annotation.concurrent.GuardedBy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A specialized implementation based on the FLIP-6 architecture.
 */
public class RPCBasedAccumulatorAggregationManager implements AccumulatorAggregationManager {
	private final JobManagerTable jobManagerTable;

	/** The accumulators registered or committed by the tasks running on this executor. */
	private final Map<JobID, Map<String, AggregatedAccumulator>> perJobAggregatedAccumulators = new HashMap<>();

	private final Object queryLock = new Object();

	/** The unfulfilled query futures, which will be completed once the query to the job master finishes. */
	@GuardedBy("queryLock")
	private final Map<JobID, Map<String, List<CompletableFuture<Accumulator>>>> perJobUnfulfilledUserQueryFutures = new HashMap<>();

	/** If the query has completed, the aggregated accumulator or exceptions caught are cached. */
	@GuardedBy("queryLock")
	private final Map<JobID, Map<String, Object>> perJobCachedQueryResults = new HashMap<>();

	public RPCBasedAccumulatorAggregationManager(JobManagerTable jobManagerTable) {
		this.jobManagerTable = jobManagerTable;
	}

	@Override
	public void registerPreAggregatedAccumulator(JobID jobId, JobVertexID jobVertexId, int subtaskIndex, String name) {
		synchronized (perJobAggregatedAccumulators) {
			AggregatedAccumulator aggregatedAccumulator = perJobAggregatedAccumulators.computeIfAbsent(jobId, k -> new HashMap<>())
				.computeIfAbsent(name, k -> new AggregatedAccumulator(jobVertexId));
			aggregatedAccumulator.registerForTask(jobVertexId, subtaskIndex);
		}
	}

	@Override
	public void commitPreAggregatedAccumulator(JobID jobId, JobVertexID jobVertexId, int subtaskIndex, String name, Accumulator value) {
		synchronized (perJobAggregatedAccumulators) {
			Map<String, AggregatedAccumulator> currentJobAccumulators = perJobAggregatedAccumulators.get(jobId);
			AggregatedAccumulator aggregatedAccumulator = (currentJobAccumulators != null ? currentJobAccumulators.get(name) : null);

			checkState(aggregatedAccumulator != null, "The committed accumulator does not exist.");

			aggregatedAccumulator.commitForTask(jobVertexId, subtaskIndex, value);

			if (aggregatedAccumulator.isAllCommitted()) {
				commitAggregatedAccumulators(jobId,
					Collections.singletonList(new CommitAccumulator(aggregatedAccumulator.getJobVertexId(),
						name,
						aggregatedAccumulator.getAggregatedValue(),
						aggregatedAccumulator.getCommittedTasks())));

				// Remove the accumulator no matter whether its value is reported to JobMaster.
				currentJobAccumulators.remove(name);
			}

			if (currentJobAccumulators.isEmpty()) {
				perJobAggregatedAccumulators.remove(jobId);
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <V, A extends Serializable> CompletableFuture<Accumulator<V, A>> queryPreAggregatedAccumulator(JobID jobId, String name) {
		synchronized (queryLock) {
			CompletableFuture<Accumulator<V, A>> localQueryFuture = new CompletableFuture<>();

			Map<String, Object> currentJobCachedQueryResults = perJobCachedQueryResults.get(jobId);
			Object cachedQueryResult = (currentJobCachedQueryResults == null ? null : currentJobCachedQueryResults.get(name));

			if (cachedQueryResult == null) {
				boolean hasQueriedJobMaster = perJobUnfulfilledUserQueryFutures.containsKey(jobId) &&
					perJobUnfulfilledUserQueryFutures.get(jobId).containsKey(name);

				if (!hasQueriedJobMaster) {
					JobManagerConnection connection = jobManagerTable.get(jobId);
					if (connection == null) {
						localQueryFuture.complete(null);
						return localQueryFuture;
					}

					perJobUnfulfilledUserQueryFutures.computeIfAbsent(jobId, k -> new HashMap<>())
						.computeIfAbsent(name, k -> new ArrayList<>())
						.add((CompletableFuture) localQueryFuture);

					CompletableFuture<Accumulator<V, A>> queryToJobMaster =
						connection.getJobManagerGateway().queryPreAggregatedAccumulator(name);

					// If the queried accumulator has already been prepared, the callback will be called directly
					// in current thread, otherwise it will be executed by the RPC main thread later.
					queryToJobMaster.whenComplete(((accumulator, throwable) -> {
						onAccumulatorQueryFinished(jobId, name, accumulator, throwable);
					}));
				} else {
					perJobUnfulfilledUserQueryFutures.get(jobId).get(name).add((CompletableFuture) localQueryFuture);
				}

				return localQueryFuture;
			}

			if (cachedQueryResult instanceof Accumulator) {
				localQueryFuture.complete((Accumulator<V, A>) cachedQueryResult);
				return localQueryFuture;
			} else if (cachedQueryResult instanceof Throwable) {
				localQueryFuture.completeExceptionally((Throwable) cachedQueryResult);
				return localQueryFuture;
			} else {
				throw new IllegalStateException("The cached result should be either accumulator or throwable.");
			}
		}
	}

	@Override
	public void clearRegistrationForTask(JobID jobId, JobVertexID jobVertexId, int subtaskIndex) {
		synchronized (perJobAggregatedAccumulators) {
			Map<String, AggregatedAccumulator> currentJobAccumulators = perJobAggregatedAccumulators.get(jobId);

			if (currentJobAccumulators != null) {
				List<CommitAccumulator> commitAccumulators = new ArrayList<>();
				List<String> shouldRemove = new ArrayList<>();

				for (Map.Entry<String, AggregatedAccumulator> entry : currentJobAccumulators.entrySet()) {
					AggregatedAccumulator aggregatedAccumulator = entry.getValue();

					if (!aggregatedAccumulator.getJobVertexId().equals(jobVertexId)) {
						continue;
					}

					aggregatedAccumulator.clearRegistrationForTask(subtaskIndex);

					if (aggregatedAccumulator.isAllCommitted()) {
						commitAccumulators.add(new CommitAccumulator(entry.getValue().getJobVertexId(),
							entry.getKey(),
							entry.getValue().getAggregatedValue(),
							entry.getValue().getCommittedTasks()));
					}

					if (aggregatedAccumulator.isAllCommitted() || aggregatedAccumulator.isEmpty()) {
						shouldRemove.add(entry.getKey());
					}
				}

				if (commitAccumulators.size() > 0) {
					commitAggregatedAccumulators(jobId, commitAccumulators);
				}

				for (String name : shouldRemove) {
					currentJobAccumulators.remove(name);
				}

				if (currentJobAccumulators.isEmpty()) {
					perJobAggregatedAccumulators.remove(jobId);
				}
			}
		}
	}

	@Override
	public void clearAccumulatorsForJob(JobID jobId) {
		synchronized (perJobAggregatedAccumulators) {
			perJobAggregatedAccumulators.computeIfPresent(jobId, (k, currentJobAccumulators) -> {
				currentJobAccumulators.clear();
				return null;
			});
		}

		synchronized (queryLock) {
			perJobUnfulfilledUserQueryFutures.computeIfPresent(jobId, (k, currentJobQueryFutures) -> {
				currentJobQueryFutures.clear();
				return null;
			});

			perJobCachedQueryResults.computeIfPresent(jobId, (k, currentJobCachedResults) -> {
				currentJobCachedResults.clear();
				return null;
			});
		}
	}

	@VisibleForTesting
	Map<JobID, Map<String, AggregatedAccumulator>> getPerJobAggregatedAccumulators() {
		return perJobAggregatedAccumulators;
	}

	@VisibleForTesting
	public Map<JobID, Map<String, List<CompletableFuture<Accumulator>>>> getPerJobUnfulfilledUserQueryFutures() {
		return perJobUnfulfilledUserQueryFutures;
	}

	@VisibleForTesting
	public Map<JobID, Map<String, Object>> getPerJobCachedQueryResults() {
		return perJobCachedQueryResults;
	}

	private void commitAggregatedAccumulators(JobID jobId, List<CommitAccumulator> accumulators) {
		assert Thread.holdsLock(perJobAggregatedAccumulators);

		JobManagerConnection connection = jobManagerTable.get(jobId);
		if (connection != null) {
			connection.getJobManagerGateway().commitPreAggregatedAccumulator(accumulators);
		}
	}

	@SuppressWarnings("unchecked")
	private <V, A extends Serializable> void onAccumulatorQueryFinished(JobID jobId, String name, Accumulator<V, A> accumulator, Throwable throwable) {
		synchronized (queryLock) {
			checkState(!perJobCachedQueryResults.containsKey(jobId) || !perJobCachedQueryResults.get(jobId).containsKey(name),
				"The target accumulator " + name + " of job " + jobId + " should not be in the cached result list.");

			checkState(perJobUnfulfilledUserQueryFutures.containsKey(jobId) && perJobUnfulfilledUserQueryFutures.get(jobId).containsKey(name),
				"The target accumulator should reside in the unfulfilled query map.");

			Map<String, Object> currentJobCacheQueryResults = perJobCachedQueryResults.computeIfAbsent(jobId, k -> new HashMap<>());

			List<CompletableFuture<Accumulator>> userQueries = perJobUnfulfilledUserQueryFutures.get(jobId).get(name);

			if (accumulator != null) {
				userQueries.forEach(userQuery -> {
					userQuery.complete(accumulator);
				});
				currentJobCacheQueryResults.put(name, accumulator);
			} else {
				userQueries.forEach(userQuery -> {
					userQuery.completeExceptionally(throwable);
				});
				currentJobCacheQueryResults.put(name, throwable);
			}

			// Remove the unused list and map.
			perJobUnfulfilledUserQueryFutures.compute(jobId, (k, currentJobUserQueryFutures) -> {
				currentJobUserQueryFutures.remove(name);
				return currentJobUserQueryFutures.size() == 0 ? null : currentJobUserQueryFutures;
			});
		}
	}

	/**
	 * The wrapper class for an accumulator, which manages its registered tasks and committed tasks.
	 */
	static final class AggregatedAccumulator {
		/** The set of tasks who have registered. */
		private final Set<Integer> registeredTasks = new HashSet<>();

		/** The set of tasks who have committed. */
		private final Set<Integer> committedTasks = new HashSet<>();

		/** The JobVertexID this accumulator belongs to. */
		private final JobVertexID jobVertexId;

		/** The currently aggregated value. */
		private Accumulator aggregatedValue;

		AggregatedAccumulator(JobVertexID jobVertexId) {
			this.jobVertexId = jobVertexId;
		}

		void registerForTask(JobVertexID jobVertexId, int subtaskIndex) {
			checkArgument(this.jobVertexId.equals(jobVertexId),
				"The registered task belongs to different JobVertex with previous registered ones");

			checkState(!registeredTasks.contains(subtaskIndex), "This task has already registered.");

			registeredTasks.add(subtaskIndex);
		}

		@SuppressWarnings("unchecked")
		void commitForTask(JobVertexID jobVertexId, int subtaskIndex, Accumulator value) {
			checkArgument(this.jobVertexId.equals(jobVertexId),
				"The registered task belongs to different JobVertex with previous registered ones");
			checkState(registeredTasks.contains(subtaskIndex), "Can not commit for an accumulator that has " +
				"not been registered before");

			if (aggregatedValue == null) {
				aggregatedValue = value.clone();
			} else {
				aggregatedValue.merge(value);
			}

			committedTasks.add(subtaskIndex);
		}

		void clearRegistrationForTask(int subtaskIndex) {
			if (registeredTasks.contains(subtaskIndex) && !committedTasks.contains(subtaskIndex)) {
				registeredTasks.remove(subtaskIndex);
			}
		}

		boolean isEmpty() {
			return registeredTasks.size() == 0;
		}

		boolean isAllCommitted() {
			return registeredTasks.size() > 0 && registeredTasks.size() == committedTasks.size();
		}

		Accumulator getAggregatedValue() {
			return aggregatedValue;
		}

		JobVertexID getJobVertexId() {
			return jobVertexId;
		}

		Set<Integer> getCommittedTasks() {
			return committedTasks;
		}

		@VisibleForTesting
		Set<Integer> getRegisteredTasks() {
			return registeredTasks;
		}
	}
}
