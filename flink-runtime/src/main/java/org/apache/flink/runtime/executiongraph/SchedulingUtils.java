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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.FutureUtils.ConjunctFuture;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.slotpool.Scheduler;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class contains scheduling logic for EAGER and LAZY_FROM_SOURCES.
 * It is used for normal scheduling and legacy failover strategy re-scheduling.
 */
public class SchedulingUtils {

	public static CompletableFuture<Void> schedule(
			ScheduleMode scheduleMode,
			final Iterable<ExecutionVertex> vertices,
			final ExecutionGraph executionGraph) {

		switch (scheduleMode) {
			case LAZY_FROM_SOURCES:
			case LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST:
				return scheduleLazy(vertices, executionGraph);

			case EAGER:
				return scheduleEager(vertices, executionGraph);

			default:
				throw new IllegalStateException(String.format("Schedule mode %s is invalid.", scheduleMode));
		}
	}

	/**
	 * Schedule vertices lazy. That means only vertices satisfying its input constraint will be scheduled.
	 *
	 * @param vertices Topologically sorted vertices to schedule.
	 * @param executionGraph The graph the given vertices belong to.
	 */
	public static CompletableFuture<Void> scheduleLazy(
			final Iterable<ExecutionVertex> vertices,
			final ExecutionGraph executionGraph) {

		executionGraph.assertRunningInJobMasterMainThread();

		final SlotProviderStrategy slotProviderStrategy = executionGraph.getSlotProviderStrategy();
		final Set<AllocationID> previousAllocations = computePriorAllocationIdsIfRequiredByScheduling(
			vertices, slotProviderStrategy.asSlotProvider());

		final ArrayList<CompletableFuture<Void>> schedulingFutures = new ArrayList<>();
		for (ExecutionVertex executionVertex : vertices) {
			// only schedule vertex when its input constraint is satisfied
			if (executionVertex.getJobVertex().getJobVertex().isInputVertex() ||
				executionVertex.checkInputDependencyConstraints()) {

				final CompletableFuture<Void> schedulingVertexFuture = executionVertex.scheduleForExecution(
					slotProviderStrategy,
					LocationPreferenceConstraint.ANY,
					previousAllocations);

				schedulingFutures.add(schedulingVertexFuture);
			}
		}

		return FutureUtils.waitForAll(schedulingFutures);
	}

	/**
	 * Schedule vertices eagerly. That means all vertices will be scheduled at once.
	 *
	 * @param vertices Topologically sorted vertices to schedule.
	 * @param executionGraph The graph the given vertices belong to.
	 */
	public static CompletableFuture<Void> scheduleEager(
			final Iterable<ExecutionVertex> vertices,
			final ExecutionGraph executionGraph) {

		executionGraph.assertRunningInJobMasterMainThread();

		checkState(executionGraph.getState() == JobStatus.RUNNING, "job is not running currently");

		// Important: reserve all the space we need up front.
		// that way we do not have any operation that can fail between allocating the slots
		// and adding them to the list. If we had a failure in between there, that would
		// cause the slots to get lost

		// collecting all the slots may resize and fail in that operation without slots getting lost
		final ArrayList<CompletableFuture<Execution>> allAllocationFutures = new ArrayList<>();

		final SlotProviderStrategy slotProviderStrategy = executionGraph.getSlotProviderStrategy();
		final Set<AllocationID> allPreviousAllocationIds = Collections.unmodifiableSet(
			computePriorAllocationIdsIfRequiredByScheduling(vertices, slotProviderStrategy.asSlotProvider()));

		// allocate the slots (obtain all their futures)
		for (ExecutionVertex ev : vertices) {
			// these calls are not blocking, they only return futures
			CompletableFuture<Execution> allocationFuture = ev.getCurrentExecutionAttempt().allocateResourcesForExecution(
				slotProviderStrategy,
				LocationPreferenceConstraint.ALL,
				allPreviousAllocationIds);

			allAllocationFutures.add(allocationFuture);
		}

		// this future is complete once all slot futures are complete.
		// the future fails once one slot future fails.
		final ConjunctFuture<Collection<Execution>> allAllocationsFuture = FutureUtils.combineAll(allAllocationFutures);

		return allAllocationsFuture.thenAccept(
			(Collection<Execution> executionsToDeploy) -> {
				for (Execution execution : executionsToDeploy) {
					try {
						execution.deploy();
					} catch (Throwable t) {
						throw new CompletionException(
							new FlinkException(
								String.format("Could not deploy execution %s.", execution),
								t));
					}
				}
			})
			// Generate a more specific failure message for the eager scheduling
			.exceptionally(
				(Throwable throwable) -> {
					final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
					final Throwable resultThrowable;
					if (strippedThrowable instanceof TimeoutException) {
						int numTotal = allAllocationsFuture.getNumFuturesTotal();
						int numComplete = allAllocationsFuture.getNumFuturesCompleted();

						String message = "Could not allocate all requires slots within timeout of "
							+ executionGraph.getAllocationTimeout() + ". Slots required: "
							+ numTotal + ", slots allocated: " + numComplete
							+ ", previous allocation IDs: " + allPreviousAllocationIds;

						StringBuilder executionMessageBuilder = new StringBuilder();

						for (int i = 0; i < allAllocationFutures.size(); i++) {
							CompletableFuture<Execution> executionFuture = allAllocationFutures.get(i);

							try {
								Execution execution = executionFuture.getNow(null);
								if (execution != null) {
									executionMessageBuilder.append("completed: " + execution);
								} else {
									executionMessageBuilder.append("incomplete: " + executionFuture);
								}
							} catch (CompletionException completionException) {
								executionMessageBuilder.append("completed exceptionally: "
									+ completionException + "/" + executionFuture);
							}

							if (i < allAllocationFutures.size() - 1) {
								executionMessageBuilder.append(", ");
							}
						}

						message += ", execution status: " + executionMessageBuilder.toString();

						resultThrowable = new NoResourceAvailableException(message);
					} else {
						resultThrowable = strippedThrowable;
					}

					throw new CompletionException(resultThrowable);
				});
	}

	/**
	 * Returns the result of {@link #computePriorAllocationIds(Iterable)},
	 * but only if the scheduling really requires it.
	 * Otherwise this method simply returns an empty set.
	 */
	private static Set<AllocationID> computePriorAllocationIdsIfRequiredByScheduling(
			final Iterable<ExecutionVertex> vertices,
			final SlotProvider slotProvider) {
		// This is a temporary optimization to avoid computing all previous allocations if not required
		// This can go away when we progress with the implementation of the Scheduler.
		if (slotProvider instanceof Scheduler &&
			((Scheduler) slotProvider).requiresPreviousExecutionGraphAllocations()) {

			return computePriorAllocationIds(vertices);
		} else {
			return Collections.emptySet();
		}
	}

	/**
	 * Computes and returns a set with the prior allocation ids for given execution vertices.
	 */
	private static Set<AllocationID> computePriorAllocationIds(final Iterable<ExecutionVertex> vertices) {
		HashSet<AllocationID> allPreviousAllocationIds = new HashSet<>();
		for (ExecutionVertex executionVertex : vertices) {
			AllocationID latestPriorAllocation = executionVertex.getLatestPriorAllocation();
			if (latestPriorAllocation != null) {
				allPreviousAllocationIds.add(latestPriorAllocation);
			}
		}
		return allPreviousAllocationIds;
	}
}
