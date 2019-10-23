/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;

import javax.annotation.Nonnegative;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Samples the output buffer availability of tasks for back pressure tracking.
 *
 * @see org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker
 */
class TaskBackPressureSampleService {

	private final ScheduledExecutor scheduledExecutor;

	TaskBackPressureSampleService(final ScheduledExecutor scheduledExecutor) {
		this.scheduledExecutor = requireNonNull(scheduledExecutor, "The scheduledExecutor must not be null.");
	}

	/**
	 * Returns a future that completes with the back pressure ratio of a task.
	 *
	 * @param task                The task to be sampled.
	 * @param numSamples          The number of samples.
	 * @param delayBetweenSamples The time to wait between samples.
	 * @return A future containing the task back pressure ratio.
	 */
	public CompletableFuture<Double> sampleTaskBackPressure(
			final OutputAvailabilitySampleableTask task,
			@Nonnegative final int numSamples,
			final Time delayBetweenSamples) {

		checkNotNull(task, "The task must not be null.");
		checkArgument(numSamples > 0, "The numSamples must be positive.");
		checkNotNull(delayBetweenSamples, "The delayBetweenSamples must not be null.");

		return sampleTaskBackPressure(
			task,
			numSamples,
			delayBetweenSamples,
			new ArrayList<>(numSamples),
			new CompletableFuture<>());
	}

	private CompletableFuture<Double> sampleTaskBackPressure(
			final OutputAvailabilitySampleableTask task,
			final int numSamples,
			final Time delayBetweenSamples,
			final List<Boolean> taskOutputAvailability,
			final CompletableFuture<Double> resultFuture) {

		final Optional<Boolean> isTaskAvailableForOutput = isTaskAvailableForOutput(task);
		if (isTaskAvailableForOutput.isPresent()) {
			taskOutputAvailability.add(isTaskAvailableForOutput.get());
		} else if (!taskOutputAvailability.isEmpty()) {
			resultFuture.complete(calculateTaskBackPressureRatio(taskOutputAvailability));
			return resultFuture;
		} else {
			throw new IllegalStateException(String.format("Cannot sample task %s. " +
					"Because the task is not running.", task.getExecutionId()));
		}

		if (numSamples > 1) {
			scheduledExecutor.schedule(() -> sampleTaskBackPressure(
				task,
				numSamples - 1,
				delayBetweenSamples,
				taskOutputAvailability,
				resultFuture), delayBetweenSamples.getSize(), delayBetweenSamples.getUnit());
		} else {
			resultFuture.complete(calculateTaskBackPressureRatio(taskOutputAvailability));
		}
		return resultFuture;
	}

	private Optional<Boolean> isTaskAvailableForOutput(final OutputAvailabilitySampleableTask task) {
		if (!task.isRunning()) {
			return Optional.empty();
		}

		final boolean isTaskAvailableForOutput = task.isAvailableForOutput();
		return Optional.of(isTaskAvailableForOutput);
	}

	private double calculateTaskBackPressureRatio(final List<Boolean> taskOutputAvailability) {
		double unavailableCount = 0.0;
		for (Boolean isAvailable: taskOutputAvailability) {
			if (!isAvailable) {
				++unavailableCount;
			}
		}
		return taskOutputAvailability.isEmpty() ? 0.0 : unavailableCount / taskOutputAvailability.size();
	}
}
