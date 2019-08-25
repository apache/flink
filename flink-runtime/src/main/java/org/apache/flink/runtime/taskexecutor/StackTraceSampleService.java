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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Samples the stack traces of tasks for back pressure tracking.
 *
 * @see org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker
 */
class StackTraceSampleService {

	private final ScheduledExecutor scheduledExecutor;

	StackTraceSampleService(final ScheduledExecutor scheduledExecutor) {
		this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor must not be null");
	}

	/**
	 * Returns a future that completes with a given number of stack trace samples of a task thread.
	 *
	 * @param task                The task to be sampled from.
	 * @param numSamples          The number of samples.
	 * @param delayBetweenSamples The time to wait between taking samples.
	 * @param maxStackTraceDepth  The maximum depth of the returned stack traces.
	 *                            Negative means unlimited.
	 * @return A future containing the stack trace samples.
	 */
	public CompletableFuture<List<StackTraceElement[]>> requestStackTraceSample(
			final StackTraceSampleableTask task,
			@Nonnegative final int numSamples,
			final Time delayBetweenSamples,
			final int maxStackTraceDepth) {

		checkNotNull(task, "task must not be null");
		checkArgument(numSamples > 0, "numSamples must be positive");
		checkNotNull(delayBetweenSamples, "delayBetweenSamples must not be null");

		return requestStackTraceSample(
			task,
			numSamples,
			delayBetweenSamples,
			maxStackTraceDepth,
			new ArrayList<>(numSamples),
			new CompletableFuture<>());
	}

	private CompletableFuture<List<StackTraceElement[]>> requestStackTraceSample(
			final StackTraceSampleableTask task,
			final int numSamples,
			final Time delayBetweenSamples,
			final int maxStackTraceDepth,
			final List<StackTraceElement[]> currentTraces,
			final CompletableFuture<List<StackTraceElement[]>> resultFuture) {

		final Optional<StackTraceElement[]> stackTrace = getStackTrace(task, maxStackTraceDepth);
		if (stackTrace.isPresent()) {
			currentTraces.add(stackTrace.get());
		} else if (!currentTraces.isEmpty()) {
			resultFuture.complete(currentTraces);
			return resultFuture;
		} else {
			throw new IllegalStateException(String.format("Cannot sample task %s. " +
					"The task is not running.",
				task.getExecutionId()));
		}

		if (numSamples > 1) {
			scheduledExecutor.schedule(() -> requestStackTraceSample(
				task,
				numSamples - 1,
				delayBetweenSamples,
				maxStackTraceDepth,
				currentTraces,
				resultFuture), delayBetweenSamples.getSize(), delayBetweenSamples.getUnit());
		} else {
			resultFuture.complete(currentTraces);
		}
		return resultFuture;
	}

	private Optional<StackTraceElement[]> getStackTrace(final StackTraceSampleableTask task, final int maxStackTraceDepth) {
		if (!task.isRunning()) {
			return Optional.empty();
		}

		final StackTraceElement[] stackTrace = task.getStackTrace();
		return Optional.of(truncateStackTrace(stackTrace, maxStackTraceDepth));
	}

	private StackTraceElement[] truncateStackTrace(final StackTraceElement[] stackTrace, final int maxStackTraceDepth) {
		if (maxStackTraceDepth > 0) {
			return Arrays.copyOfRange(stackTrace, 0, Math.min(maxStackTraceDepth, stackTrace.length));
		}
		return stackTrace;
	}
}
