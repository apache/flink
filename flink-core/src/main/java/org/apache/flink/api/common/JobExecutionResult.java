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

package org.apache.flink.api.common;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.OptionalFailure;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The result of a job execution. Gives access to the execution time of the job,
 * and to all accumulators created by this job.
 */
@Public
public class JobExecutionResult extends JobSubmissionResult {

	private final long netRuntime;

	private final Map<String, OptionalFailure<Object>> accumulatorResults;

	/**
	 * Creates a new JobExecutionResult.
	 *
	 * @param jobID The job's ID.
	 * @param netRuntime The net runtime of the job (excluding pre-flight phase like the optimizer) in milliseconds
	 * @param accumulators A map of all accumulators produced by the job.
	 */
	public JobExecutionResult(JobID jobID, long netRuntime, Map<String, OptionalFailure<Object>> accumulators) {
		super(jobID);
		this.netRuntime = netRuntime;

		if (accumulators != null) {
			this.accumulatorResults = accumulators;
		} else {
			this.accumulatorResults = Collections.emptyMap();
		}
	}

	/**
	 * Gets the net execution time of the job, i.e., the execution time in the parallel system,
	 * without the pre-flight steps like the optimizer.
	 *
	 * @return The net execution time in milliseconds.
	 */
	public long getNetRuntime() {
		return this.netRuntime;
	}

    /**
	 * Gets the net execution time of the job, i.e., the execution time in the parallel system,
	 * without the pre-flight steps like the optimizer in a desired time unit.
	 *
	 * @param desiredUnit the unit of the <tt>NetRuntime</tt>
	 * @return The net execution time in the desired unit.
	 */
	public long getNetRuntime(TimeUnit desiredUnit) {
		return desiredUnit.convert(getNetRuntime(), TimeUnit.MILLISECONDS);
	}

	/**
	 * Gets the accumulator with the given name. Returns {@code null}, if no accumulator with
	 * that name was produced.
	 *
	 * @param accumulatorName The name of the accumulator.
	 * @param <T> The generic type of the accumulator value.
	 * @return The value of the accumulator with the given name.
	 */
	@SuppressWarnings("unchecked")
	public <T> T getAccumulatorResult(String accumulatorName) {
		return (T) this.accumulatorResults.get(accumulatorName).getUnchecked();
	}

	/**
	 * Gets all accumulators produced by the job. The map contains the accumulators as
	 * mappings from the accumulator name to the accumulator value.
	 *
	 * @return A map containing all accumulators produced by the job.
	 */
	public Map<String, Object> getAllAccumulatorResults() {
		return accumulatorResults.entrySet()
			.stream()
			.collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getUnchecked()));
	}

	/**
	 * Gets the accumulator with the given name as an integer.
	 *
	 * @param accumulatorName Name of the counter
	 * @return Result of the counter, or null if the counter does not exist
	 * @throws java.lang.ClassCastException Thrown, if the accumulator was not aggregating a {@link java.lang.Integer}
	 * @deprecated Will be removed in future versions. Use {@link #getAccumulatorResult} instead.
	 */
	@Deprecated
	@PublicEvolving
	public Integer getIntCounterResult(String accumulatorName) {
		Object result = this.accumulatorResults.get(accumulatorName).getUnchecked();
		if (result == null) {
			return null;
		}
		if (!(result instanceof Integer)) {
			throw new ClassCastException("Requested result of the accumulator '" + accumulatorName
							+ "' should be Integer but has type " + result.getClass());
		}
		return (Integer) result;
	}

	/**
	 * Returns a dummy object for wrapping a JobSubmissionResult.
	 * @param result The SubmissionResult
	 * @return a JobExecutionResult
	 * @deprecated Will be removed in future versions.
	 */
	@Deprecated
	public static JobExecutionResult fromJobSubmissionResult(JobSubmissionResult result) {
		return new JobExecutionResult(result.getJobID(), -1, null);
	}
}
