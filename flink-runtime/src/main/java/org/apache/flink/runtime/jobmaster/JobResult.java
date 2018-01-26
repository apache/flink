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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Similar to {@link org.apache.flink.api.common.JobExecutionResult} but with an optional
 * {@link SerializedThrowable} when the job failed.
 *
 * <p>This is used by the {@link JobMaster} to send the results to the {@link Dispatcher}.
 */
public class JobResult implements Serializable {

	private static final long serialVersionUID = 1L;

	private final JobID jobId;

	private final Map<String, SerializedValue<Object>> accumulatorResults;

	private final long netRuntime;

	/** Stores the cause of the job failure, or {@code null} if the job finished successfully. */
	@Nullable
	private final SerializedThrowable serializedThrowable;

	private JobResult(
			final JobID jobId,
			final Map<String, SerializedValue<Object>> accumulatorResults,
			final long netRuntime,
			@Nullable final SerializedThrowable serializedThrowable) {

		checkArgument(netRuntime >= 0, "netRuntime must be greater than or equals 0");

		this.jobId = requireNonNull(jobId);
		this.accumulatorResults = requireNonNull(accumulatorResults);
		this.netRuntime = netRuntime;
		this.serializedThrowable = serializedThrowable;
	}

	/**
	 * Returns {@code true} if the job finished successfully.
	 */
	public boolean isSuccess() {
		return serializedThrowable == null;
	}

	public JobID getJobId() {
		return jobId;
	}

	public Map<String, SerializedValue<Object>> getAccumulatorResults() {
		return accumulatorResults;
	}

	public long getNetRuntime() {
		return netRuntime;
	}

	/**
	 * Returns an empty {@code Optional} if the job finished successfully, otherwise the
	 * {@code Optional} will carry the failure cause.
	 */
	public Optional<SerializedThrowable> getSerializedThrowable() {
		return Optional.ofNullable(serializedThrowable);
	}

	/**
	 * Builder for {@link JobResult}.
	 */
	@Internal
	public static class Builder {

		private JobID jobId;

		private Map<String, SerializedValue<Object>> accumulatorResults;

		private long netRuntime = -1;

		private SerializedThrowable serializedThrowable;

		public Builder jobId(final JobID jobId) {
			this.jobId = jobId;
			return this;
		}

		public Builder accumulatorResults(final Map<String, SerializedValue<Object>> accumulatorResults) {
			this.accumulatorResults = accumulatorResults;
			return this;
		}

		public Builder netRuntime(final long netRuntime) {
			this.netRuntime = netRuntime;
			return this;
		}

		public Builder serializedThrowable(final SerializedThrowable serializedThrowable) {
			this.serializedThrowable = serializedThrowable;
			return this;
		}

		public JobResult build() {
			return new JobResult(
				jobId,
				accumulatorResults == null ? Collections.emptyMap() : accumulatorResults,
				netRuntime,
				serializedThrowable);
		}
	}

	/**
	 * Creates the {@link JobResult} from the given {@link AccessExecutionGraph} which
	 * must be in a globally terminal state.
	 *
	 * @param accessExecutionGraph to create the JobResult from
	 * @return JobResult of the given AccessExecutionGraph
	 */
	public static JobResult createFrom(AccessExecutionGraph accessExecutionGraph) {
		final JobID jobId = accessExecutionGraph.getJobID();
		final JobStatus jobStatus = accessExecutionGraph.getState();

		checkArgument(
			jobStatus.isGloballyTerminalState(),
			"The job " + accessExecutionGraph.getJobName() + '(' + jobId + ") is not in a globally " +
				"terminal state. It is in state " + jobStatus + '.');

		final JobResult.Builder builder = new JobResult.Builder();
		builder.jobId(jobId);

		final long netRuntime = accessExecutionGraph.getStatusTimestamp(jobStatus) - accessExecutionGraph.getStatusTimestamp(JobStatus.CREATED);
		builder.netRuntime(netRuntime);
		builder.accumulatorResults(accessExecutionGraph.getAccumulatorsSerialized());

		if (jobStatus != JobStatus.FINISHED) {
			final ErrorInfo errorInfo = accessExecutionGraph.getFailureInfo();

			if (errorInfo != null) {
				builder.serializedThrowable(errorInfo.getException());
			}
		}

		return builder.build();
	}

}
