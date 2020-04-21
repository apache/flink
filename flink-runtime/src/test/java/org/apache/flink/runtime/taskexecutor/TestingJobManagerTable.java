/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;

import javax.annotation.Nullable;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Testing implementation of {@link JobManagerTable}.
 */
public class TestingJobManagerTable implements JobManagerTable {

	private final Function<JobID, Boolean> containsFunction;
	private final BiFunction<JobID, JobManagerConnection, Boolean> putFunction;
	private final Function<JobID, JobManagerConnection> removeFunction;
	private final Function<JobID, JobManagerConnection> getFunction;

	private TestingJobManagerTable(
			Function<JobID, Boolean> containsFunction,
			BiFunction<JobID, JobManagerConnection, Boolean> putFunction,
			Function<JobID, JobManagerConnection> removeFunction,
			Function<JobID, JobManagerConnection> getFunction) {
		this.containsFunction = containsFunction;
		this.putFunction = putFunction;
		this.removeFunction = removeFunction;
		this.getFunction = getFunction;
	}

	@Override
	public boolean contains(JobID jobId) {
		return containsFunction.apply(jobId);
	}

	@Override
	public boolean put(JobID jobId, JobManagerConnection jobManagerConnection) {
		return putFunction.apply(jobId, jobManagerConnection);
	}

	@Nullable
	@Override
	public JobManagerConnection remove(JobID jobId) {
		return removeFunction.apply(jobId);
	}

	@Nullable
	@Override
	public JobManagerConnection get(JobID jobId) {
		return getFunction.apply(jobId);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static final class Builder {
		private Function<JobID, Boolean> containsFunction = (ignored) -> false;
		private BiFunction<JobID, JobManagerConnection, Boolean> putFunction = (ignoredA, ignoredB) -> false;
		private Function<JobID, JobManagerConnection> removeFunction = (ignored) -> null;
		private Function<JobID, JobManagerConnection> getFunction = (ignored) -> null;

		private Builder() {}

		public Builder setContainsFunction(Function<JobID, Boolean> containsFunction) {
			this.containsFunction = containsFunction;
			return this;
		}

		public Builder setPutFunction(BiFunction<JobID, JobManagerConnection, Boolean> putFunction) {
			this.putFunction = putFunction;
			return this;
		}

		public Builder setRemoveFunction(Function<JobID, JobManagerConnection> removeFunction) {
			this.removeFunction = removeFunction;
			return this;
		}

		public Builder setGetFunction(Function<JobID, JobManagerConnection> getFunction) {
			this.getFunction = getFunction;
			return this;
		}

		public TestingJobManagerTable build() {
			return new TestingJobManagerTable(containsFunction, putFunction, removeFunction, getFunction);
		}
	}
}
