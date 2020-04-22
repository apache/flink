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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.function.TriFunction;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Testing implementation of {@link JobManagerTable}.
 */
public class TestingJobManagerTable implements JobManagerTable {

	private final TriFunction<JobID, ResourceID, JobManagerConnection, Boolean> putFunction;
	private final Function<JobID, Boolean> containsJobIdFunction;
	private final Function<JobID, JobManagerConnection> removeJobIdFunction;
	private final Function<JobID, JobManagerConnection> getJobIdFunction;
	private final Function<ResourceID, Boolean> containsResourceIdFunction;
	private final Function<ResourceID, JobManagerConnection> getResourceIdFunction;
	private final Supplier<Collection<JobManagerConnection>> valuesSupplier;

	private TestingJobManagerTable(
			Function<JobID, Boolean> containsJobIdFunction,
			TriFunction<JobID, ResourceID, JobManagerConnection, Boolean> putFunction,
			Function<JobID, JobManagerConnection> removeJobIdFunction,
			Function<JobID, JobManagerConnection> getJobIdFunction,
			Function<ResourceID, Boolean> containsResourceIdFunction,
			Function<ResourceID, JobManagerConnection> getResourceIdFunction,
			Supplier<Collection<JobManagerConnection>> valuesSupplier) {
		this.containsJobIdFunction = containsJobIdFunction;
		this.putFunction = putFunction;
		this.removeJobIdFunction = removeJobIdFunction;
		this.getJobIdFunction = getJobIdFunction;
		this.containsResourceIdFunction = containsResourceIdFunction;
		this.getResourceIdFunction = getResourceIdFunction;
		this.valuesSupplier = valuesSupplier;
	}

	@Override
	public boolean contains(JobID jobId) {
		return containsJobIdFunction.apply(jobId);
	}

	@Override
	public boolean contains(ResourceID resourceId) {
		return containsResourceIdFunction.apply(resourceId);
	}

	@Override
	public boolean put(JobID jobId, ResourceID resourceId, JobManagerConnection jobManagerConnection) {
		return putFunction.apply(jobId, resourceId, jobManagerConnection);
	}

	@Nullable
	@Override
	public JobManagerConnection remove(JobID jobId) {
		return removeJobIdFunction.apply(jobId);
	}

	@Nullable
	@Override
	public JobManagerConnection get(JobID jobId) {
		return getJobIdFunction.apply(jobId);
	}

	@Nullable
	@Override
	public JobManagerConnection get(ResourceID resourceId) {
		return getResourceIdFunction.apply(resourceId);
	}

	@Override
	public Collection<JobManagerConnection> values() {
		return valuesSupplier.get();
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static final class Builder {
		private TriFunction<JobID, ResourceID, JobManagerConnection, Boolean> putFunction = (ignoredA, ignoredB, ignoredC) -> false;
		private Function<JobID, Boolean> containsJobIdFunction = (ignored) -> false;
		private Function<JobID, JobManagerConnection> removeJobIdFunction = (ignored) -> null;
		private Function<JobID, JobManagerConnection> getJobIdFunction = (ignored) -> null;
		private Function<ResourceID, Boolean> containsResourceIdFunction = (ignored) -> false;
		private Function<ResourceID, JobManagerConnection> getResourceIdFunction = (ignored) -> null;
		private Supplier<Collection<JobManagerConnection>> valuesSupplier = () -> Collections.emptyList();

		private Builder() {}

		public Builder setContainsJobIdFunction(Function<JobID, Boolean> containsJobIdFunction) {
			this.containsJobIdFunction = containsJobIdFunction;
			return this;
		}

		public Builder setPutFunction(TriFunction<JobID, ResourceID, JobManagerConnection, Boolean> putFunction) {
			this.putFunction = putFunction;
			return this;
		}

		public Builder setRemoveJobIdFunction(Function<JobID, JobManagerConnection> removeJobIdFunction) {
			this.removeJobIdFunction = removeJobIdFunction;
			return this;
		}

		public Builder setGetJobIdFunction(Function<JobID, JobManagerConnection> getJobIdFunction) {
			this.getJobIdFunction = getJobIdFunction;
			return this;
		}

		public TestingJobManagerTable build() {
			return new TestingJobManagerTable(
				containsJobIdFunction,
				putFunction,
				removeJobIdFunction,
				getJobIdFunction,
				containsResourceIdFunction,
				getResourceIdFunction,
				valuesSupplier);
		}
	}
}
