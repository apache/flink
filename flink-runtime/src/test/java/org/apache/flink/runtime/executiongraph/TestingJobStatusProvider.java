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

import org.apache.flink.api.common.JobStatus;

import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link JobStatusProvider} implementation for testing purposes.
 */
public class TestingJobStatusProvider implements JobStatusProvider {

	private final Supplier<JobStatus> statusSupplier;
	private final Function<JobStatus, Long> statusTimestampRetriever;

	public TestingJobStatusProvider(Supplier<JobStatus> statusSupplier, Function<JobStatus, Long> statusTimestampRetriever) {
		this.statusSupplier = checkNotNull(statusSupplier);
		this.statusTimestampRetriever = checkNotNull(statusTimestampRetriever);
	}

	public TestingJobStatusProvider(JobStatus state, Function<JobStatus, Long> statusTimestampRetriever) {
		this.statusSupplier = () -> checkNotNull(state);
		this.statusTimestampRetriever = checkNotNull(statusTimestampRetriever);
	}

	public TestingJobStatusProvider(JobStatus state, long statusTimestamp) {
		this.statusSupplier = () -> checkNotNull(state);
		this.statusTimestampRetriever = s -> statusTimestamp;
	}

	@Override
	public JobStatus getState() {
		return statusSupplier.get();
	}

	@Override
	public long getStatusTimestamp(JobStatus status) {
		return statusTimestampRetriever.apply(status);
	}
}
