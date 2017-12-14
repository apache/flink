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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.types.Either;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import javax.annotation.Nullable;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Caches {@link SerializedJobExecutionResult}s.
 *
 * @see org.apache.flink.runtime.rest.handler.job.JobExecutionResultHandler
 */
class JobExecutionResultCache {

	private static final int MAX_RESULT_CACHE_DURATION_SECONDS = 300;

	private final Cache<JobID, Either<Throwable, SerializedJobExecutionResult>>
		jobExecutionResultCache =
		CacheBuilder.newBuilder()
			.expireAfterWrite(MAX_RESULT_CACHE_DURATION_SECONDS, TimeUnit.SECONDS)
			.build();

	public void put(final SerializedJobExecutionResult result) {
		assertJobExecutionResultNotCached(result.getJobId());
		jobExecutionResultCache.put(result.getJobId(), Either.Right(result));
	}

	public void put(final JobID jobId, Throwable throwable) {
		assertJobExecutionResultNotCached(jobId);
		jobExecutionResultCache.put(jobId, Either.Left(throwable));
	}

	public boolean contains(final JobID jobId) {
		return jobExecutionResultCache.getIfPresent(jobId) != null;
	}

	@Nullable
	public Either<Throwable, SerializedJobExecutionResult> get(final JobID jobId) {
		final Either<Throwable, SerializedJobExecutionResult> jobExecutionResult =
			jobExecutionResultCache.getIfPresent(jobId);
		jobExecutionResultCache.invalidate(jobId);
		return jobExecutionResult;
	}

	private void assertJobExecutionResultNotCached(final JobID jobId) {
		final Either<Throwable, SerializedJobExecutionResult> executionResult =
			jobExecutionResultCache.getIfPresent(jobId);
		checkState(
			executionResult == null,
			"jobExecutionResultCache already contained entry for job " + jobId);
	}

}
