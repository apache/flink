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
import org.apache.flink.runtime.jobmaster.JobResult;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import javax.annotation.Nullable;

import java.lang.ref.SoftReference;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Caches {@link JobResult}s by their job id.
 *
 * <p>Entries are cached for a finite time. However, the JobResults are wrapped in
 * {@link SoftReference}s so that the GC can free them according to memory demand.
 */
class JobExecutionResultCache {

	private static final int MAX_RESULT_CACHE_DURATION_SECONDS = 300;

	private final Cache<JobID, SoftReference<JobResult>>
		jobExecutionResultCache =
		CacheBuilder.newBuilder()
			.expireAfterWrite(MAX_RESULT_CACHE_DURATION_SECONDS, TimeUnit.SECONDS)
			.build();

	/**
	 * Adds a {@link JobResult} to the cache.
	 *
	 * @param result The entry to be added to the cache.
	 */
	public void put(final JobResult result) {
		assertJobExecutionResultNotCached(result.getJobId());
		jobExecutionResultCache.put(result.getJobId(), new SoftReference<>(result));
	}

	/**
	 * Returns {@code true} if the cache contains a {@link JobResult} for the specified
	 * {@link JobID}.
	 *
	 * @param jobId The job id for which the presence of the {@link JobResult} should be tested.
	 * @return {@code true} if the cache contains an entry, {@code false} otherwise
	 */
	public boolean contains(final JobID jobId) {
		return jobExecutionResultCache.getIfPresent(jobId) != null;
	}

	/**
	 * Returns a {@link SoftReference} to the {@link JobResult} for the specified job, and removes
	 * the entry from the cache.
	 *
	 * @param jobId The job id of the {@link JobResult}.
	 * @return A {@link SoftReference} to the {@link JobResult} for the job, or {@code null} if the
	 * entry cannot be found in the cache.
	 */
	@Nullable
	public SoftReference<JobResult> get(final JobID jobId) {
		final SoftReference<JobResult> jobResultRef = jobExecutionResultCache.getIfPresent(jobId);
		jobExecutionResultCache.invalidate(jobId);
		return jobResultRef;
	}

	private void assertJobExecutionResultNotCached(final JobID jobId) {
		checkState(
			jobExecutionResultCache.getIfPresent(jobId) == null,
			"jobExecutionResultCache already contained entry for job %s", jobId);
	}

}
