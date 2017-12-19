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
import org.apache.flink.testutils.category.Flip6;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.ref.SoftReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link JobExecutionResultCache}.
 */
@Category(Flip6.class)
public class JobExecutionResultCacheTest extends TestLogger {

	private JobExecutionResultCache jobExecutionResultCache;

	@Before
	public void setUp() {
		jobExecutionResultCache = new JobExecutionResultCache();
	}

	@Test
	public void testCacheResultUntilRetrieved() {
		final JobID jobId = new JobID();
		final JobResult jobResult = new JobResult.Builder()
			.jobId(jobId)
			.netRuntime(Long.MAX_VALUE)
			.build();
		jobExecutionResultCache.put(jobResult);

		assertThat(jobExecutionResultCache.contains(jobId), equalTo(true));

		SoftReference<JobResult> jobResultRef;
		jobResultRef = jobExecutionResultCache.get(jobId);

		assertThat(jobResultRef, notNullValue());
		assertThat(jobResultRef.get(), sameInstance(jobResult));

		assertThat(jobExecutionResultCache.contains(jobId), equalTo(false));

		jobResultRef = jobExecutionResultCache.get(jobId);
		assertThat(jobResultRef, nullValue());
	}

	@Test
	public void testThrowExceptionIfEntryAlreadyExists() {
		final JobID jobId = new JobID();
		final JobResult build = new JobResult.Builder()
			.jobId(jobId)
			.netRuntime(Long.MAX_VALUE)
			.build();
		jobExecutionResultCache.put(build);

		try {
			jobExecutionResultCache.put(build);
			fail("Expected exception not thrown.");
		} catch (final IllegalStateException e) {
			assertThat(e.getMessage(), containsString("already contained entry for job"));
		}
	}

}
