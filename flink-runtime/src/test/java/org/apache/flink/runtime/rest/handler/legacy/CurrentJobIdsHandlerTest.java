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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CurrentJobIdsHandler}.
 */
public class CurrentJobIdsHandlerTest extends TestLogger {

	private CurrentJobIdsHandler currentJobIdsHandler;

	@Mock
	private JobManagerGateway mockJobManagerGateway;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
		currentJobIdsHandler = new CurrentJobIdsHandler(Executors.directExecutor(), Time.seconds(0L));
	}

	@Test
	public void testGetPaths() {
		final String[] paths = currentJobIdsHandler.getPaths();
		assertEquals(1, paths.length);
		assertEquals("/jobs", paths[0]);
	}

	@Test
	public void testHandleJsonRequest() throws Exception {
		final JobID jobId = new JobID();
		final JobStatus jobStatus = JobStatus.RUNNING;

		when(mockJobManagerGateway.requestJobsOverview(any(Time.class))).thenReturn(
			CompletableFuture.completedFuture(new JobIdsWithStatusOverview(Collections.singleton(
				new JobIdsWithStatusOverview.JobIdWithStatus(jobId, jobStatus)))));

		final CompletableFuture<String> jsonFuture = currentJobIdsHandler.handleJsonRequest(
			Collections.emptyMap(),
			Collections.emptyMap(),
			mockJobManagerGateway);

		final String json = jsonFuture.get();

		assertThat(json, containsString(jobId.toString()));
		assertThat(json, containsString(jobStatus.name()));
	}

}
