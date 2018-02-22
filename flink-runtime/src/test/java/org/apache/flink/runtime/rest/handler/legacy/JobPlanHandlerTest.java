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

import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedJobGenerationUtils;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

import static org.mockito.Mockito.mock;

/**
 * Tests for the JobPlanHandler.
 */
public class JobPlanHandlerTest extends TestLogger {

	@Test
	public void testArchiver() throws Exception {
		JsonArchivist archivist = new JobPlanHandler.JobPlanJsonArchivist();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(originalJob);
		Assert.assertEquals(1, archives.size());

		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals("/jobs/" + originalJob.getJobID() + "/plan", archive.getPath());
		Assert.assertEquals(originalJob.getJsonPlan(), archive.getJson());
	}

	@Test
	public void testGetPaths() {
		JobPlanHandler handler = new JobPlanHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/plan", paths[0]);
	}
}
