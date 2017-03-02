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
package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.Archiver;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;
import org.junit.Assert;
import org.junit.Test;

public class JobPlanHandlerTest {

	@Test
	public void testArchiver() throws Exception {
		Archiver archiver = new JobPlanHandler.JobPlanArchiver();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();

		ArchivedJson[] archives = archiver.archiveJsonWithPath(originalJob);
		Assert.assertEquals(1, archives.length);

		ArchivedJson archive = archives[0];
		Assert.assertEquals("/jobs/" + originalJob.getJobID() + "/plan", archive.getPath());
		Assert.assertEquals(originalJob.getJsonPlan(), archive.getJson());
	}

	@Test
	public void testGetPaths() {
		JobPlanHandler handler = new JobPlanHandler(null);
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/plan", paths[0]);
	}
}
