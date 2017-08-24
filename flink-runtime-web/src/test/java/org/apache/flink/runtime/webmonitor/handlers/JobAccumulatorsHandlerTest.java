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
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.mockito.Mockito.mock;

/**
 * Tests for the JobAccumulatorsHandler.
 */
public class JobAccumulatorsHandlerTest {

	@Test
	public void testArchiver() throws Exception {
		JsonArchivist archivist = new JobAccumulatorsHandler.JobAccumulatorsJsonArchivist();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(originalJob);
		Assert.assertEquals(1, archives.size());

		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals("/jobs/" + originalJob.getJobID() + "/accumulators", archive.getPath());
		compareAccumulators(originalJob, archive.getJson());
	}

	@Test
	public void testGetPaths() {
		JobAccumulatorsHandler handler = new JobAccumulatorsHandler(mock(ExecutionGraphHolder.class));
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/accumulators", paths[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		String json = JobAccumulatorsHandler.createJobAccumulatorsJson(originalJob);

		compareAccumulators(originalJob, json);
	}

	private static void compareAccumulators(AccessExecutionGraph originalJob, String json) throws IOException {
		JsonNode result = ArchivedJobGenerationUtils.MAPPER.readTree(json);

		ArrayNode accs = (ArrayNode) result.get("job-accumulators");
		Assert.assertEquals(0, accs.size());

		Assert.assertTrue(originalJob.getAccumulatorResultsStringified().length > 0);
		ArchivedJobGenerationUtils.compareStringifiedAccumulators(
			originalJob.getAccumulatorResultsStringified(),
			(ArrayNode) result.get("user-task-accumulators"));
	}
}
