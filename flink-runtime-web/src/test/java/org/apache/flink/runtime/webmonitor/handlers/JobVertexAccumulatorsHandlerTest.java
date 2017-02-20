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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.Archiver;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class JobVertexAccumulatorsHandlerTest {

	@Test
	public void testArchiver() throws Exception {
		Archiver archiver = new JobVertexAccumulatorsHandler.JobVertexAccumulatorsArchiver();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();

		ArchivedJson[] archives = archiver.archiveJsonWithPath(originalJob);
		Assert.assertEquals(1, archives.length);

		ArchivedJson archive = archives[0];
		Assert.assertEquals("/jobs/" + originalJob.getJobID() + "/vertices/" + originalTask.getJobVertexId() + "/accumulators", archive.path);
		compareAccumulators(originalTask, archive.json);
	}

	@Test
	public void testGetPaths() {
		JobVertexAccumulatorsHandler handler = new JobVertexAccumulatorsHandler(null);
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/vertices/:vertexid/accumulators", paths[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();
		String json = JobVertexAccumulatorsHandler.createVertexAccumulatorsJson(originalTask);

		compareAccumulators(originalTask, json);
	}

	private static void compareAccumulators(AccessExecutionJobVertex originalTask, String json) throws IOException {
		JsonNode result = ArchivedJobGenerationUtils.mapper.readTree(json);

		Assert.assertEquals(originalTask.getJobVertexId().toString(), result.get("id").asText());

		ArrayNode accs = (ArrayNode) result.get("user-accumulators");
		StringifiedAccumulatorResult[] expectedAccs = originalTask.getAggregatedUserAccumulatorsStringified();

		ArchivedJobGenerationUtils.compareStringifiedAccumulators(expectedAccs, accs);
	}
}
