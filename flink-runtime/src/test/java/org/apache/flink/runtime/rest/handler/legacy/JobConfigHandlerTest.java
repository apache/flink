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

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedJobGenerationUtils;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Tests for the JobConfigHandler.
 */
public class JobConfigHandlerTest extends TestLogger {

	@Test
	public void testArchiver() throws Exception {
		JsonArchivist archivist = new JobConfigHandler.JobConfigJsonArchivist();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();

		Collection<ArchivedJson> archives = archivist.archiveJsonWithPath(originalJob);
		Assert.assertEquals(1, archives.size());

		ArchivedJson archive = archives.iterator().next();
		Assert.assertEquals("/jobs/" + originalJob.getJobID() + "/config", archive.getPath());
		compareJobConfig(originalJob, archive.getJson());
	}

	@Test
	public void testGetPaths() {
		JobConfigHandler handler = new JobConfigHandler(mock(ExecutionGraphCache.class), Executors.directExecutor());
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/config", paths[0]);
	}

	public void testJsonGeneration() throws Exception {
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		String answer = JobConfigHandler.createJobConfigJson(originalJob);
		compareJobConfig(originalJob, answer);
	}

	private static void compareJobConfig(AccessExecutionGraph originalJob, String answer) throws IOException {
		JsonNode job = ArchivedJobGenerationUtils.MAPPER.readTree(answer);

		Assert.assertEquals(originalJob.getJobID().toString(), job.get("jid").asText());
		Assert.assertEquals(originalJob.getJobName(), job.get("name").asText());

		ArchivedExecutionConfig originalConfig = originalJob.getArchivedExecutionConfig();
		JsonNode config = job.get("execution-config");

		Assert.assertEquals(originalConfig.getExecutionMode(), config.get("execution-mode").asText());
		Assert.assertEquals(originalConfig.getRestartStrategyDescription(), config.get("restart-strategy").asText());
		Assert.assertEquals(originalConfig.getParallelism(), config.get("job-parallelism").asInt());
		Assert.assertEquals(originalConfig.getObjectReuseEnabled(), config.get("object-reuse-mode").asBoolean());

		Map<String, String> originalUserConfig = originalConfig.getGlobalJobParameters();
		JsonNode userConfig = config.get("user-config");

		for (Map.Entry<String, String> originalEntry : originalUserConfig.entrySet()) {
			Assert.assertEquals(originalEntry.getValue(), userConfig.get(originalEntry.getKey()).asText());
		}
	}
}
