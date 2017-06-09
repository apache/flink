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

import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.TimeZone;

/**
 * Tests for the DashboardConfigHandler.
 */
public class DashboardConfigHandlerTest {
	@Test
	public void testGetPaths() {
		DashboardConfigHandler handler = new DashboardConfigHandler(10000L);
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/config", paths[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		long refreshInterval = 12345;
		TimeZone timeZone = TimeZone.getDefault();
		EnvironmentInformation.RevisionInformation revision = EnvironmentInformation.getRevisionInformation();

		String json = DashboardConfigHandler.createConfigJson(refreshInterval);

		JsonNode result = ArchivedJobGenerationUtils.MAPPER.readTree(json);

		Assert.assertEquals(refreshInterval, result.get("refresh-interval").asLong());
		Assert.assertEquals(timeZone.getDisplayName(), result.get("timezone-name").asText());
		Assert.assertEquals(timeZone.getRawOffset(), result.get("timezone-offset").asLong());
		Assert.assertEquals(EnvironmentInformation.getVersion(), result.get("flink-version").asText());
		Assert.assertEquals(revision.commitId + " @ " + revision.commitDate, result.get("flink-revision").asText());
	}
}
