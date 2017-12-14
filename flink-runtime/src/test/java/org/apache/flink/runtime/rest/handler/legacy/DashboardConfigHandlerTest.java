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
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedJobGenerationUtils;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;

/**
 * Tests for the DashboardConfigHandler.
 */
public class DashboardConfigHandlerTest extends TestLogger {
	@Test
	public void testGetPaths() {
		DashboardConfigHandler handler = new DashboardConfigHandler(Executors.directExecutor(), 10000L);
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/config", paths[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		long refreshInterval = 12345;
		final ZonedDateTime zonedDateTime = ZonedDateTime.now();

		final DashboardConfiguration dashboardConfiguration = DashboardConfiguration.from(refreshInterval, zonedDateTime);

		String json = DashboardConfigHandler.createConfigJson(dashboardConfiguration);

		JsonNode result = ArchivedJobGenerationUtils.MAPPER.readTree(json);

		Assert.assertEquals(refreshInterval, result.get("refresh-interval").asLong());
		Assert.assertEquals(dashboardConfiguration.getTimeZoneName(), result.get("timezone-name").asText());
		Assert.assertEquals(dashboardConfiguration.getTimeZoneOffset(), result.get("timezone-offset").asInt());
		Assert.assertEquals(dashboardConfiguration.getFlinkVersion(), result.get("flink-version").asText());
		Assert.assertEquals(dashboardConfiguration.getFlinkRevision(), result.get("flink-revision").asText());
	}
}
