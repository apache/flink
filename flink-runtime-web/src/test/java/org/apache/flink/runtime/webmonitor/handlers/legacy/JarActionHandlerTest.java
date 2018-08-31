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

package org.apache.flink.runtime.webmonitor.handlers.legacy;

import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the JarActionHandler.
 */
public class JarActionHandlerTest {

	/**
	 * Test that the savepoint settings are correctly parsed.
	 */
	@Test
	public void testSavepointRestoreSettings() throws Exception {
		Map<String, String> pathParams = new HashMap<>();
		pathParams.put("jarid", "required"); // required

		// the following should be ignored, because they are parsed from the query params
		pathParams.put("savepointPath", "ignored");
		pathParams.put("allowNonRestoredState", "ignored");

		Map<String, String> queryParams = new HashMap<>(); // <-- everything goes here

		// Nothing configured
		JarActionHandler.JarActionHandlerConfig config = JarActionHandler.JarActionHandlerConfig.fromParams(pathParams, queryParams);
		assertEquals(SavepointRestoreSettings.none(), config.getSavepointRestoreSettings());

		// Set path
		queryParams.put("savepointPath", "the-savepoint-path");
		queryParams.put("allowNonRestoredState", "");

		SavepointRestoreSettings expected = SavepointRestoreSettings.forPath("the-savepoint-path", false);

		config = JarActionHandler.JarActionHandlerConfig.fromParams(pathParams, queryParams);
		assertEquals(expected, config.getSavepointRestoreSettings());

		// Set flag
		queryParams.put("allowNonRestoredState", "true");

		expected = SavepointRestoreSettings.forPath("the-savepoint-path", true);
		config = JarActionHandler.JarActionHandlerConfig.fromParams(pathParams, queryParams);
		assertEquals(expected, config.getSavepointRestoreSettings());
	}

	/**
	 * Tests that empty String params are handled ignored.
	 */
	@Test
	public void testEmptyStringParams() throws Exception {
		Map<String, String> pathParams = new HashMap<>();
		pathParams.put("jarid", "required"); // required
		Map<String, String> queryParams = new HashMap<>();

		queryParams.put("program-args", "");
		queryParams.put("entry-class", "");
		queryParams.put("parallelism", "");
		queryParams.put("savepointPath", "");
		queryParams.put("allowNonRestoredState", "");

		// Nothing configured
		JarActionHandler.JarActionHandlerConfig config = JarActionHandler.JarActionHandlerConfig.fromParams(pathParams, queryParams);

		assertEquals(0, config.getProgramArgs().length);
		Assert.assertNull(config.getEntryClass());
		assertEquals(1, config.getParallelism());
		assertEquals(SavepointRestoreSettings.none(), config.getSavepointRestoreSettings());
	}
}
