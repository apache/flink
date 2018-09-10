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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;

import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Environment}.
 */
public class EnvironmentTest {

	private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";
	private static final String FACTORY_ENVIRONMENT_FILE = "test-sql-client-factory.yaml";

	@Test
	public void testMerging() throws Exception {
		final Map<String, String> replaceVars1 = new HashMap<>();
		replaceVars1.put("$VAR_UPDATE_MODE", "update-mode: append");
		final Environment env1 = EnvironmentFileUtil.parseModified(
			DEFAULTS_ENVIRONMENT_FILE,
			replaceVars1);

		final Map<String, String> replaceVars2 = new HashMap<>(replaceVars1);
		replaceVars2.put("TableNumber1", "NewTable");
		final Environment env2 = EnvironmentFileUtil.parseModified(
			FACTORY_ENVIRONMENT_FILE,
			replaceVars2);

		final Environment merged = Environment.merge(env1, env2);

		final Set<String> tables = new HashSet<>();
		tables.add("TableNumber1");
		tables.add("TableNumber2");
		tables.add("NewTable");
		tables.add("TableSourceSink");

		assertEquals(tables, merged.getTables().keySet());
		assertTrue(merged.getExecution().isStreamingExecution());
		assertEquals(16, merged.getExecution().getMaxParallelism());
	}
}
