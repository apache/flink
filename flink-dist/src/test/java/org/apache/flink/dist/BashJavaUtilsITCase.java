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

package org.apache.flink.dist;

import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.util.BashJavaUtils;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for BashJavaUtils.
 *
 * <p>This test requires the distribution to be assembled and is hence marked as an IT case which run after packaging.
 */
public class BashJavaUtilsITCase extends JavaBashTestBase {

	private static final String RUN_BASH_JAVA_UTILS_CMD_SCRIPT = "src/test/bin/runBashJavaUtilsCmd.sh";

	@Test
	public void testGetTmResourceParamsConfigs() throws Exception {
		String[] commands = {RUN_BASH_JAVA_UTILS_CMD_SCRIPT, BashJavaUtils.Command.GET_TM_RESOURCE_PARAMS.toString()};
		List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

		assertEquals(2, lines.size());
		ConfigurationUtils.parseJvmArgString(lines.get(lines.size() - 2));
		ConfigurationUtils.parseTmResourceDynamicConfigs(lines.get(lines.size() - 1));
	}
}
