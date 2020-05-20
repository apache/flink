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

package org.apache.flink.externalresource.gpu;

import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import static org.junit.Assume.assumeTrue;

/**
 * Tests for the gpu-discovery-common.sh.
 */
public class GPUDiscoveryScriptTest extends TestLogger {

	private static final String TEST_SCRIPT_PATH = "src/test/resources/test-coordination-mode.sh";

	@Test
	public void testNonCoordinationMode() throws Exception {
		assumeTrue(OperatingSystem.isLinux());
		testExistWithNonZero("test_non_coordination_mode");
	}

	@Test
	public void testCoordinateIndexes() throws Exception {
		assumeTrue(OperatingSystem.isLinux());
		testExistWithNonZero("test_coordinate_indexes");
	}

	@Test
	public void testPreemptFromDeadProcesses() throws Exception {
		assumeTrue(OperatingSystem.isLinux());
		testExistWithNonZero("test_preempt_from_dead_processes");
	}

	@Test
	public void testSetCoordinationFile() throws Exception {
		assumeTrue(OperatingSystem.isLinux());
		testExistWithNonZero("test_coordination_file");
	}

	private void testExistWithNonZero(String cmd) throws Exception {
		final ProcessBuilder processBuilder = new ProcessBuilder(TEST_SCRIPT_PATH, cmd);
		processBuilder.redirectErrorStream(true);
		final Process process = processBuilder.start();
		try (final BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
			final int exitValue = process.waitFor();
			if (exitValue != 0) {
				final String stdout = stdoutReader.lines().collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString();
				throw new Exception(String.format("Script exist with non-zero %d.\\n OUTPUT: %s", exitValue, stdout));
			}
		} finally {
			process.destroyForcibly();
		}
	}
}
