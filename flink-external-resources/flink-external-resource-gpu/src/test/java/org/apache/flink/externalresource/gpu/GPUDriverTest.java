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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link GPUDriver}.
 */
public class GPUDriverTest extends TestLogger {

	private static final String TESTING_DISCOVERY_SCRIPT_PATH = "src/test/resources/testing-gpu-discovery.sh";

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void testGPUDriverWithTestScript() throws Exception {
		final int gpuAmount = 2;
		final Configuration config = new Configuration();
		config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, TESTING_DISCOVERY_SCRIPT_PATH);

		final GPUDriver gpuDriver = new GPUDriver(config);
		final Set<GPUInfo> gpuResource = gpuDriver.retrieveResourceInfo(gpuAmount);

		assertThat(gpuResource.size(), is(gpuAmount));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGPUDriverWithInvalidAmount() throws Exception {
		final int gpuAmount = -1;
		final Configuration config = new Configuration();
		config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, TESTING_DISCOVERY_SCRIPT_PATH);

		final GPUDriver gpuDriver = new GPUDriver(config);
		gpuDriver.retrieveResourceInfo(gpuAmount);
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testGPUDriverWithIllegalConfigTestScript() throws Exception {
		final Configuration config = new Configuration();
		config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, " ");

		new GPUDriver(config);
	}

	@Test(expected = FileNotFoundException.class)
	public void testGPUDriverWithTestScriptDoNotExist() throws Exception {
		final Configuration config = new Configuration();
		config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, "invalid/path");

		new GPUDriver(config);
	}

	@Test(expected = FlinkException.class)
	public void testGPUDriverWithInexecutableScript() throws Exception {
		final Configuration config = new Configuration();
		final File inexecutableFile = TEMP_FOLDER.newFile();
		assertTrue(inexecutableFile.setExecutable(false));

		config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, inexecutableFile.getAbsolutePath());

		new GPUDriver(config);
	}

	@Test(expected = FlinkException.class)
	public void testGPUDriverWithTestScriptExitWithNonZero() throws Exception {
		final Configuration config = new Configuration();
		config.setString(GPUDriver.DISCOVERY_SCRIPT_PATH, TESTING_DISCOVERY_SCRIPT_PATH);
		config.setString(GPUDriver.DISCOVERY_SCRIPT_ARG, "--exit-non-zero");

		final GPUDriver gpuDriver = new GPUDriver(config);
		gpuDriver.retrieveResourceInfo(1);
	}
}
