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

package org.apache.flink.client.cli;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.util.Collections;

import static org.apache.flink.client.cli.CliFrontendTestUtils.checkJobManagerAddress;
import static org.junit.Assert.fail;

/**
 * Tests that verify that the CLI client picks up the correct address for the JobManager
 * from configuration and configs.
 */
public class CliFrontendAddressConfigurationTest extends TestLogger {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@Test
	public void testValidConfig() throws Exception {
		CliFrontend frontend = new CliFrontend(
			GlobalConfiguration.loadConfiguration(CliFrontendTestUtils.getConfigDir()),
			Collections.singletonList(new DefaultCLI()),
			CliFrontendTestUtils.getConfigDir());

		RunOptions options = CliFrontendParser.parseRunCommand(new String[] {});

		ClusterClient clusterClient = frontend.retrieveClient(options);

		checkJobManagerAddress(
				clusterClient.getFlinkConfiguration(),
				CliFrontendTestUtils.TEST_JOB_MANAGER_ADDRESS,
				CliFrontendTestUtils.TEST_JOB_MANAGER_PORT);
	}

	@Test
	public void testManualOptionsOverridesConfig() {
		try {
			CliFrontend frontend = new CliFrontend(
				new Configuration(),
				Collections.singletonList(new DefaultCLI()),
				CliFrontendTestUtils.getConfigDir());

			RunOptions options = CliFrontendParser.parseRunCommand(new String[] {"-m", "203.0.113.22:7788"});

			ClusterClient client = frontend.retrieveClient(options);
			Configuration config = client.getFlinkConfiguration();

			InetSocketAddress expectedAddress = new InetSocketAddress("203.0.113.22", 7788);

			checkJobManagerAddress(config, expectedAddress.getHostName(), expectedAddress.getPort());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
