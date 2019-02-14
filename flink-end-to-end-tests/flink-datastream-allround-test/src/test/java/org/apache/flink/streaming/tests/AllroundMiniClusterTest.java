/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;

/**
 * DataStreamAllroundTestProgram on MiniCluster for manual debugging purposes.
 */
@Ignore("Test is already part of end-to-end tests. This is for manual debugging.")
public class AllroundMiniClusterTest extends TestLogger {

	@BeforeClass
	public static void beforeClass() {
		org.apache.log4j.PropertyConfigurator.configure(
			AllroundMiniClusterTest.class.getClassLoader().getResource("log4j.properties"));
	}

	@ClassRule
	public static MiniClusterWithClientResource miniClusterResource = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setNumberTaskManagers(4)
			.setNumberSlotsPerTaskManager(2)
			.setConfiguration(createConfiguration())
			.build());

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static Configuration createConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, true);
		configuration.setString(EXECUTION_FAILOVER_STRATEGY.key(), "region");
		return configuration;
	}

	@Test
	public void runTest() throws Exception {
		File checkpointDir = temporaryFolder.newFolder();
		DataStreamAllroundTestProgram.main(
			new String[]{
				"--environment.parallelism", "8",
				"--state_backend.checkpoint_directory", checkpointDir.toURI().toString(),
				"--state_backend", "rocks",
				"--state_backend.rocks.incremental", "true",
				"--test.simulate_failure", "true",
				"--test.simulate_failure.max_failures", String.valueOf(Integer.MAX_VALUE),
				"--test.simulate_failure.num_records", "100000"
			});
	}
}
