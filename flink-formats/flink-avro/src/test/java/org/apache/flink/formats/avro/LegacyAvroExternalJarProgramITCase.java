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

package org.apache.flink.formats.avro;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.testjar.AvroExternalJarProgram;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Collections;

/**
 * IT case for the {@link AvroExternalJarProgram}.
 */
public class LegacyAvroExternalJarProgramITCase extends TestLogger {

	private static final String JAR_FILE = "maven-test-jar.jar";

	private static final String TEST_DATA_FILE = "/testdata.avro";

	@Test
	public void testExternalProgram() {

		LocalFlinkMiniCluster testMiniCluster = null;

		try {
			int parallelism = 4;
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, parallelism);
			testMiniCluster = new LocalFlinkMiniCluster(config, false);
			testMiniCluster.start();

			String jarFile = JAR_FILE;
			String testData = getClass().getResource(TEST_DATA_FILE).toString();

			PackagedProgram program = new PackagedProgram(new File(jarFile), new String[] { testData });

			TestEnvironment.setAsContext(
				testMiniCluster,
				parallelism,
				Collections.singleton(new Path(jarFile)),
				Collections.<URL>emptyList());

			config.setString(JobManagerOptions.ADDRESS, "localhost");
			config.setInteger(JobManagerOptions.PORT, testMiniCluster.getLeaderRPCPort());

			program.invokeInteractiveModeForExecution();
		}
		catch (Throwable t) {
			System.err.println(t.getMessage());
			t.printStackTrace();
			Assert.fail("Error during the packaged program execution: " + t.getMessage());
		}
		finally {
			TestEnvironment.unsetAsContext();

			if (testMiniCluster != null) {
				try {
					testMiniCluster.stop();
				} catch (Throwable t) {
					// ignore
				}
			}
		}
	}
}
