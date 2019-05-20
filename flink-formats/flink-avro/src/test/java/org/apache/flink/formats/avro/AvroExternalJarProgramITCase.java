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

import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.testjar.AvroExternalJarProgram;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/**
 * IT case for the {@link AvroExternalJarProgram}.
 */
public class AvroExternalJarProgramITCase extends TestLogger {

	private static final String JAR_FILE = "maven-test-jar.jar";

	private static final String TEST_DATA_FILE = "/testdata.avro";

	private static final int PARALLELISM = 4;

	private static final MiniCluster MINI_CLUSTER = new MiniCluster(
		new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(1)
			.setNumSlotsPerTaskManager(PARALLELISM)
			.build());

	@BeforeClass
	public static void setUp() throws Exception {
		MINI_CLUSTER.start();
	}

	@AfterClass
	public static void tearDown() {
		TestEnvironment.unsetAsContext();
		MINI_CLUSTER.closeAsync();
	}

	@Test
	public void testExternalProgram() throws Exception {

		String jarFile = JAR_FILE;
		try {
			JobWithJars.checkJarFile(new File(jarFile).getAbsoluteFile().toURI().toURL());
		} catch (IOException e) {
			jarFile = "target/".concat(jarFile);
		}

		TestEnvironment.setAsContext(
			MINI_CLUSTER,
			PARALLELISM,
			Collections.singleton(new Path(jarFile)),
			Collections.emptyList());

		String testData = getClass().getResource(TEST_DATA_FILE).toString();

		PackagedProgram program = new PackagedProgram(new File(jarFile), new String[]{testData});

		program.invokeInteractiveModeForExecution();
	}
}
