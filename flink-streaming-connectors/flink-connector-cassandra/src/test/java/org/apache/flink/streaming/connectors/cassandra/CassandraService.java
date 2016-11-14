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
package org.apache.flink.streaming.connectors.cassandra;

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.TestJvmProcess;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

public class CassandraService extends TestJvmProcess {
	private File tmpDir;
	private File tmpCassandraYaml;

	public CassandraService() throws Exception {
		createCassandraYaml();
		setJVMMemory(512);
	}

	private void createCassandraYaml() throws IOException {
		// generate temporary files
		tmpDir = CommonTestUtils.createTempDirectory();
		ClassLoader classLoader = CassandraConnectorITCase.class.getClassLoader();
		File file = new File(classLoader.getResource("cassandra.yaml").getFile());
		tmpCassandraYaml = new File(tmpDir.getAbsolutePath() + File.separator + "cassandra.yaml");

		assertTrue(tmpCassandraYaml.createNewFile());
		BufferedWriter b = new BufferedWriter(new FileWriter(tmpCassandraYaml));

		//copy cassandra.yaml; inject absolute paths into cassandra.yaml
		Scanner scanner = new Scanner(file);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			line = line.replace("$PATH", "'" + tmpCassandraYaml.getParentFile());
			b.write(line + "\n");
			b.flush();
		}
		scanner.close();
	}

	@Override
	public String getName() {
		return "CassandraService";
	}

	@Override
	public String[] getJvmArgs() {
		return new String[]{
			tmpCassandraYaml.toURI().toString(),
			// these options were taken directly from the jvm.options file in the cassandra repo
			"-XX:+UseThreadPriorities",
			"-Xss256k",
			"-XX:+AlwaysPreTouch",
			"-XX:+UseTLAB",
			"-XX:+ResizeTLAB",
			"-XX:+UseNUMA",
			"-XX:+PerfDisableSharedMem",
			"-XX:+UseParNewGC",
			"-XX:+UseConcMarkSweepGC",
			"-XX:+CMSParallelRemarkEnabled",
			"-XX:SurvivorRatio=8",
			"-XX:MaxTenuringThreshold=1",
			"-XX:CMSInitiatingOccupancyFraction=75",
			"-XX:+UseCMSInitiatingOccupancyOnly",
			"-XX:CMSWaitDuration=10000",
			"-XX:+CMSParallelInitialMarkEnabled",
			"-XX:+CMSEdenChunksRecordAlways",
			"-XX:+CMSClassUnloadingEnabled",};
	}

	@Override
	public String getEntryPointClassName() {
		return CassandraServiceEntryPoint.class.getName();
	}

	public static class CassandraServiceEntryPoint {
		public static void main(String[] args) throws InterruptedException {
			final CassandraDaemon cassandraDaemon = new CassandraDaemon();

			System.setProperty("cassandra.config", args[0]);

			cassandraDaemon.activate();

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					cassandraDaemon.deactivate();
				}
			});

			// Run forever
			new CountDownLatch(1).await();
		}

	}
}
