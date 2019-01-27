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

package org.apache.flink.test.runtime.minicluster;

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.test.util.MiniClusterResource.CODEBASE_KEY;
import static org.apache.flink.test.util.MiniClusterResource.NEW_CODEBASE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * MiniCluster use the same ResourceManager as StandaloneSessionCluster.
 * So this is also an integration tests for {@link org.apache.flink.runtime.resourcemanager.StandaloneResourceManager}.
 */
public class MiniClusterITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(MiniClusterITCase.class);

	private MiniCluster miniCluster;
	private Configuration configuration;
	private static File tempConfPathForRun = null;
	private String flinkConfDir;
	private String jobManagerAddress;

	// Temp directory which is deleted after the unit test.
	public static TemporaryFolder tmp = new TemporaryFolder();

	@Before
	public void startMiniCluster() throws Exception {
		tmp.create();
		setConf();
		final int numTaskManagers = 1;
		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
				.setConfiguration(configuration)
				.setNumTaskManagers(numTaskManagers)
				.setNumSlotsPerTaskManager(1)
				.build();
		miniCluster = new MiniCluster(miniClusterConfiguration);
		miniCluster.start();
		configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().getPort());
		jobManagerAddress = miniCluster.getRestAddress().getHost() + ":" + miniCluster.getRestAddress().getPort();
	}

	@Test
	public void testSubmitJob() throws Exception {
		CliFrontend cli = new CliFrontend(
				configuration,
				CliFrontend.loadCustomCommandLines(configuration, flinkConfDir));
		File exampleJarLocation = getTestJarPath("StreamingWordCount-test-jar.jar");
		String[] args = new String[]{"run", "-m", jobManagerAddress, exampleJarLocation.getAbsolutePath()};
		int returnValue = cli.parseParameters(args);
		assertEquals(0, returnValue);
	}

	@Test
	public void testSubmitJobWithResource() throws Exception {
		CliFrontend cli = new CliFrontend(
				configuration,
				CliFrontend.loadCustomCommandLines(configuration, flinkConfDir));
		File exampleJarLocation = getTestJarPath("StreamingWordCount-test-jar.jar");
		String[] args = new String[]{"run", "-m", jobManagerAddress, exampleJarLocation.getAbsolutePath(),
				"--resource", "vcores:0.1,memory:100"};
		int returnValue = cli.parseParameters(args);
		assertEquals(0, returnValue);
	}

	@Test
	public void testSubmitJobWithResourceAndDetach() throws Exception {
		CliFrontend cli = new CliFrontend(
				configuration,
				CliFrontend.loadCustomCommandLines(configuration, flinkConfDir));
		File exampleJarLocation = getTestJarPath("StreamingWordCount-test-jar.jar");
		String[] args = new String[]{"run", "-m", jobManagerAddress, "-d", exampleJarLocation.getAbsolutePath(),
				"--resource", "vcores:0.1,memory:100"};
		int returnValue = cli.parseParameters(args);
		assertEquals(0, returnValue);
		assertTrue(waitForAllJobsFinishedOrTimeout());
	}

	@Test
	public void testSubmitJobWithBigResource() throws Exception {
		CliFrontend cli = new CliFrontend(
				configuration,
				CliFrontend.loadCustomCommandLines(configuration, null));
		File exampleJarLocation = getTestJarPath("StreamingWordCount-test-jar.jar");
		String[] args = new String[]{"run", "-m", jobManagerAddress, exampleJarLocation.getAbsolutePath(),
				"--resource", "vcores:100,memory:100"};
		new Thread(() -> cli.parseParameters(args)).start();
		assertFalse(waitForAllJobsFinishedOrTimeout());
	}

	@Test
	public void testSubmitJobWithBigResourceAndDetach() throws Exception {
		CliFrontend cli = new CliFrontend(
				configuration,
				CliFrontend.loadCustomCommandLines(configuration, null));
		File exampleJarLocation = getTestJarPath("StreamingWordCount-test-jar.jar");
		String[] args = new String[]{"run", "-m", jobManagerAddress, "-d", exampleJarLocation.getAbsolutePath(),
				"--resource", "vcores:100,memory:100"};
		int returnValue = cli.parseParameters(args);
		assertEquals(0, returnValue);
		assertFalse(waitForAllJobsFinishedOrTimeout());
	}

	@After
	public void stopMiniCluster() {
		if (miniCluster != null && miniCluster.isRunning()) {
			miniCluster.closeAsync();
		}
		tmp.delete();
	}

	// Utility methods

	/**
	 * Wait for all jobs finished or timeout.
	 * @return True if all jobs finished and false if timeout.
	 */
	private boolean waitForAllJobsFinishedOrTimeout() throws ExecutionException, InterruptedException {
		for (int i = 0; i < 10; i++) {
			AtomicBoolean finished = new AtomicBoolean(true);
			if (miniCluster.listJobs().get().size() > 0) {
				miniCluster.listJobs().get().forEach(e -> {
					if (!e.getJobState().equals(JobStatus.FINISHED)) {
						finished.set(false);
					}
				});
				if (finished.get()) {
					return true;
				}
			}
			LOG.info("Wait for all jobs to be finished");
			Thread.sleep(500);
		}
		return false;
	}

	private File getTestJarPath(String fileName) throws FileNotFoundException {
		File f = new File(fileName);
		File current = new File("");
		if (!current.getAbsolutePath().contains("target")) {
			f = new File("target", fileName);
		}
		if (!f.exists()) {
			throw new FileNotFoundException("Test jar " + f.getAbsolutePath() + " not present. Invoke tests using maven "
					+ "or build the jar using 'mvn process-test-classes' in flink-tests");
		}
		return f;
	}

	private void setConf() throws IOException {
		configuration = new Configuration();

		tempConfPathForRun = tmp.newFolder("conf");

		configuration.setString(CoreOptions.MODE,
				Objects.equals(NEW_CODEBASE, System.getProperty(CODEBASE_KEY)) ? CoreOptions.NEW_MODE : CoreOptions.LEGACY_MODE);
		configuration.setString(JobManagerOptions.ADDRESS, "localhost");
		configuration.setInteger(RestOptions.PORT, 0);

		BootstrapTools.writeConfiguration(
				configuration,
				new File(tempConfPathForRun, "flink-conf.yaml"));

		flinkConfDir = tempConfPathForRun.getAbsolutePath();

		LOG.info("Temporary Flink configuration directory to be used for secure test: {}", flinkConfDir);
	}
}
