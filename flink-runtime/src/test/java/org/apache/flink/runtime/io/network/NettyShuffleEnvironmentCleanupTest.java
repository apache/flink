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

package org.apache.flink.runtime.io.network;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.testutils.TestJvmProcess;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.util.OperatingSystem;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Duration;
import java.util.Map;

import static org.apache.flink.runtime.testutils.CommonTestUtils.createTemporaryLog4JProperties;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Verifies whether netty shuffle releases all the resources on shutdown, like the temporary directories.
 */
public class NettyShuffleEnvironmentCleanupTest {
	private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

	@Test
	public void testRemovingTmpDirectoriesOnSignals() throws Exception {
		assumeTrue(OperatingSystem.isLinux()
				|| OperatingSystem.isFreeBSD()
				|| OperatingSystem.isSolaris()
				|| OperatingSystem.isMac());

		File confDir = temporaryFolder.newFolder();
		File confFile = new File(confDir + "/flink-conf.yaml");

		File taskManagerTmpDir = temporaryFolder.newFolder();

		Configuration config = new Configuration();
		config.setString(JobManagerOptions.ADDRESS, "localhost");
		config.setString(RestOptions.BIND_PORT, "0");
		config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.newFolder().getAbsolutePath());
		config.setString(CoreOptions.TMP_DIRS, taskManagerTmpDir.getAbsolutePath());
		config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "4m");
		config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, 100);
		config.setString(TaskManagerOptions.TOTAL_FLINK_MEMORY, "512m");

		try (FileOutputStream fos = new FileOutputStream(confFile);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos))) {
			for (Map.Entry<String, String> e : config.toMap().entrySet()) {
				writer.write(e.getKey());
				writer.write(": ");
				writer.write(e.getValue());
				writer.newLine();
			}

			writer.flush();
		}

		TaskManagerProcess taskManagerProcess = null;

		try (final StandaloneSessionClusterEntrypoint clusterEntrypoint = new StandaloneSessionClusterEntrypoint(config)) {
			String javaCommand = getJavaCommandPath();
			if (javaCommand == null) {
				fail("Could not find java executable.");
			}

			clusterEntrypoint.startCluster();

			taskManagerProcess = new TaskManagerProcess(javaCommand, confDir.getAbsolutePath());
			taskManagerProcess.startProcess();

			// Waits till netty shuffle environment has created the tmp directories.
			Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
			File[] nettyShuffleTmpFiles = findNettyShuffleDirectories(taskManagerTmpDir);

			while (nettyShuffleTmpFiles.length == 0 && deadline.hasTimeLeft()) {
				Thread.sleep(100);
				nettyShuffleTmpFiles = findNettyShuffleDirectories(taskManagerTmpDir);
			}

			if (nettyShuffleTmpFiles.length == 0) {
				fail("The TaskManager process does not create shuffle directories in time, " +
						"its output is: \n" + taskManagerProcess.getProcessOutput());
			}

			Process kill = Runtime.getRuntime().exec("kill " + taskManagerProcess.getProcessId());
			kill.waitFor();
			assertEquals("Failed to send SIG_TERM to process", 0, kill.exitValue());

			deadline = Deadline.now().plus(TEST_TIMEOUT);
			while (taskManagerProcess.isAlive() && deadline.hasTimeLeft()) {
				Thread.sleep(100);
			}

			if (taskManagerProcess.isAlive()) {
				fail("The TaskManager process does not terminate in time, its output is: \n" + taskManagerProcess.getProcessOutput());
			}

			// Checks if the directories are cleared.
			nettyShuffleTmpFiles = findNettyShuffleDirectories(taskManagerTmpDir);
			assertEquals("The TaskManager does not remove the tmp shuffle directories after termination, " +
							"its output is \n" + taskManagerProcess.getProcessOutput(),
					0, nettyShuffleTmpFiles.length);
		}
	}

	private File[] findNettyShuffleDirectories(File rootTmpDir) {
		return rootTmpDir.listFiles((dir, name) -> name.contains(NettyShuffleServiceFactory.DIR_NAME_PREFIX));
	}

	/**
	 * A {@link TaskManagerRunner} instance running in a separate JVM.
	 */
	private static class TaskManagerProcess extends TestJvmProcess {
		private final String configDirPath;

		public TaskManagerProcess(String javaCommandPath, String configDirPath) throws IOException {
			super(javaCommandPath, createTemporaryLog4JProperties().getAbsolutePath());
			this.configDirPath = configDirPath;
		}

		@Override
		public String getName() {
			return "TaskManager";
		}

		@Override
		public String[] getJvmArgs() {
			return new String[]{
					"--configDir",
					configDirPath
			};
		}

		@Override
		public String getEntryPointClassName() {
			return TaskManagerRunner.class.getName();
		}
	}
}
