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

package org.apache.flink.runtime.io.disk;

import org.apache.commons.io.FilenameUtils;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.testutils.TestJvmProcess;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.ShutdownHookUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Tests the logic of {@link FileChannelManagerImpl}.
 */
public class FileChannelManagerImplTest {
	private static final Logger LOG = LoggerFactory.getLogger(FileChannelManagerImplTest.class);

	private static final String DIR_NAME_PREFIX = "manager-test";

	private static final String COULD_KILL_SIGNAL_FILE = "could-kill";

	private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testDirectoriesCleanupOnKillWithoutCallerHook() throws Exception {
		testDirectoriesCleanupOnKill(false);
	}

	@Test
	public void testDirectoriesCleanupOnKillWithCallerHook() throws Exception {
		testDirectoriesCleanupOnKill(true);
	}

	private void testDirectoriesCleanupOnKill(boolean callerHasHook) throws Exception {
		assumeTrue(OperatingSystem.isLinux()
				|| OperatingSystem.isFreeBSD()
				|| OperatingSystem.isSolaris()
				|| OperatingSystem.isMac());

		File fileChannelDir = temporaryFolder.newFolder();
		File signalDir = temporaryFolder.newFolder();

		FileChannelManagerTestProcess fileChannelManagerTestProcess = null;

		String javaCommand = getJavaCommandPath();
		if (javaCommand == null) {
			fail("Could not find java executable.");
		}

		try {
			fileChannelManagerTestProcess = new FileChannelManagerTestProcess(
					callerHasHook,
					fileChannelDir.getAbsolutePath(),
					FilenameUtils.concat(signalDir.getAbsolutePath(), COULD_KILL_SIGNAL_FILE));
			fileChannelManagerTestProcess.startProcess();

			// Waits till netty shuffle environment has created the tmp directories.
			Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);

			while (!fileOrDirExists(signalDir, COULD_KILL_SIGNAL_FILE) && deadline.hasTimeLeft()) {
				Thread.sleep(100);
			}

			if (!fileOrDirExists(signalDir, COULD_KILL_SIGNAL_FILE) ||
					!fileOrDirExists(fileChannelDir, DIR_NAME_PREFIX)) {
				fail("The file channel manager test process does not create target directories in time, " +
						"its output is: \n" + fileChannelManagerTestProcess.getProcessOutput());
			}

			Process kill = Runtime.getRuntime().exec("kill " + fileChannelManagerTestProcess.getProcessId());
			kill.waitFor();
			assertEquals("Failed to send SIG_TERM to process", 0, kill.exitValue());

			deadline = Deadline.now().plus(TEST_TIMEOUT);
			while (fileChannelManagerTestProcess.isAlive() && deadline.hasTimeLeft()) {
				Thread.sleep(100);
			}

			if (fileChannelManagerTestProcess.isAlive()) {
				fail("The file channel manager test process does not terminate in time, its output is: \n" + fileChannelManagerTestProcess.getProcessOutput());
			}

			// Checks if the directories are cleared.
			assertThat("The file channel manager test process does not remove the tmp shuffle directories after termination, " +
							"its output is \n" + fileChannelManagerTestProcess.getProcessOutput(),
					fileOrDirExists(fileChannelDir, DIR_NAME_PREFIX),
					is(false));
		} finally {
			if (fileChannelManagerTestProcess != null) {
				fileChannelManagerTestProcess.destroy();
			}
		}
	}

	private boolean fileOrDirExists(File rootTmpDir, String namePattern) {
		File[] candidates = rootTmpDir.listFiles((dir, name) -> name.contains(namePattern));
		return candidates != null && candidates.length > 0;
	}

	/**
	 * The {@link FileChannelManagerCleanupRunner} instance running in a separate JVM process.
	 */
	private static class FileChannelManagerTestProcess extends TestJvmProcess {
		private final boolean callerHasHook;
		private final String tmpDirectories;
		private final String couldKillSignalFilePath;

		public FileChannelManagerTestProcess(boolean callerHasHook, String tmpDirectories, String couldKillSignalFilePath) throws Exception {
			this.callerHasHook = callerHasHook;
			this.tmpDirectories = tmpDirectories;
			this.couldKillSignalFilePath = couldKillSignalFilePath;
		}

		@Override
		public String getName() {
			return "File Channel Manager Test";
		}

		@Override
		public String[] getJvmArgs() {
			return new String[]{
					Boolean.toString(callerHasHook),
					tmpDirectories,
					couldKillSignalFilePath
			};
		}

		@Override
		public String getEntryPointClassName() {
			return FileChannelManagerCleanupRunner.class.getName();
		}
	}

	/**
	 * The entry point class to test the file channel manager cleanup with shutdown hook.
	 */
	public static class FileChannelManagerCleanupRunner {

		public static void main(String[] args) throws Exception{
			boolean callerHasHook = Boolean.parseBoolean(args[0]);
			String tmpDirectory = args[1];
			String couldKillSignalFilePath = args[2];

			FileChannelManager manager = new FileChannelManagerImpl(new String[]{tmpDirectory}, DIR_NAME_PREFIX);

			if (callerHasHook) {
				ShutdownHookUtil.addShutdownHook(() -> manager.close(), "Caller", LOG);
			}

			// Single main process what we can be killed.
			new File(couldKillSignalFilePath).createNewFile();

			// Waits till get killed. If we have not killed in time, make sure we exit finally.
			// Meanwhile, the test will fail due to process not terminated in time.
			Thread.sleep(5 * TEST_TIMEOUT.toMillis());
			System.exit(1);
		}
	}
}
