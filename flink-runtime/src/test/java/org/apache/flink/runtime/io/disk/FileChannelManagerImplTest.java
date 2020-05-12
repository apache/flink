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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.testutils.TestJvmProcess;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Tests the logic of {@link FileChannelManagerImpl}.
 */
public class FileChannelManagerImplTest extends TestLogger {
	private static final Logger LOG = LoggerFactory.getLogger(FileChannelManagerImplTest.class);

	private static final String DIR_NAME_PREFIX = "manager-test";

	/**
	 * Marker file indicating the test process is ready to be killed. We could not simply kill the process
	 * after FileChannelManager has created temporary files since we also need to ensure the caller has
	 * also registered the shutdown hook if <tt>callerHasHook</tt> is true.
	 */
	private static final String SIGNAL_FILE_FOR_KILLING = "could-kill";

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
		File signalFile = new File(signalDir.getAbsolutePath(), SIGNAL_FILE_FOR_KILLING);

		FileChannelManagerTestProcess fileChannelManagerTestProcess = new FileChannelManagerTestProcess(
			callerHasHook,
			fileChannelDir.getAbsolutePath(),
			signalFile.getAbsolutePath());

		try {
			fileChannelManagerTestProcess.startProcess();

			// Waits till the process has created temporary files and registered the corresponding shutdown hooks.
			TestJvmProcess.waitForMarkerFile(signalFile, TEST_TIMEOUT.toMillis());

			Process kill = Runtime.getRuntime().exec("kill " + fileChannelManagerTestProcess.getProcessId());
			kill.waitFor();
			assertEquals("Failed to send SIG_TERM to process", 0, kill.exitValue());

			Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
			while (fileChannelManagerTestProcess.isAlive() && deadline.hasTimeLeft()) {
				Thread.sleep(100);
			}

			assertFalse(
				"The file channel manager test process does not terminate in time, its output is: \n" +
					fileChannelManagerTestProcess.getProcessOutput(),
				fileChannelManagerTestProcess.isAlive());

			// Checks if the directories are cleared.
			assertFalse(
				"The file channel manager test process does not remove the tmp shuffle directories after termination, " +
					"its output is \n" + fileChannelManagerTestProcess.getProcessOutput(),
				fileOrDirExists(fileChannelDir, DIR_NAME_PREFIX));
		} finally {
			fileChannelManagerTestProcess.destroy();
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
		private final String signalFilePath;

		FileChannelManagerTestProcess(boolean callerHasHook, String tmpDirectories, String signalFilePath) throws Exception {
			this.callerHasHook = callerHasHook;
			this.tmpDirectories = tmpDirectories;
			this.signalFilePath = signalFilePath;
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
					signalFilePath
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
			String signalFilePath = args[2];

			FileChannelManager manager = new FileChannelManagerImpl(new String[]{tmpDirectory}, DIR_NAME_PREFIX);

			if (callerHasHook) {
				// Verifies the case that both FileChannelManager and its upper component
				// have registered shutdown hooks, like in IOManager.
				ShutdownHookUtil.addShutdownHook(() -> manager.close(), "Caller", LOG);
			}

			// Signals the main process to execute the kill action.
			new File(signalFilePath).createNewFile();

			// Blocks the process to wait to be killed.
			Thread.sleep(3 * TEST_TIMEOUT.toMillis());
		}
	}
}
