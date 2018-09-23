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

package org.apache.flink.test.recovery;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.BlobServerResource;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getCurrentClasspath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Abstract base for tests verifying the behavior of the recovery in the
 * case when a TaskManager fails (process is killed) in the middle of a job execution.
 *
 * <p>The test works with multiple task managers processes by spawning JVMs.
 * Initially, it starts a JobManager in process and two TaskManagers JVMs with
 * 2 task slots each.
 * It submits a program with parallelism 4 and waits until all tasks are brought up.
 * Coordination between the test and the tasks happens via checking for the
 * existence of temporary files. It then starts another TaskManager, which is
 * guaranteed to remain empty (all tasks are already deployed) and kills one of
 * the original task managers. The recovery should restart the tasks on the new TaskManager.
 */
public abstract class AbstractTaskManagerProcessFailureRecoveryTest extends TestLogger {

	protected static final String READY_MARKER_FILE_PREFIX = "ready_";
	protected static final String PROCEED_MARKER_FILE = "proceed";
	protected static final String FINISH_MARKER_FILE_PREFIX = "finish_";

	protected static final int PARALLELISM = 4;

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final BlobServerResource blobServerResource = new BlobServerResource();

	@Test
	public void testTaskManagerProcessFailure() throws Exception {

		final StringWriter processOutput1 = new StringWriter();
		final StringWriter processOutput2 = new StringWriter();
		final StringWriter processOutput3 = new StringWriter();

		Process taskManagerProcess1 = null;
		Process taskManagerProcess2 = null;
		Process taskManagerProcess3 = null;

		File coordinateTempDir = null;

		final int jobManagerPort = NetUtils.getAvailablePort();
		final int restPort = NetUtils.getAvailablePort();

		Configuration jmConfig = new Configuration();
		jmConfig.setString(AkkaOptions.ASK_TIMEOUT, "100 s");
		jmConfig.setString(JobManagerOptions.ADDRESS, "localhost");
		jmConfig.setInteger(JobManagerOptions.PORT, jobManagerPort);
		jmConfig.setLong(HeartbeatManagerOptions.HEARTBEAT_INTERVAL, 500L);
		jmConfig.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 10000L);
		jmConfig.setInteger(RestOptions.PORT, restPort);

		final StandaloneSessionClusterEntrypoint clusterEntrypoint = new StandaloneSessionClusterEntrypoint(jmConfig);

		try {
			// check that we run this test only if the java command
			// is available on this machine
			String javaCommand = getJavaCommandPath();
			if (javaCommand == null) {
				System.out.println("---- Skipping Process Failure test : Could not find java executable ----");
				return;
			}

			// create a logging file for the process
			File tempLogFile = File.createTempFile(getClass().getSimpleName() + "-", "-log4j.properties");
			tempLogFile.deleteOnExit();
			CommonTestUtils.printLog4jDebugConfig(tempLogFile);

			// coordination between the processes goes through a directory
			coordinateTempDir = temporaryFolder.newFolder();

			clusterEntrypoint.startCluster();

			// the TaskManager java command
			String[] command = new String[] {
					javaCommand,
					"-Dlog.level=DEBUG",
					"-Dlog4j.configuration=file:" + tempLogFile.getAbsolutePath(),
					"-Xms80m", "-Xmx80m",
					"-classpath", getCurrentClasspath(),
					TaskExecutorProcessEntryPoint.class.getName(),
					String.valueOf(jobManagerPort)
			};

			// start the first two TaskManager processes
			taskManagerProcess1 = new ProcessBuilder(command).start();
			new CommonTestUtils.PipeForwarder(taskManagerProcess1.getErrorStream(), processOutput1);
			taskManagerProcess2 = new ProcessBuilder(command).start();
			new CommonTestUtils.PipeForwarder(taskManagerProcess2.getErrorStream(), processOutput2);

			// the program will set a marker file in each of its parallel tasks once they are ready, so that
			// this coordinating code is aware of this.
			// the program will very slowly consume elements until the marker file (later created by the
			// test driver code) is present
			final File coordinateDirClosure = coordinateTempDir;
			final AtomicReference<Throwable> errorRef = new AtomicReference<>();

			// we trigger program execution in a separate thread
			Thread programTrigger = new Thread("Program Trigger") {
				@Override
				public void run() {
					try {
						testTaskManagerFailure(restPort, coordinateDirClosure);
					}
					catch (Throwable t) {
						t.printStackTrace();
						errorRef.set(t);
					}
				}
			};

			//start the test program
			programTrigger.start();

			// wait until all marker files are in place, indicating that all tasks have started
			// max 20 seconds
			if (!waitForMarkerFiles(coordinateTempDir, READY_MARKER_FILE_PREFIX, PARALLELISM, 120000)) {
				// check if the program failed for some reason
				if (errorRef.get() != null) {
					Throwable error = errorRef.get();
					error.printStackTrace();
					fail("The program encountered a " + error.getClass().getSimpleName() + " : " + error.getMessage());
				}
				else {
					// no error occurred, simply a timeout
					fail("The tasks were not started within time (" + 120000 + "msecs)");
				}
			}

			// start the third TaskManager
			taskManagerProcess3 = new ProcessBuilder(command).start();
			new CommonTestUtils.PipeForwarder(taskManagerProcess3.getErrorStream(), processOutput3);

			// kill one of the previous TaskManagers, triggering a failure and recovery
			taskManagerProcess1.destroy();
			taskManagerProcess1 = null;

			// we create the marker file which signals the program functions tasks that they can complete
			touchFile(new File(coordinateTempDir, PROCEED_MARKER_FILE));

			// wait for at most 5 minutes for the program to complete
			programTrigger.join(300000);

			// check that the program really finished
			assertFalse("The program did not finish in time", programTrigger.isAlive());

			// check whether the program encountered an error
			if (errorRef.get() != null) {
				Throwable error = errorRef.get();
				error.printStackTrace();
				fail("The program encountered a " + error.getClass().getSimpleName() + " : " + error.getMessage());
			}

			// all seems well :-)
		}
		catch (Exception e) {
			e.printStackTrace();
			printProcessLog("TaskManager 1", processOutput1.toString());
			printProcessLog("TaskManager 2", processOutput2.toString());
			printProcessLog("TaskManager 3", processOutput3.toString());
			fail(e.getMessage());
		}
		catch (Error e) {
			e.printStackTrace();
			printProcessLog("TaskManager 1", processOutput1.toString());
			printProcessLog("TaskManager 2", processOutput2.toString());
			printProcessLog("TaskManager 3", processOutput3.toString());
			throw e;
		}
		finally {
			if (taskManagerProcess1 != null) {
				taskManagerProcess1.destroy();
			}
			if (taskManagerProcess2 != null) {
				taskManagerProcess2.destroy();
			}
			if (taskManagerProcess3 != null) {
				taskManagerProcess3.destroy();
			}

			clusterEntrypoint.shutDownAsync(ApplicationStatus.SUCCEEDED, null, true).get();
		}
	}

	/**
	 * The test program should be implemented here in a form of a separate thread.
	 * This provides a solution for checking that it has been terminated.
	 *
	 * @param jobManagerPort The port for submitting the topology to the local cluster
	 * @param coordinateDir TaskManager failure will be triggered only after processes
	 *                             have successfully created file under this directory
	 */
	public abstract void testTaskManagerFailure(int jobManagerPort, File coordinateDir) throws Exception;

	protected static void printProcessLog(String processName, String log) {
		if (log == null || log.length() == 0) {
			return;
		}

		System.out.println("-----------------------------------------");
		System.out.println(" BEGIN SPAWNED PROCESS LOG FOR " + processName);
		System.out.println("-----------------------------------------");
		System.out.println(log);
		System.out.println("-----------------------------------------");
		System.out.println("		END SPAWNED PROCESS LOG");
		System.out.println("-----------------------------------------");
	}

	protected static void touchFile(File file) throws IOException {
		if (!file.exists()) {
			new FileOutputStream(file).close();
		}
		if (!file.setLastModified(System.currentTimeMillis())) {
			throw new IOException("Could not touch the file.");
		}
	}

	protected static boolean waitForMarkerFiles(File basedir, String prefix, int num, long timeout) {
		long now = System.currentTimeMillis();
		final long deadline = now + timeout;

		while (now < deadline) {
			boolean allFound = true;

			for (int i = 0; i < num; i++) {
				File nextToCheck = new File(basedir, prefix + i);
				if (!nextToCheck.exists()) {
					allFound = false;
					break;
				}
			}

			if (allFound) {
				return true;
			}
			else {
				// not all found, wait for a bit
				try {
					Thread.sleep(10);
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				now = System.currentTimeMillis();
			}
		}

		return false;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * The entry point for the TaskManager JVM. Simply configures and runs a TaskManager.
	 */
	public static class TaskManagerProcessEntryPoint {

		private static final Logger LOG = LoggerFactory.getLogger(TaskManagerProcessEntryPoint.class);

		public static void main(String[] args) {
			try {
				int jobManagerPort = Integer.parseInt(args[0]);

				Configuration cfg = new Configuration();
				cfg.setString(JobManagerOptions.ADDRESS, "localhost");
				cfg.setInteger(JobManagerOptions.PORT, jobManagerPort);
				cfg.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "4m");
				cfg.setInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS, 100);
				cfg.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);
				cfg.setString(AkkaOptions.ASK_TIMEOUT, "100 s");

				TaskManager.selectNetworkInterfaceAndRunTaskManager(cfg,
					ResourceID.generate(), TaskManager.class);

				// wait forever
				Object lock = new Object();
				synchronized (lock) {
					lock.wait();
				}
			}
			catch (Throwable t) {
				LOG.error("Failed to start TaskManager process", t);
				System.exit(1);
			}
		}
	}

	/**
	 * The entry point for the TaskExecutor JVM. Simply configures and runs a TaskExecutor.
	 */
	public static class TaskExecutorProcessEntryPoint {

		private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorProcessEntryPoint.class);

		public static void main(String[] args) {
			try {
				int jobManagerPort = Integer.parseInt(args[0]);

				Configuration cfg = new Configuration();
				cfg.setString(JobManagerOptions.ADDRESS, "localhost");
				cfg.setInteger(JobManagerOptions.PORT, jobManagerPort);
				cfg.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "4m");
				cfg.setInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS, 100);
				cfg.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);
				cfg.setString(AkkaOptions.ASK_TIMEOUT, "100 s");

				TaskManagerRunner.runTaskManager(cfg, ResourceID.generate());
			}
			catch (Throwable t) {
				LOG.error("Failed to start TaskManager process", t);
				System.exit(1);
			}
		}
	}

}
