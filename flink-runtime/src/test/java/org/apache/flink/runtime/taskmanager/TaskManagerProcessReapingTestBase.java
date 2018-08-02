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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.standalone.StandaloneResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getCurrentClasspath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.isProcessAlive;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that the TaskManager process properly exits when the TaskManager actor dies.
 */
public abstract class TaskManagerProcessReapingTestBase extends TestLogger {

	/**
	 * Called after the task manager has been started up. After calling this
	 * method, the test base checks that the process exits.
	 */
	abstract void onTaskManagerProcessRunning(ActorRef taskManager);

	/**
	 * Called after the task manager has successfully terminated.
	 */
	void onTaskManagerProcessTerminated(String processOutput) {
		// Default does nothing
	}

	@Test
	public void testReapProcessOnFailure() throws Exception {
		Process taskManagerProcess = null;
		ActorSystem jmActorSystem = null;

		final StringWriter processOutput = new StringWriter();

		final Configuration config = new Configuration();

		final String jobManagerHostname = "localhost";
		final int jobManagerPort = NetUtils.getAvailablePort();

		config.setString(JobManagerOptions.ADDRESS, jobManagerHostname);
		config.setInteger(JobManagerOptions.PORT, jobManagerPort);

		final HighAvailabilityServices highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			config,
			TestingUtils.defaultExecutor(),
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

		try {
			String javaCommand = getJavaCommandPath();

			// check that we run this test only if the java command
			// is available on this machine
			if (javaCommand == null) {
				System.out.println("---- Skipping TaskManagerProcessReapingTest : Could not find java executable ----");
				return;
			}

			// create a logging file for the process
			File tempLogFile = File.createTempFile("testlogconfig", "properties");
			tempLogFile.deleteOnExit();
			CommonTestUtils.printLog4jDebugConfig(tempLogFile);

			// start a JobManager
			Tuple2<String, Object> localAddress = new Tuple2<String, Object>(jobManagerHostname, jobManagerPort);
			jmActorSystem = AkkaUtils.createActorSystem(config, new Some<>(localAddress));

			ActorRef jmActor = JobManager.startJobManagerActors(
				config,
				jmActorSystem,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				Option.empty(),
				JobManager.class,
				MemoryArchivist.class)._1;

			FlinkResourceManager.startResourceManagerActors(
				new Configuration(),
				jmActorSystem,
				highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
				StandaloneResourceManager.class);

			final int taskManagerPort = NetUtils.getAvailablePort();

			// start the task manager process
			String[] command = new String[] {
					javaCommand,
					"-Dlog.level=DEBUG",
					"-Dlog4j.configuration=file:" + tempLogFile.getAbsolutePath(),
					"-Xms256m", "-Xmx256m",
					"-classpath", getCurrentClasspath(),
					TaskManagerTestEntryPoint.class.getName(),
					String.valueOf(jobManagerPort), String.valueOf(taskManagerPort)
			};

			ProcessBuilder bld = new ProcessBuilder(command);
			taskManagerProcess = bld.start();
			new PipeForwarder(taskManagerProcess.getErrorStream(), processOutput);

			// grab the reference to the TaskManager. try multiple times, until the process
			// is started and the TaskManager is up
			String taskManagerActorName = String.format("akka.tcp://flink@%s/user/%s",
					"localhost:" + taskManagerPort,
					TaskExecutor.TASK_MANAGER_NAME);

			ActorRef taskManagerRef = null;
			Throwable lastError = null;
			for (int i = 0; i < 40; i++) {
				try {
					taskManagerRef = TaskManager.getTaskManagerRemoteReference(
							taskManagerActorName, jmActorSystem, new FiniteDuration(25, TimeUnit.SECONDS));
					break;
				}
				catch (Throwable t) {
					// TaskManager probably not ready yet
					lastError = t;
				}
				Thread.sleep(500);
			}

			assertTrue("TaskManager process died", isProcessAlive(taskManagerProcess));

			if (taskManagerRef == null) {
				if (lastError != null) {
					lastError.printStackTrace();
				}
				fail("TaskManager process did not launch the TaskManager properly. Failed to look up "
						+ taskManagerActorName);
			}

			// kill the TaskManager actor
			onTaskManagerProcessRunning(taskManagerRef);

			// wait for max 5 seconds for the process to terminate
			{
				long now = System.currentTimeMillis();
				long deadline = now + 10000;

				while (now < deadline && isProcessAlive(taskManagerProcess)) {
					Thread.sleep(100);
					now = System.currentTimeMillis();
				}
			}

			assertFalse("TaskManager process did not terminate upon actor death", isProcessAlive(taskManagerProcess));

			int returnCode = taskManagerProcess.exitValue();
			assertEquals("TaskManager died, but not because of the process reaper",
					TaskManager.RUNTIME_FAILURE_RETURN_CODE(), returnCode);

			onTaskManagerProcessTerminated(processOutput.toString());
		}
		catch (Exception e) {
			e.printStackTrace();
			printProcessLog(processOutput.toString());
			fail(e.getMessage());
		}
		catch (Error e) {
			e.printStackTrace();
			printProcessLog(processOutput.toString());
			throw e;
		}
		finally {
			if (taskManagerProcess != null) {
				taskManagerProcess.destroy();
			}
			if (jmActorSystem != null) {
				jmActorSystem.shutdown();
			}
			if (highAvailabilityServices != null) {
				highAvailabilityServices.closeAndCleanupAllData();
			}
		}
	}

	private static void printProcessLog(String log) {
		System.out.println("-----------------------------------------");
		System.out.println("       BEGIN SPAWNED PROCESS LOG");
		System.out.println("-----------------------------------------");
		System.out.println(log);
		System.out.println("-----------------------------------------");
		System.out.println("        END SPAWNED PROCESS LOG");
		System.out.println("-----------------------------------------");
	}

	// --------------------------------------------------------------------------------------------

	public static class TaskManagerTestEntryPoint {

		public static void main(String[] args) throws Exception {
			int jobManagerPort = Integer.parseInt(args[0]);
			int taskManagerPort = Integer.parseInt(args[1]);

			Configuration cfg = new Configuration();
			cfg.setString(JobManagerOptions.ADDRESS, "localhost");
			cfg.setInteger(JobManagerOptions.PORT, jobManagerPort);
			cfg.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "4m");
			cfg.setInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS, 256);

			final HighAvailabilityServices highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
				cfg,
				TestingUtils.defaultExecutor(),
				HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

			try {
				TaskManager.runTaskManager(
					"localhost",
					ResourceID.generate(),
					taskManagerPort,
					cfg,
					highAvailabilityServices);

				// wait forever
				Object lock = new Object();
				synchronized (lock) {
					lock.wait();
				}
			}
			catch (Throwable t) {
				System.exit(1);
			} finally {
				highAvailabilityServices.closeAndCleanupAllData();
			}
		}
	}

	private static class PipeForwarder extends Thread {

		private final StringWriter target;
		private final InputStream source;

		public PipeForwarder(InputStream source, StringWriter target) {
			super("Pipe Forwarder");
			setDaemon(true);

			this.source = source;
			this.target = target;

			start();
		}

		@Override
		public void run() {
			try {
				int next;
				while ((next = source.read()) != -1) {
					target.write(next);
				}
			}
			catch (IOException e) {
				// terminate
			}
		}
	}
}
