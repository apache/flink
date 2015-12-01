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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.NetUtils;

import org.junit.Test;

import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getCurrentClasspath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.isProcessAlive;

/**
 * Tests that the TaskManager process properly exits when the TaskManager actor dies.
 */
public class TaskManagerProcessReapingTest {

	@Test
	public void testReapProcessOnFailure() {
		Process taskManagerProcess = null;
		ActorSystem jmActorSystem = null;

		final StringWriter processOutput = new StringWriter();

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

			final InetAddress localhost = InetAddress.getByName("localhost");
			final int jobManagerPort = NetUtils.getAvailablePort();

			// start a JobManager
			Tuple2<String, Object> localAddress = new Tuple2<String, Object>(localhost.getHostAddress(), jobManagerPort);
			jmActorSystem = AkkaUtils.createActorSystem(
					new Configuration(), new Some<Tuple2<String, Object>>(localAddress));

			JobManager.startJobManagerActors(
				new Configuration(),
				jmActorSystem,
				JobManager.class,
				MemoryArchivist.class);

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
					org.apache.flink.util.NetUtils.ipAddressAndPortToUrlString(localhost, taskManagerPort),
					TaskManager.TASK_MANAGER_NAME());

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
			taskManagerRef.tell(PoisonPill.getInstance(), ActorRef.noSender());

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

		public static void main(String[] args) {
			try {
				int jobManagerPort = Integer.parseInt(args[0]);
				int taskManagerPort = Integer.parseInt(args[1]);

				Configuration cfg = new Configuration();
				cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
				cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerPort);
				cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 4);
				cfg.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 256);

				TaskManager.runTaskManager("localhost", taskManagerPort, cfg);

				// wait forever
				Object lock = new Object();
				synchronized (lock) {
					lock.wait();
				}
			}
			catch (Throwable t) {
				System.exit(1);
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
