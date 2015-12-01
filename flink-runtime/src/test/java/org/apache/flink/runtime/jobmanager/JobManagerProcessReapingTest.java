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

package org.apache.flink.runtime.jobmanager;

import static org.junit.Assert.*;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getCurrentClasspath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.isProcessAlive;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.junit.Test;

import org.apache.flink.configuration.Configuration;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests that the JobManager process properly exits when the JobManager actor dies.
 */
public class JobManagerProcessReapingTest {

	@Test
	public void testReapProcessOnFailure() {
		Process jmProcess = null;
		ActorSystem localSystem = null;

		final StringWriter processOutput = new StringWriter();

		try {
			String javaCommand = getJavaCommandPath();

			// check that we run this test only if the java command
			// is available on this machine
			if (javaCommand == null) {
				System.out.println("---- Skipping JobManagerProcessReapingTest : Could not find java executable ----");
				return;
			}

			// create a logging file for the process
			File tempLogFile = File.createTempFile("testlogconfig", "properties");
			tempLogFile.deleteOnExit();
			CommonTestUtils.printLog4jDebugConfig(tempLogFile);

			// start a JobManger process
			// the log level must be at least INFO, otherwise the bound port cannot be retrieved
			String[] command = new String[] {
					javaCommand,
					"-Dlog.level=DEBUG",
					"-Dlog4j.configuration=file:" + tempLogFile.getAbsolutePath(),
					"-Xms256m", "-Xmx256m",
					"-classpath", getCurrentClasspath(),
					JobManagerTestEntryPoint.class.getName()
			};

			// spawn the process and collect its output
			ProcessBuilder bld = new ProcessBuilder(command);
			jmProcess = bld.start();
			new PipeForwarder(jmProcess.getErrorStream(), processOutput);

			// start another actor system so we can send something to the JobManager
			Tuple2<String, Object> localAddress = new Tuple2<String, Object>("localhost", 0);
			localSystem = AkkaUtils.createActorSystem(
					new Configuration(), new Some<Tuple2<String, Object>>(localAddress));

			// grab the reference to the JobManager. try multiple times, until the process
			// is started and the JobManager is up
			ActorRef jobManagerRef = null;
			Throwable lastError = null;

			// Log message on JobManager must be: Starting JobManager at ...://flink@...:port/..."
			// otherwise, the pattern does not match and, thus, cannot retrieve the bound port
			String pattern = "Starting JobManager at [^:]*://flink@[^:]*:(\\d*)/";
			Pattern r = Pattern.compile(pattern);
			int jobManagerPort = -1;

			for (int i = 0; i < 40; i++) {
				Matcher m = r.matcher(processOutput.toString());

				if (m.find()) {
					jobManagerPort = Integer.parseInt(m.group(1));
					break;
				}

				Thread.sleep(500);
			}

			if (jobManagerPort != -1) {
				try {
					jobManagerRef = JobManager.getJobManagerActorRef(
						new InetSocketAddress("localhost", jobManagerPort),
						localSystem, new FiniteDuration(25, TimeUnit.SECONDS));
				} catch (Throwable t) {
					// job manager probably not ready yet
					lastError = t;
				}
			} else {
				fail("Could not determine port of started JobManager.");
			}

			assertTrue("JobManager process died", isProcessAlive(jmProcess));

			if (jobManagerRef == null) {
				if (lastError != null) {
					lastError.printStackTrace();
				}
				fail("JobManager process did not launch the JobManager properly. Failed to look up JobManager actor at"
						+ " localhost:" + jobManagerPort);
			}

			// kill the JobManager actor
			jobManagerRef.tell(PoisonPill.getInstance(), ActorRef.noSender());

			// wait for max 5 seconds for the process to terminate
			{
				long now = System.currentTimeMillis();
				long deadline = now + 5000;

				while (now < deadline && isProcessAlive(jmProcess)) {
					Thread.sleep(100);
					now = System.currentTimeMillis();
				}
			}

			assertFalse("JobManager process did not terminate upon actor death", isProcessAlive(jmProcess));

			int returnCode = jmProcess.exitValue();
			assertEquals("JobManager died, but not because of the process reaper",
					JobManager.RUNTIME_FAILURE_RETURN_CODE(), returnCode);
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
			if (jmProcess != null) {
				jmProcess.destroy();
			}
			if (localSystem != null) {
				localSystem.shutdown();
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

	public static class JobManagerTestEntryPoint {

		public static void main(String[] args) {
			try {
				Configuration config = new Configuration();
				config.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, -1);

				JobManager.runJobManager(config, JobManagerMode.CLUSTER, "localhost", 0);
				System.exit(0);
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
