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
import org.apache.flink.runtime.akka.AkkaUtils;
import org.junit.Test;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Tests that the JobManager process properly exits when the JobManager actor dies.
 */
public class JobManagerProcessReapingTest {

	private static final int JOB_MANAGER_PORT = 56532;

	@Test
	public void testReapProcessOnFailure() {
		Process jmProcess = null;
		ActorSystem localSystem = null;

		try {
			String javaCommand = getJavaCommandPath();

			// check that we run this test only if the java command
			// is available on this machine
			if (javaCommand == null) {
				return;
			}

			// start a JobManger process
			String[] command = new String[] {
					javaCommand,
					"-Dlog.level=OFF",
					"-Xms256m", "-Xmx256m",
					"-classpath", getCurrentClasspath(),
					JobManagerTestEntryPoint.class.getName()
			};

			ProcessBuilder bld = new ProcessBuilder(command);
			jmProcess = bld.start();

			// start another actor system so we can send something to the JobManager
			Tuple2<String, Object> localAddress = new Tuple2<String, Object>("localhost", 0);
			localSystem = AkkaUtils.createActorSystem(
					new Configuration(), new Some<Tuple2<String, Object>>(localAddress));

			// grab the reference to the JobManager. try multiple times, until the process
			// is started and the JobManager is up
			ActorRef jobManagerRef = null;
			for (int i = 0; i < 20; i++) {
				try {
					jobManagerRef = JobManager.getJobManagerRemoteReference(
							new InetSocketAddress("localhost", JOB_MANAGER_PORT),
							localSystem, new FiniteDuration(20, TimeUnit.SECONDS));
					break;
				}
				catch (Throwable t) {
					// job manager probably not ready yet
				}
				Thread.sleep(500);
			}

			assertTrue("JobManager process died", isProcessAlive(jmProcess));
			assertTrue("JobManager process did not launch the JobManager properly", jobManagerRef != null);

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
			fail(e.getMessage());
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

	// --------------------------------------------------------------------------------------------

	public static class JobManagerTestEntryPoint {

		public static void main(String[] args) {
			try {
				Configuration cfg = new Configuration();
				cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
				cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, JOB_MANAGER_PORT);

				JobManager.runJobManager(cfg, ExecutionMode.CLUSTER(), "localhost", JOB_MANAGER_PORT);
				System.exit(0);
			}
			catch (Throwable t) {
				System.exit(1);
			}
		}
	}
}
