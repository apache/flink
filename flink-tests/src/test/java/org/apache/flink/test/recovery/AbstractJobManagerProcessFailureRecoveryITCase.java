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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.JobManagerActorTestUtils;
import org.apache.flink.runtime.testutils.JobManagerProcess;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import scala.Option;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.testutils.CommonTestUtils.createTempDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Verify behaviour in case of JobManager process failure during job execution.
 *
 * <p>The test works with multiple job managers processes by spawning JVMs.
 *
 * <p>Initially, it starts two TaskManager (2 slots each) and two JobManager JVMs.
 *
 * <p>It submits a program with parallelism 4 and waits until all tasks are brought up.
 * Coordination between the test and the tasks happens via checking for the existence of
 * temporary files. It then kills the leading JobManager process. The recovery should restart the
 * tasks on the new JobManager.
 *
 * <p>This follows the same structure as {@link AbstractTaskManagerProcessFailureRecoveryTest}.
 */
public abstract class AbstractJobManagerProcessFailureRecoveryITCase extends TestLogger {

	private final static ZooKeeperTestEnvironment ZooKeeper = new ZooKeeperTestEnvironment(1);

	private final static FiniteDuration TestTimeOut = new FiniteDuration(5, TimeUnit.MINUTES);

	private static final File FileStateBackendBasePath;

	static {
		try {
			FileStateBackendBasePath = CommonTestUtils.createTempDirectory();
		}
		catch (IOException e) {
			throw new RuntimeException("Error in test setup. Could not create directory.", e);
		}
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (ZooKeeper != null) {
			ZooKeeper.shutdown();
		}

		if (FileStateBackendBasePath != null) {
			FileUtils.deleteDirectory(FileStateBackendBasePath);
		}
	}

	@Before
	public void cleanUp() throws Exception {
		ZooKeeper.deleteAll();

		FileUtils.cleanDirectory(FileStateBackendBasePath);
	}

	protected static final String READY_MARKER_FILE_PREFIX = "ready_";
	protected static final String FINISH_MARKER_FILE_PREFIX = "finish_";
	protected static final String PROCEED_MARKER_FILE = "proceed";

	protected static final int PARALLELISM = 4;

	/**
	 * Test program with JobManager failure.
	 *
	 * @param zkQuorum ZooKeeper quorum to connect to
	 * @param coordinateDir Coordination directory
	 * @throws Exception
	 */
	public abstract void testJobManagerFailure(String zkQuorum, File coordinateDir) throws Exception;

	@Test
	public void testJobManagerProcessFailure() throws Exception {
		// Config
		final int numberOfJobManagers = 2;
		final int numberOfTaskManagers = 2;
		final int numberOfSlotsPerTaskManager = 2;

		assertEquals(PARALLELISM, numberOfTaskManagers * numberOfSlotsPerTaskManager);

		// Setup
		// Test actor system
		ActorSystem testActorSystem;

		// Job managers
		final JobManagerProcess[] jmProcess = new JobManagerProcess[numberOfJobManagers];

		// Task managers
		final ActorSystem[] tmActorSystem = new ActorSystem[numberOfTaskManagers];

		// Leader election service
		LeaderRetrievalService leaderRetrievalService = null;

		// Coordination between the processes goes through a directory
		File coordinateTempDir = null;

		try {
			final Deadline deadline = TestTimeOut.fromNow();

			// Coordination directory
			coordinateTempDir = createTempDirectory();

			// Job Managers
			Configuration config = ZooKeeperTestUtils.createZooKeeperRecoveryModeConfig(
					ZooKeeper.getConnectString(), FileStateBackendBasePath.getPath());

			// Start first process
			jmProcess[0] = new JobManagerProcess(0, config);
			jmProcess[0].createAndStart();

			// Task manager configuration
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 4);
			config.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 100);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);

			// Start the task manager process
			for (int i = 0; i < numberOfTaskManagers; i++) {
				tmActorSystem[i] = AkkaUtils.createActorSystem(AkkaUtils.getDefaultAkkaConfig());
				TaskManager.startTaskManagerComponentsAndActor(
						config, tmActorSystem[i], "localhost",
						Option.<String>empty(), Option.<LeaderRetrievalService>empty(),
						false, TaskManager.class);
			}

			// Test actor system
			testActorSystem = AkkaUtils.createActorSystem(AkkaUtils.getDefaultAkkaConfig());

			jmProcess[0].getActorRef(testActorSystem, deadline.timeLeft());

			// Leader listener
			TestingListener leaderListener = new TestingListener();
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(config);
			leaderRetrievalService.start(leaderListener);

			// Initial submission
			leaderListener.waitForNewLeader(deadline.timeLeft().toMillis());

			String leaderAddress = leaderListener.getAddress();
			UUID leaderId = leaderListener.getLeaderSessionID();

			// Get the leader ref
			ActorRef leaderRef = AkkaUtils.getActorRef(leaderAddress, testActorSystem, deadline.timeLeft());
			ActorGateway leaderGateway = new AkkaActorGateway(leaderRef, leaderId);

			// Wait for all task managers to connect to the leading job manager
			JobManagerActorTestUtils.waitForTaskManagers(numberOfTaskManagers, leaderGateway,
					deadline.timeLeft());

			final File coordinateDirClosure = coordinateTempDir;
			final Throwable[] errorRef = new Throwable[1];

			// we trigger program execution in a separate thread
			Thread programTrigger = new Thread("Program Trigger") {
				@Override
				public void run() {
					try {
						testJobManagerFailure(ZooKeeper.getConnectString(), coordinateDirClosure);
					}
					catch (Throwable t) {
						t.printStackTrace();
						errorRef[0] = t;
					}
				}
			};

			//start the test program
			programTrigger.start();

			// wait until all marker files are in place, indicating that all tasks have started
			AbstractTaskManagerProcessFailureRecoveryTest.waitForMarkerFiles(coordinateTempDir,
					READY_MARKER_FILE_PREFIX, PARALLELISM, deadline.timeLeft().toMillis());

			// Kill one of the job managers and trigger recovery
			jmProcess[0].destroy();

			jmProcess[1] = new JobManagerProcess(1, config);
			jmProcess[1].createAndStart();

			jmProcess[1].getActorRef(testActorSystem, deadline.timeLeft());

			// we create the marker file which signals the program functions tasks that they can complete
			AbstractTaskManagerProcessFailureRecoveryTest.touchFile(new File(coordinateTempDir, PROCEED_MARKER_FILE));

			programTrigger.join(deadline.timeLeft().toMillis());

			// We wait for the finish marker file. We don't wait for the program trigger, because
			// we submit in detached mode.
			AbstractTaskManagerProcessFailureRecoveryTest.waitForMarkerFiles(coordinateTempDir,
					FINISH_MARKER_FILE_PREFIX, 1, deadline.timeLeft().toMillis());

			// check that the program really finished
			assertFalse("The program did not finish in time", programTrigger.isAlive());

			// check whether the program encountered an error
			if (errorRef[0] != null) {
				Throwable error = errorRef[0];
				error.printStackTrace();
				fail("The program encountered a " + error.getClass().getSimpleName() + " : " + error.getMessage());
			}
		}
		catch (Exception e) {
			e.printStackTrace();

			for (JobManagerProcess p : jmProcess) {
				if (p != null) {
					p.printProcessLog();
				}
			}

			fail(e.getMessage());
		}
		finally {
			for (int i = 0; i < numberOfTaskManagers; i++) {
				if (tmActorSystem[i] != null) {
					tmActorSystem[i].shutdown();
				}
			}

			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}

			for (JobManagerProcess jmProces : jmProcess) {
				if (jmProces != null) {
					jmProces.destroy();
				}
			}

			// Delete coordination directory
			if (coordinateTempDir != null) {
				try {
					FileUtils.deleteDirectory(coordinateTempDir);
				}
				catch (Throwable ignored) {
				}
			}
		}
	}

}
