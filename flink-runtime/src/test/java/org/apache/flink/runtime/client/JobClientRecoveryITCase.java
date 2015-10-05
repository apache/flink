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

package org.apache.flink.runtime.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.StreamingMode;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class JobClientRecoveryITCase extends TestLogger {

	protected final static Logger LOG = LoggerFactory.getLogger(JobClientRecoveryITCase.class);

	private final static ZooKeeperTestEnvironment ZooKeeper = new ZooKeeperTestEnvironment(1);

	private final static File FileStateBackendBasePath;

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

	/**
	 * Tests that the client updates the actor with the new leader job manager in case of an error.
	 * There are three job managers in total, two of which are killed after they become leader in
	 * the course of this test.
	 */
	@Test
	public void testJobManagerConnectionLoss() throws Exception {
		final Configuration config = ZooKeeperTestUtils.createZooKeeperRecoveryModeConfig(
				ZooKeeper.getConnectString(), FileStateBackendBasePath.getPath());

		JobManagerProcess[] jobManagerProcess = new JobManagerProcess[3];
		ActorSystem taskManagerActorSystem = null;
		ActorSystem testActorSystem = null;
		LeaderRetrievalService leaderRetrievalService = null;

		final Deadline deadline = new FiniteDuration(10, TimeUnit.MINUTES).fromNow();

		try {
			// Job manager
			for (int i = 0; i < jobManagerProcess.length; i++) {
				jobManagerProcess[i] = new JobManagerProcess(i, config);
				jobManagerProcess[i].createAndStart();

				LOG.info("Started {}.", jobManagerProcess[i]);
			}

			// Task manager
			taskManagerActorSystem = AkkaUtils.createDefaultActorSystem();
			TaskManager.startTaskManagerComponentsAndActor(config, taskManagerActorSystem,
					"localhost", Option.<String>empty(), Option.<LeaderRetrievalService>empty(),
					false, StreamingMode.STREAMING, TaskManager.class);

			LOG.info("Started taskmanager.");

			testActorSystem = AkkaUtils.createDefaultActorSystem();

			// Leader listener
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(config);
			TestingListener leaderListener = new TestingListener();
			leaderRetrievalService.start(leaderListener);

			LOG.info("Waiting for leader retrieval.");

			// Who da boss?
			leaderListener.waitForNewLeader(deadline.timeLeft().toMillis());

			LOG.info("Leader determined as {}. Trying to determine, which job manager process this is.",
					leaderListener.getAddress());

			int leaderIndex = determineLeaderIndex(leaderListener.getAddress(), jobManagerProcess);

			LOG.info("Leader determined as process {}.", leaderIndex);

			ActorRef leaderRef = jobManagerProcess[leaderIndex]
					.getActorRef(testActorSystem, deadline.timeLeft());

			final AkkaActorGateway leaderGateway = new AkkaActorGateway(leaderRef,
					leaderListener.getLeaderSessionID());

			LOG.info("Waiting for task manager to connect.");

			// Wait for the task manager to connect
			JobManagerActorTestUtils.waitForTaskManagers(1, leaderGateway, deadline.timeLeft());

			LOG.info("Task manager connected.");

			// Blocking job graph
			final JobGraph jobGraph = new JobGraph("blocking job graph");
			JobVertex vertex = new JobVertex("blocking vertex");
			vertex.setInvokableClass(BlockingTwiceTask.class);
			vertex.setParallelism(1);
			jobGraph.addVertex(vertex);

			// Create job client
			final AtomicReference<Throwable> asyncError = new AtomicReference<>();

			final ActorSystem jobClientActorSystem = testActorSystem;

			Thread submitter = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						LOG.info("Submitting blocking twice job.");

						JobClient.submitJobAndWait(jobClientActorSystem, config, leaderGateway,
								jobGraph, deadline.timeLeft(), false, ClassLoader.getSystemClassLoader());

						LOG.info("Job submission returned.");
					}
					catch (Throwable t) {
						asyncError.set(t);
					}
				}
			});

			submitter.start();

			// Wait for the job to start running
			LOG.info("Waiting for first execution of job graph.");

			// Wait for the task to enter the branch for the first time before killing the leader
			while (BlockingTwiceTask.HasBlockedExecution != 1) {
				Thread.sleep(100);
			}

			LOG.info("Killing the leader.");

			// Kill the leader
			jobManagerProcess[leaderIndex].destroy();

			LOG.info("Waiting for leader retrieval");

			leaderListener.waitForNewLeader(deadline.timeLeft().toMillis());

			LOG.info("Leader determined as {}. Trying to determine, which job manager process this is.",
					leaderListener.getAddress());

			leaderIndex = determineLeaderIndex(leaderListener.getAddress(), jobManagerProcess);

			LOG.info("Leader determined as process {}", leaderIndex);

			LOG.info("Waiting for second execution of job graph.");

			// Wait for the task to enter the branch for the second time before killing the leader
			while (BlockingTwiceTask.HasBlockedExecution != 2) {
				Thread.sleep(100);
			}

			LOG.info("Killing the leader.");

			// Kill the new leader. Do this for the second time to make sure that the second leader
			// is watched by the client as well.
			jobManagerProcess[leaderIndex].destroy();

			LOG.info("Waiting for job submission to return.");

			// Wait for submitter thread to finish after recovery (see blocking twice task)
			submitter.join();

			Throwable t = asyncError.get();
			if (t != null) {
				throw t;
			}
		}
		catch (Throwable t) {
			for (JobManagerProcess jobManager : jobManagerProcess) {
				if (jobManager != null) {
					jobManager.printProcessLog();
				}
			}

			t.printStackTrace();
		}
		finally {
			for (JobManagerProcess jobManager : jobManagerProcess) {
				if (jobManager != null) {
					jobManager.destroy();
				}
			}

			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}

			if (taskManagerActorSystem != null) {
				taskManagerActorSystem.shutdown();
			}

			if (testActorSystem != null) {
				testActorSystem.shutdown();
			}
		}
	}

	private int determineLeaderIndex(String leaderAddress, JobManagerProcess[] jobManagerProcesses) {
		for (int i = 0; i < jobManagerProcesses.length; i++) {
			JobManagerProcess jm = jobManagerProcesses[i];

			if (jm.getJobManagerAkkaURL().equals(leaderAddress)) {
				return i;
			}
		}

		throw new IllegalStateException("Failed to determine leading process");
	}

	public static class BlockingTwiceTask extends AbstractInvokable {

		private volatile static int BlockExecution = 2;
		private volatile static int HasBlockedExecution = 0;

		@Override
		public void registerInputOutput() throws Exception {
			// Nothing to do
		}

		@Override
		public void invoke() throws Exception {
			if (BlockExecution > 0) {
				BlockExecution--;

				// Tell the test that it's OK to kill the leader
				HasBlockedExecution++;
				new CountDownLatch(1).await();
			}
		}
	}
}
