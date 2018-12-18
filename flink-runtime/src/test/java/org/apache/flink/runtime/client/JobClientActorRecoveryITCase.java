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

import akka.actor.PoisonPill;
import org.apache.curator.test.TestingServer;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.concurrent.Await;
import scala.concurrent.Promise;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.util.concurrent.TimeUnit;


public class JobClientActorRecoveryITCase extends TestLogger {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public static TestingServer zkServer;

	@BeforeClass
	public static void setup() throws Exception {
		zkServer = new TestingServer();

		zkServer.start();
	}

	public static void teardown() throws Exception {
		if (zkServer != null) {
			zkServer.stop();
			zkServer = null;
		}
	}

	/**
	 * Tests whether the JobClientActor can connect to a newly elected leading job manager to obtain
	 * the JobExecutionResult. The submitted job blocks for the first execution attempt. The
	 * leading job manager will be killed so that the second job manager will be elected as the
	 * leader. The newly elected leader has to retrieve the checkpointed job from ZooKeeper
	 * and continue its execution. This time, the job does not block and, thus, can be finished.
	 * The execution result should be sent to the JobClientActor which originally submitted the
	 * job.
	 *
	 * @throws Exception
	 */
	@Test
	public void testJobClientRecovery() throws Exception {
		File rootFolder = tempFolder.getRoot();

		Configuration config = ZooKeeperTestUtils.createZooKeeperHAConfig(
			zkServer.getConnectString(),
			rootFolder.getPath());

		config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, 2);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);

		final TestingCluster cluster = new TestingCluster(config);
		cluster.start();

		JobVertex blockingVertex = new JobVertex("Blocking Vertex");
		blockingVertex.setInvokableClass(BlockingTask.class);
		blockingVertex.setParallelism(1);
		final JobGraph jobGraph = new JobGraph("Blocking Test Job", blockingVertex);
		final Promise<JobExecutionResult> promise = new scala.concurrent.impl.Promise.DefaultPromise<>();

		Deadline deadline = new FiniteDuration(2, TimeUnit.MINUTES).fromNow();

		try {
			Thread submitter = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						JobExecutionResult result = cluster.submitJobAndWait(jobGraph, false);
						promise.success(result);
					} catch (Exception e) {
						promise.failure(e);
					}
				}
			});

			submitter.start();

			synchronized (BlockingTask.waitLock) {
				while (BlockingTask.HasBlockedExecution < 1 && deadline.hasTimeLeft()) {
					BlockingTask.waitLock.wait(deadline.timeLeft().toMillis());
				}
			}

			if (deadline.isOverdue()) {
				Assert.fail("The job has not blocked within the given deadline.");
			}

			ActorGateway gateway = cluster.getLeaderGateway(deadline.timeLeft());

			gateway.tell(TestingJobManagerMessages.getDisablePostStop());
			gateway.tell(PoisonPill.getInstance());

			// if the job fails then an exception is thrown here
			Await.result(promise.future(), deadline.timeLeft());
		} finally {
			cluster.stop();
		}
	}

	public static class BlockingTask extends AbstractInvokable {

		private volatile static int BlockExecution = 1;
		private volatile static int HasBlockedExecution = 0;
		private static Object waitLock = new Object();

		public BlockingTask(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			if (BlockExecution > 0) {
				BlockExecution--;

				// Tell the test that it's OK to kill the leader
				synchronized (waitLock) {
					HasBlockedExecution++;
					waitLock.notifyAll();
				}
			}
		}
	}
}
