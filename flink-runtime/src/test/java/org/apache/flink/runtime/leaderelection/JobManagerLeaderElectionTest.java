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

package org.apache.flink.runtime.leaderelection;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.testkit.JavaTestKit;
import akka.util.Timeout;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.StreamingMode;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.instance.InstanceManager;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.testingUtils.TestingJobManager;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.LeaderElectionUtils;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

public class JobManagerLeaderElectionTest extends TestLogger {

	private static ActorSystem actorSystem;
	private static TestingServer testingServer;
	private static Timeout timeout = new Timeout(TestingUtils.TESTING_DURATION());
	private static FiniteDuration duration = new FiniteDuration(5, TimeUnit.MINUTES);

	@BeforeClass
	public static void setup() throws Exception {
		actorSystem = ActorSystem.create("TestingActorSystem");

		testingServer = new TestingServer();
	}

	@AfterClass
	public static void teardown() throws Exception {
		if (actorSystem != null) {
			JavaTestKit.shutdownActorSystem(actorSystem);
		}

		if(testingServer != null) {
			testingServer.stop();
		}
	}

	/**
	 * Tests that a single JobManager is elected as the leader by ZooKeeper.
	 */
	@Test
	public void testLeaderElection() throws Exception {
		final Configuration configuration = new Configuration();

		configuration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");
		configuration.setString(
			ConfigConstants.ZOOKEEPER_QUORUM_KEY,
			testingServer.getConnectString());

		ActorRef jm = null;

		try {
			Props jmProps = createJobManagerProps(configuration);

			jm = actorSystem.actorOf(jmProps);

			Future<Object> leaderFuture = Patterns.ask(
					jm,
					TestingJobManagerMessages.getNotifyWhenLeader(),
					timeout);

			Await.ready(leaderFuture, duration);
		} finally {
			TestingUtils.stopActor(jm);
		}

	}

	/**
	 * Tests that a second JobManager is elected as the leader once the previous leader dies.
	 */
	@Test
	public void testLeaderReelection() throws Exception {
		final Configuration configuration = new Configuration();

		configuration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");
		configuration.setString(
			ConfigConstants.ZOOKEEPER_QUORUM_KEY,
			testingServer.getConnectString());

		ActorRef jm;
		ActorRef jm2 = null;

		try {
			Props jmProps = createJobManagerProps(configuration);

			jm = actorSystem.actorOf(jmProps);

			Future<Object> leaderFuture = Patterns.ask(
					jm,
					TestingJobManagerMessages.getNotifyWhenLeader(),
					timeout);

			Await.ready(leaderFuture, duration);

			Props jmProps2 = createJobManagerProps(configuration);

			jm2 = actorSystem.actorOf(jmProps2);

			jm.tell(PoisonPill.getInstance(), ActorRef.noSender());

			// now the second JobManager should be elected as the leader
			Future<Object> leader2Future = Patterns.ask(
					jm2,
					TestingJobManagerMessages.getNotifyWhenLeader(),
					timeout
			);

			Await.ready(leader2Future, duration);
		} finally {
			TestingUtils.stopActor(jm2);
		}
	}

	private Props createJobManagerProps(Configuration configuration) throws Exception {
		LeaderElectionService leaderElectionService = LeaderElectionUtils.
				createLeaderElectionService(configuration);

		return Props.create(
				TestingJobManager.class,
				configuration,
				TestingUtils.defaultExecutionContext(),
				new InstanceManager(),
				new Scheduler(TestingUtils.defaultExecutionContext()),
				new BlobLibraryCacheManager(new BlobServer(configuration), 10l),
				ActorRef.noSender(),
				1,
				1L,
				AkkaUtils.getDefaultTimeout(),
				StreamingMode.BATCH_ONLY,
				leaderElectionService
		);
	}
}
