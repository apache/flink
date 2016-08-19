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

package org.apache.flink.runtime.rpc.resourcemanager;

import akka.actor.ActorSystem;
import akka.dispatch.OnSuccess;
import akka.testkit.JavaTestKit;
import akka.util.Timeout;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.NonHaServices;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.jobmaster.JobMaster;
import org.apache.flink.runtime.rpc.taskexecutor.SlotReport;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResourceManagerToTaskExecutorHeartbeatSchedulerTest extends TestLogger {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private static TestingServer testingServer;

	private static ActorSystem actorSystem;
	private static ActorSystem actorSystem2;
	private static AkkaRpcService akkaRpcService;
	private static AkkaRpcService akkaRpcService2;

	private static final Timeout timeout = new Timeout(10000, TimeUnit.MILLISECONDS);

	@BeforeClass
	public static void setup() throws Exception {
		testingServer = new TestingServer();

		actorSystem = AkkaUtils.createDefaultActorSystem();
		actorSystem2 = AkkaUtils.createDefaultActorSystem();

		akkaRpcService = new AkkaRpcService(actorSystem, timeout);
		akkaRpcService2 = new AkkaRpcService(actorSystem2, timeout);
	}

	@AfterClass
	public static void teardown() throws Exception {
		akkaRpcService.stopService();
		akkaRpcService2.stopService();

		actorSystem.shutdown();
		actorSystem2.shutdown();

		actorSystem.awaitTermination();
		actorSystem2.awaitTermination();

		testingServer.stop();
	}

	@Test
	public void testStart() throws Exception {
		ExecutorService executorService = new ForkJoinPool();

		Configuration configuration = ZooKeeperTestUtils
			.createZooKeeperRecoveryModeConfig(
				testingServer.getConnectString(),
				tempFolder.getRoot().getPath());

		CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
		LeaderElectionService leaderElectionService = ZooKeeperUtils.createLeaderElectionService(client,
			configuration);
		ResourceManager resourceManager = new ResourceManager(akkaRpcService, executorService, leaderElectionService);

		HighAvailabilityServices highAvailabilityServices = new NonHaServices(resourceManager.getAddress());
		ResourceID resourceID = ResourceID.generate();
		TaskExecutor taskExecutor = new TaskExecutor(akkaRpcService2, highAvailabilityServices, resourceID);

		resourceManager.start();
		taskExecutor.start();

		ResourceManagerToTaskExecutorHeartbeatScheduler heartbeatScheduler = new ResourceManagerToTaskExecutorHeartbeatScheduler(resourceManager, resourceManager.getLeaderSessionID(), taskExecutor.getSelf(), taskExecutor.getAddress(), resourceID, log);
		heartbeatScheduler.start();
		Assert.assertFalse(heartbeatScheduler.isClosed());
		// if heartbeat scheduler cannot receive heartbeat response from taskExecutor for maxAttempt times, it will close itself.
		// else it will continue to trigger next heartbeat in the given interval milliseconds
		FiniteDuration timeout = new FiniteDuration(40, TimeUnit.SECONDS);
		Deadline deadline = timeout.fromNow();

		while (deadline.hasTimeLeft() && !heartbeatScheduler.isClosed()) {
			Thread.sleep(100);
		}

		Assert.assertFalse(heartbeatScheduler.isClosed());

		heartbeatScheduler.close();
		Assert.assertTrue(heartbeatScheduler.isClosed());
	}
}
