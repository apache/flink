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

package org.apache.flink.runtime.rpc.jobmaster;

import akka.actor.ActorSystem;
import akka.util.Timeout;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.mock;

public class JobMasterTest {
	private TestingServer testingServer;

	@Before
	public void before() {
		try {
			testingServer = new TestingServer();
			testingServer.start();
		} catch (Exception e) {
			throw new RuntimeException("Could not start ZooKeeper testing cluster.", e);
		}
	}

	@After
	public void after() {
		if(testingServer != null) {
			try {
				testingServer.stop();
			} catch (IOException e) {
				throw new RuntimeException("Could not stop ZooKeeper testing cluster.", e);
			}
			testingServer = null;
		}
	}

	/**
	 * Test that JM can successfully grant leadership from Standalone election service.
	 */
	@Test
	public void testGrantLeadershipFromStandaloneLeadershipElectionService() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(ConfigConstants.RECOVERY_MODE, "standalone");

		Timeout akkaTimeout = new Timeout(10, TimeUnit.SECONDS);
		ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();
		RpcService testingRpcService = new AkkaRpcService(actorSystem, akkaTimeout);
		ExecutorService testingExecutorService = new ForkJoinPool();

		JobMaster jobMaster = new JobMaster(configuration, testingRpcService, testingExecutorService);

		UUID sessionID = UUID.randomUUID();
		jobMaster.grantLeadership(sessionID);


		UUID grantedSessionID = Whitebox.getInternalState(jobMaster, "leaderSessionID");
		assertEquals(sessionID, grantedSessionID);
	}

	/**
	 * Test that JM can successfull grant leadership from Zookeeper election service.
	 */
	@Test
	public void testGrantLeadershipFromZookeeperLeadershipElectionService() throws Exception {
		TemporaryFolder tmpFolder = new TemporaryFolder();
		tmpFolder.create();

		Configuration configuration = ZooKeeperTestUtils
			.createZooKeeperRecoveryModeConfig(
				testingServer.getConnectString(),
				tmpFolder.getRoot().getPath());

		Timeout akkaTimeout = new Timeout(10, TimeUnit.SECONDS);
		ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();
		RpcService testingRpcService = new AkkaRpcService(actorSystem, akkaTimeout);
		ExecutorService testingExecutorService = new ForkJoinPool();

		JobMaster jobMaster = new JobMaster(configuration, testingRpcService, testingExecutorService);

		UUID sessionID = UUID.randomUUID();
		jobMaster.grantLeadership(sessionID);


		UUID grantedSessionID = Whitebox.getInternalState(jobMaster, "leaderSessionID");
		assertEquals(sessionID, grantedSessionID);
	}
}
