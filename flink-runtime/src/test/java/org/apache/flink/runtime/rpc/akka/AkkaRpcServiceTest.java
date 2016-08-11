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

package org.apache.flink.runtime.rpc.akka;

import akka.actor.ActorSystem;
import akka.util.Timeout;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.jobmaster.JobMaster;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManager;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AkkaRpcServiceTest extends TestLogger {

	/**
	 * Tests that the {@link JobMaster} can connect to the {@link ResourceManager} using the
	 * {@link AkkaRpcService}.
	 */
	@Test
	public void testJobMasterResourceManagerRegistration() throws Exception {
		Timeout akkaTimeout = new Timeout(10, TimeUnit.SECONDS);
		ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();
		ActorSystem actorSystem2 = AkkaUtils.createDefaultActorSystem();
		AkkaRpcService akkaRpcService = new AkkaRpcService(actorSystem, akkaTimeout);
		AkkaRpcService akkaRpcService2 = new AkkaRpcService(actorSystem2, akkaTimeout);
		ExecutorService executorService = new ForkJoinPool();

		ResourceManager resourceManager = new ResourceManager(akkaRpcService, executorService);
		JobMaster jobMaster = new JobMaster(akkaRpcService2, executorService);

		ResourceManagerGateway rm = resourceManager.getSelf();

		assertTrue(rm instanceof AkkaGateway);

		AkkaGateway akkaClient = (AkkaGateway) rm;

		
		jobMaster.registerAtResourceManager(AkkaUtils.getAkkaURL(actorSystem, akkaClient.getRpcServer()));

		// wait for successful registration
		FiniteDuration timeout = new FiniteDuration(200, TimeUnit.SECONDS);
		Deadline deadline = timeout.fromNow();

		while (deadline.hasTimeLeft() && !jobMaster.isConnected()) {
			Thread.sleep(100);
		}

		assertFalse(deadline.isOverdue());

		jobMaster.shutDown();
		resourceManager.shutDown();
	}
}
