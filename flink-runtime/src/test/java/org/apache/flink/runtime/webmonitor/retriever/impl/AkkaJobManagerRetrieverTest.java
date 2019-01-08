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

package org.apache.flink.runtime.webmonitor.retriever.impl;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClientActorTest;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.concurrent.Await;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test for the {@link AkkaJobManagerRetriever}.
 */
public class AkkaJobManagerRetrieverTest extends TestLogger {

	private static final Time timeout = Time.seconds(10L);
	private static ActorSystem actorSystem;

	@BeforeClass
	public static void setup() {
		actorSystem = AkkaUtils.createDefaultActorSystem();
	}

	@AfterClass
	public static void teardown() throws InterruptedException, TimeoutException {
		if (actorSystem != null) {
			actorSystem.terminate();
			Await.ready(actorSystem.whenTerminated(), FutureUtils.toFiniteDuration(timeout));

			actorSystem = null;
		}
	}

	/**
	 * Tests that we can retrieve the current leading job manager.
	 */
	@Test
	public void testAkkaJobManagerRetrieval() throws Exception {
		AkkaJobManagerRetriever akkaJobManagerRetriever = new AkkaJobManagerRetriever(actorSystem, timeout, 0, Time.milliseconds(0L));
		SettableLeaderRetrievalService settableLeaderRetrievalService = new SettableLeaderRetrievalService();

		CompletableFuture<JobManagerGateway> gatewayFuture = akkaJobManagerRetriever.getFuture();
		final UUID leaderSessionId = UUID.randomUUID();

		ActorRef actorRef = null;

		try {
			actorRef = actorSystem.actorOf(
				Props.create(JobClientActorTest.PlainActor.class, leaderSessionId));

			final String address = actorRef.path().toString();

			settableLeaderRetrievalService.start(akkaJobManagerRetriever);

			// check that the gateway future has not been completed since there is no leader yet
			assertFalse(gatewayFuture.isDone());

			settableLeaderRetrievalService.notifyListener(address, leaderSessionId);

			JobManagerGateway jobManagerGateway = gatewayFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertEquals(address, jobManagerGateway.getAddress());
		} finally {
			settableLeaderRetrievalService.stop();

			if (actorRef != null) {
				TestingUtils.stopActorGracefully(actorRef);
			}
		}
	}
}
