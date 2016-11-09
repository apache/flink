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

package org.apache.flink.runtime.instance;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.AskTimeoutException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AkkaActorGatewayTest extends TestLogger {

	private static ActorSystem actorSystem;

	@BeforeClass
	public static void setup() {
		actorSystem = AkkaUtils.createDefaultActorSystem();
	}

	@AfterClass
	public static void tearDown() {
		if (actorSystem != null) {
			actorSystem.shutdown();
			actorSystem.awaitTermination(AkkaUtils.getDefaultTimeoutAsFiniteDuration());
		}
	}

	/**
	 * Tests that an early timeout is triggered if the remote actor system
	 * - is not reachable
	 * - does not contain the specified actor
	 */
	@Test
	public void testEarlyTimeout() throws Exception {
		ActorSystem remoteActorSystem = AkkaUtils.createDefaultActorSystem();
		final UUID leaderId = UUID.randomUUID();
		final Deadline deadline = new FiniteDuration(20L, TimeUnit.SECONDS).fromNow();
		final int message = 42;

		try {
			ActorRef simpleActor = remoteActorSystem.actorOf(Props.create(SimpleActor.class, leaderId));

			final String simpleActorPath = AkkaUtils.getAkkaURL(remoteActorSystem, simpleActor);

			ActorSelection selection = actorSystem.actorSelection(simpleActorPath);

			ActorRef remoteSimpleActor = Await.result(selection.resolveOne(deadline.timeLeft()), deadline.timeLeft());

			final AkkaActorGateway remoteSimpleActorGateway = new AkkaActorGateway(remoteSimpleActor, leaderId, actorSystem.dispatcher());

			// test successful ask operation
			Future<Object> futureResponse = remoteSimpleActorGateway.ask(message, deadline.timeLeft());
			Object response = Await.result(futureResponse, deadline.timeLeft());

			assertEquals(message, response);

			// kill remote actor to test timeout
			remoteActorSystem.stop(simpleActor);

			// Test non reachable actor
			futureResponse = remoteSimpleActorGateway.ask(message, deadline.timeLeft());

			try {
				// make sure that we generate a timeout before the ask timeout kicks in to test
				// that we can detect a non reachable actor early
				Await.result(futureResponse, deadline.timeLeft().div(2.0));
				fail("We expected an AskTimeoutException here.");
			} catch (AskTimeoutException ate) {
				// we expected an ask timeout
			} catch (TimeoutException te) {
				// this timeout exception originates from the Await.result, thus the early timeout
				// did not fire
				fail("Await.result timed out even though we expected an early ask timeout.");
			}

			remoteActorSystem.shutdown();
			remoteActorSystem.awaitTermination(deadline.timeLeft());

			// Test non reachable actor system
			futureResponse = remoteSimpleActorGateway.ask(message, deadline.timeLeft());

			try {
				// make sure that we generate a timeout before the ask timeout kicks in to test
				// that we can detect a non reachable actor early
				Await.result(futureResponse, deadline.timeLeft().div(2.0));
				fail("We expected an AskTimeoutException here.");
			} catch (AskTimeoutException ate) {
				// we expected an ask timeout
			} catch (TimeoutException te) {
				// this timeout exception originates from the Await.result, thus the early timeout
				// did not fire
				fail("Await.result timed out even though we expected an early ask timeout.");
			}

		} finally {
			remoteActorSystem.shutdown();
			remoteActorSystem.awaitTermination(AkkaUtils.getDefaultTimeoutAsFiniteDuration());
		}
	}

	private static class SimpleActor extends FlinkUntypedActor {

		private final UUID leaderId;

		SimpleActor(UUID leaderId) {
			this.leaderId = leaderId;
		}

		@Override
		protected void handleMessage(Object message) throws Exception {
			sender().tell(message, getSelf());
		}

		@Override
		protected UUID getLeaderSessionID() {
			return leaderId;
		}
	}
}
