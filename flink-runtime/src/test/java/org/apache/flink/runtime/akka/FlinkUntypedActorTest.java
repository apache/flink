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

package org.apache.flink.runtime.akka;

import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Kill;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link FlinkUntypedActor}.
 */
public class FlinkUntypedActorTest {

	private static ActorSystem actorSystem;

	@BeforeClass
	public static void setup() {
		actorSystem = ActorSystem.create("TestingActorSystem", TestingUtils.testConfig());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(actorSystem);
	}

	/**
	 * Tests that LeaderSessionMessage messages with a wrong leader session ID are filtered
	 * out.
	 */
	@Test
	public void testLeaderSessionMessageFilteringOfFlinkUntypedActor() {
		final UUID leaderSessionID = UUID.randomUUID();
		final UUID oldSessionID = UUID.randomUUID();

		TestActorRef<PlainFlinkUntypedActor> actor = null;

		try {
			actor = TestActorRef.create(
					actorSystem, Props.create(PlainFlinkUntypedActor.class, leaderSessionID));

			final PlainFlinkUntypedActor underlyingActor = actor.underlyingActor();

			actor.tell(new JobManagerMessages.LeaderSessionMessage(leaderSessionID, 1), ActorRef.noSender());
			actor.tell(new JobManagerMessages.LeaderSessionMessage(oldSessionID, 2), ActorRef.noSender());
			actor.tell(new JobManagerMessages.LeaderSessionMessage(leaderSessionID, 2), ActorRef.noSender());
			actor.tell(1, ActorRef.noSender());

			assertEquals(3, underlyingActor.getMessageCounter());

		} finally {
			stopActor(actor);
		}
	}

	/**
	 * Tests that an exception is thrown, when the FlinkUntypedActore receives a message which
	 * extends {@link RequiresLeaderSessionID} and is not wrapped in a LeaderSessionMessage.
	 */
	@Test
	public void testThrowingExceptionWhenReceivingNonWrappedRequiresLeaderSessionIDMessage() {
		final UUID leaderSessionID = UUID.randomUUID();

		TestActorRef<PlainFlinkUntypedActor> actor = null;

		try {
			final Props props = Props.create(PlainFlinkUntypedActor.class, leaderSessionID);
			actor = TestActorRef.create(actorSystem, props);

			actor.receive(new JobManagerMessages.LeaderSessionMessage(leaderSessionID, 1));

			try {
				actor.receive(new PlainRequiresLeaderSessionID());

				fail("Expected an exception to be thrown, because a RequiresLeaderSessionID" +
						"message was sent without being wrapped in LeaderSessionMessage.");
			} catch (Exception e) {
				assertEquals("Received a message PlainRequiresLeaderSessionID " +
						"without a leader session ID, even though the message requires a " +
						"leader session ID.",
						e.getMessage());
			}

		} finally {
			stopActor(actor);
		}
	}

	private static void stopActor(ActorRef actor) {
		if (actor != null) {
			actor.tell(Kill.getInstance(), ActorRef.noSender());
		}
	}

	static class PlainFlinkUntypedActor extends FlinkUntypedActor {

		private UUID leaderSessionID;

		private int messageCounter;

		public PlainFlinkUntypedActor(UUID leaderSessionID) {

			this.leaderSessionID = leaderSessionID;
			this.messageCounter = 0;
		}

		@Override
		protected void handleMessage(Object message) throws Exception {
			messageCounter++;
		}

		@Override
		protected UUID getLeaderSessionID() {
			return leaderSessionID;
		}

		public int getMessageCounter() {
			return messageCounter;
		}
	}

	static class PlainRequiresLeaderSessionID implements RequiresLeaderSessionID {
		@Override
		public String toString() {
			return "PlainRequiresLeaderSessionID";
		}
	}
}
