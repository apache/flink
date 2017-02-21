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

package org.apache.flink.runtime.taskmanager.heartbeat;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import akka.testkit.TestProbe;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.taskmanager.heartbeat.messages.Heartbeat;
import org.apache.flink.runtime.taskmanager.heartbeat.messages.HeartbeatResponse;
import org.apache.flink.runtime.taskmanager.heartbeat.messages.HeartbeatTimeout;
import org.apache.flink.runtime.taskmanager.heartbeat.messages.RequestAccumulatorSnapshots;
import org.apache.flink.runtime.taskmanager.heartbeat.messages.UpdateAccumulatorSnapshots;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HeartbeatActorTest extends TestLogger {

	private static ActorSystem actorsSystem;

	private static final FiniteDuration timeout = new FiniteDuration(20L, TimeUnit.SECONDS);

	private static final Logger LOG = LoggerFactory.getLogger(HeartbeatActorTest.class);

	@BeforeClass
	public static void setup() {
		actorsSystem = AkkaUtils.createActorSystem(TestingUtils.getDefaultTestingActorSystemConfig());
	}

	@AfterClass
	public static void tearDown() {
		actorsSystem.shutdown();
		actorsSystem.awaitTermination(timeout);
	}

	/**
	 * Test whether the heartbeat actor sends heartbeat requests to the heartbeat target.
	 */
	@Test
	public void testHeartbeat() throws IOException {
		new JavaTestKit(actorsSystem) {{
			final FiniteDuration heartbeatInterval = new FiniteDuration(20L, TimeUnit.MILLISECONDS);
			final FiniteDuration initialHeartbeatPause = new FiniteDuration(100L, TimeUnit.MILLISECONDS);
			final FiniteDuration maxHeartbeatPause = initialHeartbeatPause;
			final int numHeartbeats = 10;


			final UUID leaderId = UUID.randomUUID();
			final InstanceID instanceId = new InstanceID();

			final ActorRef forwardingActor = actorsSystem.actorOf(
				Props.create(
					TestingUtils.ForwardingActor.class,
					getRef(),
					Option.apply(leaderId)),
				"ForwardingActor");

			final TestProbe taskManager = TestProbe.apply(actorsSystem);

			final JobID jobId = new JobID();
			final ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();

			final Collection<AccumulatorSnapshot> accumulatorSnapshots = Arrays.asList(
				new AccumulatorSnapshot(
					jobId,
					executionAttemptId,
					Collections.<String, Accumulator<?, ?>>emptyMap()));

			final ActorRef heartbeatActor = actorsSystem.actorOf(
				Props.create(
					HeartbeatActor.class,
					taskManager.ref(),
					instanceId,
					forwardingActor,
					leaderId,
					heartbeatInterval,
					initialHeartbeatPause,
					maxHeartbeatPause,
					log),
				"heartbeatActor");

			try {
				final Deadline deadline = timeout.fromNow();

				heartbeatActor.tell(new JobManagerMessages.LeaderSessionMessage(leaderId, new UpdateAccumulatorSnapshots(accumulatorSnapshots)), getRef());

				for (int i = 0; i < numHeartbeats && deadline.hasTimeLeft(); ++i) {
					JobManagerMessages.LeaderSessionMessage message = taskManager.expectMsgClass(
						deadline.timeLeft(),
						JobManagerMessages.LeaderSessionMessage.class);
					assertTrue(message.message() instanceof RequestAccumulatorSnapshots);

					Heartbeat heartbeat = expectMsgClass(
						deadline.timeLeft(),
						Heartbeat.class);

					assertEquals(instanceId, heartbeat.getInstanceId());
					assertEquals(accumulatorSnapshots, heartbeat.getAccumulatorSnapshots());

					getLastSender().tell(
						new JobManagerMessages.LeaderSessionMessage(
							leaderId,
							HeartbeatResponse.getInstance()),
						getRef());
				}
			} finally {
				TestingUtils.stopActor(forwardingActor);
				TestingUtils.stopActor(heartbeatActor);
			}
		}};
	}

	/**
	 * Test that the heartbeat actor throws a HeartbeatTimeout if the heartbeat target stops sending
	 * heartbeat responses.
	 */
	@Test
	public void testTimeout() throws IOException, InterruptedException {
		new JavaTestKit(actorsSystem) {{
			final FiniteDuration heartbeatInterval = new FiniteDuration(20L, TimeUnit.MILLISECONDS);
			final FiniteDuration initialHeartbeatPause = new FiniteDuration(500L, TimeUnit.MILLISECONDS);
			final FiniteDuration maxHeartbeatPause = new FiniteDuration(2000L, TimeUnit.MILLISECONDS);

			final UUID leaderId = UUID.randomUUID();
			final InstanceID instanceId = new InstanceID();

			final ActorRef simpleHeartbeatResponder = actorsSystem.actorOf(
				Props.create(
					SimpleHeartbeatResponder.class,
					leaderId),
				"HeartbeatResponder");

			final ActorRef forwardingActor = actorsSystem.actorOf(
				Props.create(
					TestingUtils.ForwardingActor.class,
					getRef(),
					Option.apply(leaderId)),
				"ForwardingActor");

			final JobID jobId = new JobID();
			final ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();

			final Collection<AccumulatorSnapshot> accumulatorSnapshots = Arrays.asList(
				new AccumulatorSnapshot(
					jobId,
					executionAttemptId,
					Collections.<String, Accumulator<?, ?>>emptyMap()));

			final ActorRef heartbeatActor = actorsSystem.actorOf(
				Props.create(
					HeartbeatActor.class,
					forwardingActor,
					instanceId,
					simpleHeartbeatResponder,
					leaderId,
					heartbeatInterval,
					initialHeartbeatPause,
					maxHeartbeatPause,
					log),
				"heartbeatActor");

			try {
				heartbeatActor.tell(new JobManagerMessages.LeaderSessionMessage(leaderId, new UpdateAccumulatorSnapshots(accumulatorSnapshots)), getRef());

				FiniteDuration maxTimeForTimeoutMessage = new FiniteDuration(initialHeartbeatPause.toMillis() * 2L, TimeUnit.MILLISECONDS);

				Deadline deadline = maxTimeForTimeoutMessage.fromNow();

				while (deadline.hasTimeLeft()) {
					// receive the request accumulator requests
					expectMsgClass(deadline.timeLeft(), RequestAccumulatorSnapshots.class);
				}

				TestingUtils.stopActor(simpleHeartbeatResponder);

				deadline = maxTimeForTimeoutMessage.fromNow();

				HeartbeatTimeout heartbeatTimeout = null;

				while(deadline.hasTimeLeft()) {
					Object msg = receiveOne(deadline.timeLeft());

					if (msg instanceof HeartbeatTimeout) {
						heartbeatTimeout = (HeartbeatTimeout) msg;
						break;
					}
				}

				assertNotNull(heartbeatTimeout);

				assertEquals(leaderId, heartbeatTimeout.getLeaderId());

			} finally {
				TestingUtils.stopActor(forwardingActor);
				TestingUtils.stopActor(heartbeatActor);
			}
		}};
	}

	private static class SimpleHeartbeatResponder extends FlinkUntypedActor {

		private final UUID leaderId;

		public SimpleHeartbeatResponder(UUID leaderId) {
			this.leaderId = leaderId;
		}

		@Override
		protected void handleMessage(Object message) throws Exception {
			if (message instanceof Heartbeat) {
				getSender().tell(new JobManagerMessages.LeaderSessionMessage(leaderId, HeartbeatResponse.getInstance()), getSelf());
			}
		}

		@Override
		protected UUID getLeaderSessionID() {
			return leaderId;
		}
	}
}
