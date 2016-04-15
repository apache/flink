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

package org.apache.flink.client;

import akka.actor.*;
import akka.testkit.JavaTestKit;

import org.apache.flink.client.cli.CommandLineOptions;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

import static org.apache.flink.client.CliFrontendTestUtils.pipeSystemOutToNull;
import static org.apache.flink.client.CliFrontendTestUtils.clearGlobalConfiguration;
import static org.junit.Assert.*;

public class CliFrontendStopTest extends TestLogger {

	private static ActorSystem actorSystem;

	@BeforeClass
	public static void setup() {
		pipeSystemOutToNull();
		clearGlobalConfiguration();
		actorSystem = ActorSystem.create("TestingActorSystem");
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(actorSystem);
		actorSystem = null;
	}

	@Test
	public void testStop() throws Exception {
		// test unrecognized option
		{
			String[] parameters = { "-v", "-l" };
			CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			int retCode = testFrontend.stop(parameters);
			assertTrue(retCode != 0);
		}

		// test missing job id
		{
			String[] parameters = {};
			CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			int retCode = testFrontend.stop(parameters);
			assertTrue(retCode != 0);
		}

		// test stop properly
		{
			JobID jid = new JobID();
			String jidString = jid.toString();

			final UUID leaderSessionID = UUID.randomUUID();
			final ActorRef jm = actorSystem.actorOf(Props.create(CliJobManager.class, jid, leaderSessionID));

			final ActorGateway gateway = new AkkaActorGateway(jm, leaderSessionID);

			String[] parameters = { jidString };
			StopTestCliFrontend testFrontend = new StopTestCliFrontend(gateway);

			int retCode = testFrontend.stop(parameters);
			assertTrue(retCode == 0);
		}

		// test unknown job Id
		{
			JobID jid1 = new JobID();
			JobID jid2 = new JobID();

			final UUID leaderSessionID = UUID.randomUUID();
			final ActorRef jm = actorSystem.actorOf(Props.create(CliJobManager.class, jid1, leaderSessionID));

			final ActorGateway gateway = new AkkaActorGateway(jm, leaderSessionID);

			String[] parameters = { jid2.toString() };
			StopTestCliFrontend testFrontend = new StopTestCliFrontend(gateway);

			assertTrue(testFrontend.stop(parameters) != 0);
		}
	}

	protected static final class StopTestCliFrontend extends CliFrontend {

		private ActorGateway jobManagerGateway;

		public StopTestCliFrontend(ActorGateway jobManagerGateway) throws Exception {
			super(CliFrontendTestUtils.getConfigDir());
			this.jobManagerGateway = jobManagerGateway;
		}

		@Override
		public ActorGateway getJobManagerGateway(CommandLineOptions options) {
			return jobManagerGateway;
		}
	}

	protected static final class CliJobManager extends FlinkUntypedActor {
		private final JobID jobID;
		private final UUID leaderSessionID;

		public CliJobManager(final JobID jobID, final UUID leaderSessionID) {
			this.jobID = jobID;
			this.leaderSessionID = leaderSessionID;
		}

		@Override
		public void handleMessage(Object message) {
			if (message instanceof JobManagerMessages.RequestTotalNumberOfSlots$) {
				getSender().tell(decorateMessage(1), getSelf());
			} else if (message instanceof JobManagerMessages.StopJob) {
				JobManagerMessages.StopJob stopJob = (JobManagerMessages.StopJob) message;

				if (jobID != null && jobID.equals(stopJob.jobID())) {
					getSender().tell(decorateMessage(new Status.Success(new Object())), getSelf());
				} else {
					getSender()
							.tell(decorateMessage(new Status.Failure(new Exception(
									"Wrong or no JobID"))), getSelf());
				}
			} else if (message instanceof JobManagerMessages.RequestRunningJobsStatus$) {
				getSender().tell(decorateMessage(new JobManagerMessages.RunningJobsStatus()),
						getSelf());
			}
		}

		@Override
		protected UUID getLeaderSessionID() {
			return leaderSessionID;
		}
	}
}
