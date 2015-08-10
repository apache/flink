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
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;

import java.util.UUID;

import static org.apache.flink.client.CliFrontendTestUtils.pipeSystemOutToNull;
import static org.junit.Assert.*;

public class CliFrontendListCancelTest {

	private static ActorSystem actorSystem;

	@BeforeClass
	public static void setup(){
		pipeSystemOutToNull();
		actorSystem = ActorSystem.create("TestingActorSystem");
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(actorSystem);
		actorSystem = null;
	}
	
	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
		CliFrontendTestUtils.clearGlobalConfiguration();
	}
	
	@Test
	public void testCancel() {
		try {
			// test unrecognized option
			{
				String[] parameters = {"-v", "-l"};
				CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode != 0);
			}
			
			// test missing job id
			{
				String[] parameters = {};
				CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode != 0);
			}
			
			// test cancel properly
			{
				JobID jid = new JobID();
				String jidString = jid.toString();

				final Option<UUID> leaderSessionID = Option.<UUID>apply(UUID.randomUUID());

				final ActorRef jm = actorSystem.actorOf(Props.create(
								CliJobManager.class,
								jid,
								leaderSessionID
						)
				);

				final ActorGateway gateway = new AkkaActorGateway(jm, leaderSessionID);
				
				String[] parameters = { jidString };
				InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(gateway);

				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode == 0);
			}

			// test cancel properly
			{
				JobID jid1 = new JobID();
				JobID jid2 = new JobID();

				final Option<UUID> leaderSessionID = Option.<UUID>apply(UUID.randomUUID());

				final ActorRef jm = actorSystem.actorOf(
						Props.create(
								CliJobManager.class,
								jid1,
								leaderSessionID
						)
				);

				final ActorGateway gateway = new AkkaActorGateway(jm, leaderSessionID);

				String[] parameters = { jid2.toString() };
				InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(gateway);

				assertTrue(testFrontend.cancel(parameters) != 0);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}

	@Test
	public void testList() {
		try {
			// test unrecognized option
			{
				String[] parameters = {"-v", "-k"};
				CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
				int retCode = testFrontend.list(parameters);
				assertTrue(retCode != 0);
			}
			
			// test list properly
			{
				final Option<UUID> leaderSessionID = Option.<UUID>apply(UUID.randomUUID());
				final ActorRef jm = actorSystem.actorOf(
						Props.create(
								CliJobManager.class,
								(Object)null,
								leaderSessionID
						)
				);
				final ActorGateway gateway = new AkkaActorGateway(jm, leaderSessionID);
				String[] parameters = {"-r", "-s"};
				InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(gateway);
				int retCode = testFrontend.list(parameters);
				assertTrue(retCode == 0);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}


	protected static final class InfoListTestCliFrontend extends CliFrontend {

		private ActorGateway jobManagerGateway;

		public InfoListTestCliFrontend(ActorGateway jobManagerGateway) throws Exception {
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
		private final Option<UUID> leaderSessionID;

		public CliJobManager(final JobID jobID, final Option<UUID> leaderSessionID){
			this.jobID = jobID;
			this.leaderSessionID = leaderSessionID;
		}

		@Override
		public void handleMessage(Object message) {
			if (message instanceof JobManagerMessages.RequestTotalNumberOfSlots$) {
				getSender().tell(decorateMessage(1), getSelf());
			}
			else if (message instanceof JobManagerMessages.CancelJob) {
				JobManagerMessages.CancelJob cancelJob = (JobManagerMessages.CancelJob) message;

				if (jobID != null && jobID.equals(cancelJob.jobID())) {
					getSender().tell(
							decorateMessage(new Status.Success(new Object())),
							getSelf());
				}
				else {
					getSender().tell(
							decorateMessage(new Status.Failure(new Exception("Wrong or no JobID"))),
							getSelf());
				}
			}
			else if (message instanceof JobManagerMessages.RequestRunningJobsStatus$) {
				getSender().tell(
						decorateMessage(new JobManagerMessages.RunningJobsStatus()),
						getSelf());
			}
		}

		@Override
		protected Option<UUID> getLeaderSessionID() {
			return leaderSessionID;
		}
	}
}
