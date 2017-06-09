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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.CommandLineOptions;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status;
import akka.testkit.JavaTestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

import static org.apache.flink.client.CliFrontendTestUtils.pipeSystemOutToNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the CANCEL and LIST commands.
 */
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

				final UUID leaderSessionID = UUID.randomUUID();

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

				final UUID leaderSessionID = UUID.randomUUID();

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

	/**
	 * Tests cancelling with the savepoint option.
	 */
	@Test
	public void testCancelWithSavepoint() throws Exception {
		{
			// Cancel with savepoint (no target directory)
			JobID jid = new JobID();
			UUID leaderSessionID = UUID.randomUUID();

			Props props = Props.create(CliJobManager.class, jid, leaderSessionID);
			ActorRef jm = actorSystem.actorOf(props);
			ActorGateway gateway = new AkkaActorGateway(jm, leaderSessionID);

			String[] parameters = { "-s", jid.toString() };
			InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(gateway);
			assertEquals(0, testFrontend.cancel(parameters));
		}

		{
			// Cancel with savepoint (with target directory)
			JobID jid = new JobID();
			UUID leaderSessionID = UUID.randomUUID();

			Props props = Props.create(CliJobManager.class, jid, leaderSessionID, "targetDirectory");
			ActorRef jm = actorSystem.actorOf(props);
			ActorGateway gateway = new AkkaActorGateway(jm, leaderSessionID);

			String[] parameters = { "-s", "targetDirectory", jid.toString() };
			InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(gateway);
			assertEquals(0, testFrontend.cancel(parameters));
		}

		{
			// Cancel with savepoint (with target directory), but no job ID
			JobID jid = new JobID();
			UUID leaderSessionID = UUID.randomUUID();

			Props props = Props.create(CliJobManager.class, jid, leaderSessionID, "targetDirectory");
			ActorRef jm = actorSystem.actorOf(props);
			ActorGateway gateway = new AkkaActorGateway(jm, leaderSessionID);

			String[] parameters = { "-s", "targetDirectory" };
			InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(gateway);
			assertNotEquals(0, testFrontend.cancel(parameters));
		}

		{
			// Cancel with savepoint (no target directory) and no job ID
			JobID jid = new JobID();
			UUID leaderSessionID = UUID.randomUUID();

			Props props = Props.create(CliJobManager.class, jid, leaderSessionID);
			ActorRef jm = actorSystem.actorOf(props);
			ActorGateway gateway = new AkkaActorGateway(jm, leaderSessionID);

			String[] parameters = { "-s" };
			InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(gateway);
			assertNotEquals(0, testFrontend.cancel(parameters));
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
				final UUID leaderSessionID = UUID.randomUUID();
				final ActorRef jm = actorSystem.actorOf(
						Props.create(
								CliJobManager.class,
								null,
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

	private static final class InfoListTestCliFrontend extends CliFrontend {

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

	private static final class CliJobManager extends FlinkUntypedActor {
		private final JobID jobID;
		private final UUID leaderSessionID;
		private final String targetDirectory;

		public CliJobManager(final JobID jobID, final UUID leaderSessionID){
			this(jobID, leaderSessionID, null);
		}

		public CliJobManager(final JobID jobID, final UUID leaderSessionID, String targetDirectory){
			this.jobID = jobID;
			this.leaderSessionID = leaderSessionID;
			this.targetDirectory = targetDirectory;
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
							decorateMessage(new Status.Success(new JobManagerMessages.CancellationSuccess(jobID, null))),
							getSelf());
				}
				else {
					getSender().tell(
							decorateMessage(new Status.Failure(new Exception("Wrong or no JobID"))),
							getSelf());
				}
			}
			else if (message instanceof JobManagerMessages.CancelJobWithSavepoint) {
				JobManagerMessages.CancelJobWithSavepoint cancelJob = (JobManagerMessages.CancelJobWithSavepoint) message;

				if (jobID != null && jobID.equals(cancelJob.jobID())) {
					if (targetDirectory == null && cancelJob.savepointDirectory() == null ||
							targetDirectory != null && targetDirectory.equals(cancelJob.savepointDirectory())) {
						getSender().tell(
								decorateMessage(new JobManagerMessages.CancellationSuccess(jobID, targetDirectory)),
								getSelf());
					} else {
						getSender().tell(
								decorateMessage(new JobManagerMessages.CancellationFailure(jobID, new Exception("Wrong target directory"))),
								getSelf());
					}
				}
				else {
					getSender().tell(
							decorateMessage(new JobManagerMessages.CancellationFailure(jobID, new Exception("Wrong or no JobID"))),
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
		protected UUID getLeaderSessionID() {
			return leaderSessionID;
		}
	}
}
