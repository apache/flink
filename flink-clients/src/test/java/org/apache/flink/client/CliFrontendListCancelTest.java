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
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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

				final ActorRef jm = actorSystem.actorOf(Props.create(CliJobManager.class, jid));
				
				String[] parameters = { jidString };
				InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(jm);

				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode == 0);
			}

			// test cancel properly
			{
				JobID jid1 = new JobID();
				JobID jid2 = new JobID();

				final ActorRef jm = actorSystem.actorOf(Props.create(CliJobManager.class, jid1));

				String[] parameters = { jid2.toString() };
				InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(jm);

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
				final ActorRef jm = actorSystem.actorOf(Props.create(CliJobManager.class, (Object)null));
				String[] parameters = {"-r", "-s"};
				InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(jm);
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

		private ActorRef jobmanager;



		public InfoListTestCliFrontend(ActorRef jobmanager) throws Exception {
			super(CliFrontendTestUtils.getConfigDir());
			this.jobmanager = jobmanager;
		}

		@Override
		public ActorRef getJobManager(CommandLineOptions options) {
			return jobmanager;
		}
	}

	protected static final class CliJobManager extends UntypedActor{
		private final JobID jobID;

		public CliJobManager(final JobID jobID){
			this.jobID = jobID;
		}

		@Override
		public void onReceive(Object message) throws Exception {
			if (message instanceof JobManagerMessages.RequestTotalNumberOfSlots$) {
				getSender().tell(1, getSelf());
			}
			else if (message instanceof JobManagerMessages.CancelJob) {
				JobManagerMessages.CancelJob cancelJob = (JobManagerMessages.CancelJob) message;

				if (jobID != null && jobID.equals(cancelJob.jobID())) {
					getSender().tell(new Status.Success(new Object()), getSelf());
				}
				else {
					getSender().tell(new Status.Failure(new Exception("Wrong or no JobID")), getSelf());
				}
			}
			else if (message instanceof JobManagerMessages.RequestRunningJobsStatus$) {
				getSender().tell(new JobManagerMessages.RunningJobsStatus(), getSelf());
			}
		}
	}
}
