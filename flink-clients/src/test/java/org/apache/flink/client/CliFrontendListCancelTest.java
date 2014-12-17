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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status;
import akka.actor.UntypedActor;
import akka.testkit.JavaTestKit;
import org.apache.commons.cli.CommandLine;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CliFrontendListCancelTest {

	private static ActorSystem actorSystem;

	@BeforeClass
	public static void setup(){
		actorSystem = ActorSystem.create("TestingActorSystem");
	}

	@AfterClass
	public static void teardown(){
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
				CliFrontend testFrontend = new CliFrontend();
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode == 2);
			}
			
			// test missing job id
			{
				String[] parameters = {};
				CliFrontend testFrontend = new CliFrontend();
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode != 0);
			}
			
			// test cancel properly
			{
				JobID jid = new JobID();
				String jidString = jid.toString();

				final ActorRef jm = actorSystem.actorOf(Props.create(CliJobManager.class, jid));
				
				String[] parameters = {"-i", jidString};
				InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(jm);
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode == 0);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	
	@Test
	public void testList() {
		try {
			final ActorRef jm = actorSystem.actorOf(Props.create(CliJobManager.class, (Object)null));

			// test unrecognized option
			{
				String[] parameters = {"-v", "-k"};
				CliFrontend testFrontend = new CliFrontend();
				int retCode = testFrontend.list(parameters);
				assertTrue(retCode == 2);
			}
			
			// test missing flags
			{
				String[] parameters = {};
				CliFrontend testFrontend = new CliFrontend();
				int retCode = testFrontend.list(parameters);
				assertTrue(retCode != 0);
			}
			
			// test list properly
			{
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


	protected static final class InfoListTestCliFrontend extends CliFrontendTestUtils.TestingCliFrontend {
		private ActorRef jobmanager;

		public InfoListTestCliFrontend(ActorRef jobmanager){
			this.jobmanager = jobmanager;
		}

		@Override
		public ActorRef getJobManager(CommandLine line){
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
			if(message instanceof JobManagerMessages.RequestTotalNumberOfSlots$){
				getSender().tell(1, getSelf());
			}else if(message instanceof JobManagerMessages.CancelJob){
				JobManagerMessages.CancelJob cancelJob = (JobManagerMessages.CancelJob) message;
				assertEquals(jobID, cancelJob.jobID());
				getSender().tell(new Status.Success(new Object()), getSelf());
			}else if(message instanceof  JobManagerMessages.RequestRunningJobs$){
				getSender().tell(new JobManagerMessages.RunningJobs(),
						getSelf());
			}
		}
	}
}
