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

package org.apache.flink.runtime.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.testkit.JavaTestKit;
import akka.util.Timeout;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobClientMessages;
import org.apache.flink.runtime.messages.JobClientMessages.AttachToJobAndWait;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.messages.JobManagerMessages.*;

public class JobClientActorTest extends TestLogger {

	private static ActorSystem system;
	private static JobGraph testJobGraph = new JobGraph("Test Job");
	private static Configuration clientConfig;

	@BeforeClass
	public static void setup() {
		clientConfig = new Configuration();
		system = AkkaUtils.createLocalActorSystem(clientConfig);
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
		system = null;
	}

	/** Tests that a {@link JobClientActorSubmissionTimeoutException} is thrown when the job cannot
	 * be submitted by the JobSubmissionClientActor. This is here the case, because the started JobManager
	 * never replies to a {@link SubmitJob} message.
	 *
	 * @throws Exception
	 */
	@Test(expected=JobClientActorSubmissionTimeoutException.class)
	public void testSubmissionTimeout() throws Exception {
		FiniteDuration jobClientActorTimeout = new FiniteDuration(1L, TimeUnit.SECONDS);
		FiniteDuration timeout = jobClientActorTimeout.$times(2);

		UUID leaderSessionID = UUID.randomUUID();

		ActorRef jobManager = system.actorOf(
			Props.create(
				PlainActor.class,
				leaderSessionID));

		SettableLeaderRetrievalService settableLeaderRetrievalService = new SettableLeaderRetrievalService(
			jobManager.path().toString(),
			leaderSessionID
		);

		Props jobClientActorProps = JobSubmissionClientActor.createActorProps(
			settableLeaderRetrievalService,
			jobClientActorTimeout,
			false,
			clientConfig);

		ActorRef jobClientActor = system.actorOf(jobClientActorProps);


		Future<Object> jobExecutionResult = Patterns.ask(
			jobClientActor,
			new JobClientMessages.SubmitJobAndWait(testJobGraph),
			new Timeout(timeout));

		Await.result(jobExecutionResult, timeout);
	}


	/** Tests that a {@link JobClientActorRegistrationTimeoutException} is thrown when the registration
	 * cannot be performed at the JobManager by the JobAttachmentClientActor. This is here the case, because the
	 * started JobManager never replies to a {@link RegisterJobClient} message.
	 */
	@Test(expected=JobClientActorRegistrationTimeoutException.class)
	public void testRegistrationTimeout() throws Exception {
		FiniteDuration jobClientActorTimeout = new FiniteDuration(1L, TimeUnit.SECONDS);
		FiniteDuration timeout = jobClientActorTimeout.$times(2);

		UUID leaderSessionID = UUID.randomUUID();

		ActorRef jobManager = system.actorOf(
			Props.create(
				PlainActor.class,
				leaderSessionID));

		SettableLeaderRetrievalService settableLeaderRetrievalService = new SettableLeaderRetrievalService(
			jobManager.path().toString(),
			leaderSessionID
		);

		Props jobClientActorProps = JobAttachmentClientActor.createActorProps(
			settableLeaderRetrievalService,
			jobClientActorTimeout,
			false);

		ActorRef jobClientActor = system.actorOf(jobClientActorProps);

		Future<Object> jobExecutionResult = Patterns.ask(
			jobClientActor,
			new JobClientMessages.AttachToJobAndWait(testJobGraph.getJobID()),
			new Timeout(timeout));

		Await.result(jobExecutionResult, timeout);
	}

	/** Tests that a {@link JobClientActorConnectionTimeoutException}
	 * is thrown when the JobSubmissionClientActor wants to submit a job but has not connected to a JobManager.
	 *
	 * @throws Exception
	 */
	@Test(expected=JobClientActorConnectionTimeoutException.class)
	public void testConnectionTimeoutWithoutJobManagerForSubmission() throws Exception {
		FiniteDuration jobClientActorTimeout = new FiniteDuration(1L, TimeUnit.SECONDS);
		FiniteDuration timeout = jobClientActorTimeout.$times(2);

		SettableLeaderRetrievalService settableLeaderRetrievalService = new SettableLeaderRetrievalService(
			"localhost",
			HighAvailabilityServices.DEFAULT_LEADER_ID);

		Props jobClientActorProps = JobSubmissionClientActor.createActorProps(
			settableLeaderRetrievalService,
			jobClientActorTimeout,
			false,
			clientConfig);

		ActorRef jobClientActor = system.actorOf(jobClientActorProps);

		Future<Object> jobExecutionResult = Patterns.ask(
			jobClientActor,
			new JobClientMessages.SubmitJobAndWait(testJobGraph),
			new Timeout(timeout));

		Await.result(jobExecutionResult, timeout);
	}

	/** Tests that a {@link JobClientActorConnectionTimeoutException}
	 * is thrown when the JobAttachmentClientActor attach to a job at the JobManager
	 * but has not connected to a JobManager.
	 */
	@Test(expected=JobClientActorConnectionTimeoutException.class)
	public void testConnectionTimeoutWithoutJobManagerForRegistration() throws Exception {
		FiniteDuration jobClientActorTimeout = new FiniteDuration(1L, TimeUnit.SECONDS);
		FiniteDuration timeout = jobClientActorTimeout.$times(2);

		SettableLeaderRetrievalService settableLeaderRetrievalService = new SettableLeaderRetrievalService(
			"localhost",
			HighAvailabilityServices.DEFAULT_LEADER_ID);

		Props jobClientActorProps = JobAttachmentClientActor.createActorProps(
			settableLeaderRetrievalService,
			jobClientActorTimeout,
			false);

		ActorRef jobClientActor = system.actorOf(jobClientActorProps);

		Future<Object> jobExecutionResult = Patterns.ask(
			jobClientActor,
			new JobClientMessages.AttachToJobAndWait(testJobGraph.getJobID()),
			new Timeout(timeout));

		Await.result(jobExecutionResult, timeout);
	}

	/** Tests that a {@link org.apache.flink.runtime.client.JobClientActorConnectionTimeoutException}
	 * is thrown after a successful job submission if the JobManager dies.
	 *
	 * @throws Exception
	 */
	@Test(expected=JobClientActorConnectionTimeoutException.class)
	public void testConnectionTimeoutAfterJobSubmission() throws Exception {
		FiniteDuration jobClientActorTimeout = new FiniteDuration(1L, TimeUnit.SECONDS);
		FiniteDuration timeout = jobClientActorTimeout.$times(2);

		UUID leaderSessionID = UUID.randomUUID();

		ActorRef jobManager = system.actorOf(
			Props.create(
				JobAcceptingActor.class,
				leaderSessionID));

		SettableLeaderRetrievalService settableLeaderRetrievalService = new SettableLeaderRetrievalService(
			jobManager.path().toString(),
			leaderSessionID
		);

		Props jobClientActorProps = JobSubmissionClientActor.createActorProps(
			settableLeaderRetrievalService,
			jobClientActorTimeout,
			false,
			clientConfig);

		ActorRef jobClientActor = system.actorOf(jobClientActorProps);

		Future<Object> jobExecutionResult = Patterns.ask(
			jobClientActor,
			new JobClientMessages.SubmitJobAndWait(testJobGraph),
			new Timeout(timeout));

		Future<Object> waitFuture = Patterns.ask(jobManager, new RegisterTest(), new Timeout(timeout));

		Await.result(waitFuture, timeout);

		jobManager.tell(PoisonPill.getInstance(), ActorRef.noSender());

		Await.result(jobExecutionResult, timeout);
	}

	/** Tests that a {@link JobClientActorConnectionTimeoutException}
	 * is thrown after a successful registration of the client at the JobManager.
	 */
	@Test(expected=JobClientActorConnectionTimeoutException.class)
	public void testConnectionTimeoutAfterJobRegistration() throws Exception {
		FiniteDuration jobClientActorTimeout = new FiniteDuration(1L, TimeUnit.SECONDS);
		FiniteDuration timeout = jobClientActorTimeout.$times(2L);

		UUID leaderSessionID = UUID.randomUUID();

		ActorRef jobManager = system.actorOf(
			Props.create(
				JobAcceptingActor.class,
				leaderSessionID));

		SettableLeaderRetrievalService settableLeaderRetrievalService = new SettableLeaderRetrievalService(
			jobManager.path().toString(),
			leaderSessionID
		);

		Props jobClientActorProps = JobAttachmentClientActor.createActorProps(
			settableLeaderRetrievalService,
			jobClientActorTimeout,
			false);

		ActorRef jobClientActor = system.actorOf(jobClientActorProps);

		Future<Object> jobExecutionResult = Patterns.ask(
			jobClientActor,
			new AttachToJobAndWait(testJobGraph.getJobID()),
			new Timeout(timeout));

		Future<Object> waitFuture = Patterns.ask(jobManager, new RegisterTest(), new Timeout(timeout));

		Await.result(waitFuture, timeout);

		jobManager.tell(PoisonPill.getInstance(), ActorRef.noSender());

		Await.result(jobExecutionResult, timeout);
	}


	/** Tests that JobClient throws an Exception if the JobClientActor dies and can't answer to
	 * {@link akka.actor.Identify} message anymore.
	 */
	@Test
	public void testGuaranteedAnswerIfJobClientDies() throws Exception {
		FiniteDuration timeout = new FiniteDuration(2L, TimeUnit.SECONDS);

			UUID leaderSessionID = UUID.randomUUID();

		ActorRef jobManager = system.actorOf(
			Props.create(
				JobAcceptingActor.class,
				leaderSessionID));

		SettableLeaderRetrievalService settableLeaderRetrievalService = new SettableLeaderRetrievalService(
			jobManager.path().toString(),
			leaderSessionID
		);

		TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		highAvailabilityServices.setJobMasterLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID, settableLeaderRetrievalService);

		JobListeningContext jobListeningContext =
			JobClient.submitJob(
				system,
				clientConfig,
				highAvailabilityServices,
				testJobGraph,
				timeout,
				false,
				getClass().getClassLoader());

		Future<Object> waitFuture = Patterns.ask(jobManager, new RegisterTest(), new Timeout(timeout));
		Await.result(waitFuture, timeout);

		// kill the job client actor which has been registered at the JobManager
		jobListeningContext.getJobClientActor().tell(PoisonPill.getInstance(), ActorRef.noSender());

		try {
			// should not block but return an error
			JobClient.awaitJobResult(jobListeningContext);
			Assert.fail();
		} catch (JobExecutionException e) {
			// this is what we want
		} finally {
			highAvailabilityServices.closeAndCleanupAllData();
		}
	}

	public static class PlainActor extends FlinkUntypedActor {

		private final UUID leaderSessionID;

		public PlainActor(UUID leaderSessionID) {
			this.leaderSessionID = leaderSessionID;
		}

		@Override
		protected void handleMessage(Object message) throws Exception {
			if (message instanceof RequestBlobManagerPort$) {
				getSender().tell(1337, getSelf());
			}
		}

		@Override
		protected UUID getLeaderSessionID() {
			return leaderSessionID;
		}
	}

	public static class JobAcceptingActor extends FlinkUntypedActor {
		private final UUID leaderSessionID;
		private boolean jobAccepted = false;
		private ActorRef testFuture = ActorRef.noSender();

		public JobAcceptingActor(UUID leaderSessionID) {
			this.leaderSessionID = leaderSessionID;
		}

		@Override
		protected void handleMessage(Object message) throws Exception {
			if (message instanceof SubmitJob) {
				getSender().tell(
					new JobSubmitSuccess(((SubmitJob) message).jobGraph().getJobID()),
					getSelf());

				jobAccepted = true;

				if (testFuture != ActorRef.noSender()) {
					testFuture.tell(Acknowledge.get(), getSelf());
				}
			}
			else if (message instanceof RegisterJobClient) {
				getSender().tell(
					new RegisterJobClientSuccess(((RegisterJobClient) message).jobID()),
					getSelf());

				jobAccepted = true;

				if (testFuture != ActorRef.noSender()) {
					testFuture.tell(Acknowledge.get(), getSelf());
				}
			}
			else if (message instanceof RequestBlobManagerPort$) {
				getSender().tell(1337, getSelf());
			} else if (message instanceof RegisterTest) {
				testFuture = getSender();

				if (jobAccepted) {
					testFuture.tell(Acknowledge.get(), getSelf());
				}
			}
		}

		@Override
		protected UUID getLeaderSessionID() {
			return leaderSessionID;
		}
	}

	public static class RegisterTest{}

}
