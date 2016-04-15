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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.messages.JobClientMessages;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.Messages;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class JobClientActorTest extends TestLogger {

	private static ActorSystem system;
	private static JobGraph testJobGraph = new JobGraph("Test Job", new ExecutionConfig());

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
		system = null;
	}

	/** Tests that a {@link JobClientActorSubmissionTimeoutException} is thrown when the job cannot
	 * be submitted by the JobClientActor. This is here the case, because the started JobManager
	 * never replies to a SubmitJob message.
	 *
	 * @throws Exception
	 */
	@Test(expected=JobClientActorSubmissionTimeoutException.class)
	public void testSubmissionTimeout() throws Exception {
		FiniteDuration jobClientActorTimeout = new FiniteDuration(5, TimeUnit.SECONDS);
		FiniteDuration timeout = jobClientActorTimeout.$times(2);

		UUID leaderSessionID = UUID.randomUUID();

		ActorRef jobManager = system.actorOf(
			Props.create(
				PlainActor.class,
				leaderSessionID));

		TestingLeaderRetrievalService testingLeaderRetrievalService = new TestingLeaderRetrievalService(
			jobManager.path().toString(),
			leaderSessionID
		);

		Props jobClientActorProps = JobClientActor.createJobClientActorProps(
			testingLeaderRetrievalService,
			jobClientActorTimeout,
			false);

		ActorRef jobClientActor = system.actorOf(jobClientActorProps);


		Future<Object> jobExecutionResult = Patterns.ask(
			jobClientActor,
			new JobClientMessages.SubmitJobAndWait(testJobGraph),
			new Timeout(timeout));

		Await.result(jobExecutionResult, timeout);
	}

	/** Tests that a {@link org.apache.flink.runtime.client.JobClientActorConnectionTimeoutException}
	 * is thrown when the JobClientActor wants to submit a job but has not connected to a JobManager.
	 *
	 * @throws Exception
	 */
	@Test(expected=JobClientActorConnectionTimeoutException.class)
	public void testConnectionTimeoutWithoutJobManager() throws Exception {
		FiniteDuration jobClientActorTimeout = new FiniteDuration(5, TimeUnit.SECONDS);
		FiniteDuration timeout = jobClientActorTimeout.$times(2);

		TestingLeaderRetrievalService testingLeaderRetrievalService = new TestingLeaderRetrievalService();

		Props jobClientActorProps = JobClientActor.createJobClientActorProps(
			testingLeaderRetrievalService,
			jobClientActorTimeout,
			false);

		ActorRef jobClientActor = system.actorOf(jobClientActorProps);

		Future<Object> jobExecutionResult = Patterns.ask(
			jobClientActor,
			new JobClientMessages.SubmitJobAndWait(testJobGraph),
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
		FiniteDuration jobClientActorTimeout = new FiniteDuration(5, TimeUnit.SECONDS);
		FiniteDuration timeout = jobClientActorTimeout.$times(2);

		UUID leaderSessionID = UUID.randomUUID();

		ActorRef jobManager = system.actorOf(
			Props.create(
				JobAcceptingActor.class,
				leaderSessionID));

		TestingLeaderRetrievalService testingLeaderRetrievalService = new TestingLeaderRetrievalService(
			jobManager.path().toString(),
			leaderSessionID
		);

		Props jobClientActorProps = JobClientActor.createJobClientActorProps(
			testingLeaderRetrievalService,
			jobClientActorTimeout,
			false);

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

	public static class PlainActor extends FlinkUntypedActor {

		private final UUID leaderSessionID;

		public PlainActor(UUID leaderSessionID) {
			this.leaderSessionID = leaderSessionID;
		}

		@Override
		protected void handleMessage(Object message) throws Exception {

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
			if (message instanceof JobManagerMessages.SubmitJob) {
				getSender().tell(
					new JobManagerMessages.JobSubmitSuccess(((JobManagerMessages.SubmitJob) message).jobGraph().getJobID()),
					getSelf());

				jobAccepted = true;

				if(testFuture != ActorRef.noSender()) {
					testFuture.tell(Messages.getAcknowledge(), getSelf());
				}
			} else if (message instanceof RegisterTest) {
				testFuture = getSender();

				if (jobAccepted) {
					testFuture.tell(Messages.getAcknowledge(), getSelf());
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
