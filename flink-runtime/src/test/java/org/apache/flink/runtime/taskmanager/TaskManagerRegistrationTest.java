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

package org.apache.flink.runtime.taskmanager;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.InvalidActorNameException;
import akka.actor.Terminated;
import akka.testkit.JavaTestKit;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.standalone.StandaloneResourceManager;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import org.apache.flink.runtime.messages.RegistrationMessages.AcknowledgeRegistration;
import org.apache.flink.runtime.messages.RegistrationMessages.RegisterTaskManager;
import org.apache.flink.runtime.messages.RegistrationMessages.RefuseRegistration;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.testingUtils.TestingUtils.stopActor;
import static org.apache.flink.runtime.testingUtils.TestingUtils.createTaskManager;
import static org.apache.flink.runtime.testingUtils.TestingUtils.stopActorGatewaysGracefully;
import static org.apache.flink.runtime.testingUtils.TestingUtils.stopActorGracefully;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The tests in this class verify the behavior of the TaskManager
 * when connecting to the JobManager, and when the JobManager
 * is unreachable.
 */
public class TaskManagerRegistrationTest extends TestLogger {

	// use one actor system throughout all tests
	private static ActorSystem actorSystem;

	private static Configuration config;

	private static FiniteDuration timeout = new FiniteDuration(20, TimeUnit.SECONDS);

	private TestingHighAvailabilityServices highAvailabilityServices;

	@BeforeClass
	public static void startActorSystem() {
		config = new Configuration();
		config.setString(AkkaOptions.ASK_TIMEOUT, "5 s");
		config.setString(AkkaOptions.WATCH_HEARTBEAT_INTERVAL, "200 ms");
		config.setString(AkkaOptions.WATCH_HEARTBEAT_PAUSE, "2 s");
		config.setInteger(AkkaOptions.WATCH_THRESHOLD, 2);

		actorSystem = AkkaUtils.createLocalActorSystem(config);
	}

	@AfterClass
	public static void shutdownActorSystem() {
		if (actorSystem != null) {
			actorSystem.terminate();
		}
	}

	@Before
	public void setupTest() {
		highAvailabilityServices = new TestingHighAvailabilityServices();
	}

	@After
	public void tearDownTest() throws Exception {
		highAvailabilityServices.closeAndCleanupAllData();
		highAvailabilityServices = null;
	}

	/**
	 * A test that verifies that two TaskManagers correctly register at the
	 * JobManager.
	 */
	@Test
	public void testSimpleRegistration() throws Exception {
		new JavaTestKit(actorSystem) {{

			ActorGateway jobManager = null;
			ActorGateway taskManager1 = null;
			ActorGateway taskManager2 = null;
			ActorGateway resourceManager = null;

			EmbeddedHaServices embeddedHaServices = null;

			try {
				embeddedHaServices = new EmbeddedHaServices(Executors.directExecutor());

				// a simple JobManager
				jobManager = TestingUtils.createJobManager(
					actorSystem,
					TestingUtils.defaultExecutor(),
					TestingUtils.defaultExecutor(),
					config,
					embeddedHaServices);

				resourceManager = new AkkaActorGateway(
					startResourceManager(config, embeddedHaServices),
					jobManager.leaderSessionID());

				// start two TaskManagers. it will automatically try to register
				taskManager1 = createTaskManager(
						actorSystem,
						embeddedHaServices,
						config,
						true,
						false);

				taskManager2 = createTaskManager(
						actorSystem,
						embeddedHaServices,
						config,
						true,
						false);

				// check that the TaskManagers are registered
				Future<Object> responseFuture1 = taskManager1.ask(
						TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
						timeout);

				Future<Object> responseFuture2 = taskManager2.ask(
						TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
						timeout);

				Object response1 = Await.result(responseFuture1, timeout);
				Object response2 = Await.result(responseFuture2, timeout);

				assertTrue(response1 instanceof TaskManagerMessages.RegisteredAtJobManager);
				assertTrue(response2 instanceof TaskManagerMessages.RegisteredAtJobManager);

				// check that the JobManager has 2 TaskManagers registered
				Future<Object> numTaskManagersFuture = jobManager.ask(
						JobManagerMessages.getRequestNumberRegisteredTaskManager(),
						timeout);

				Integer count = (Integer) Await.result(numTaskManagersFuture, timeout);
				assertEquals(2, count.intValue());
			}
			catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			} finally {
				stopActorGatewaysGracefully(Arrays.asList(taskManager1, taskManager2, jobManager, resourceManager));

				embeddedHaServices.closeAndCleanupAllData();
			}
		}};
	}

	/**
	 * A test that verifies that two TaskManagers correctly register at the
	 * JobManager.
	 */
	@Test
	public void testDelayedRegistration() throws Exception {
		new JavaTestKit(actorSystem) {{
			ActorGateway jobManager = null;
			ActorGateway taskManager = null;

			FiniteDuration delayedTimeout = timeout.$times(3L);

			final EmbeddedHaServices embeddedHaServices = new EmbeddedHaServices(Executors.directExecutor());

			try {
				// start a TaskManager that tries to register at the JobManager before the JobManager is
				// available. we give it the regular JobManager akka URL
				taskManager = createTaskManager(
						actorSystem,
						embeddedHaServices,
						new Configuration(),
						true,
						false);

				// let it try for a bit
				Thread.sleep(6000L);

				// now start the JobManager, with the regular akka URL
				jobManager = TestingUtils.createJobManager(
					actorSystem,
					TestingUtils.defaultExecutor(),
					TestingUtils.defaultExecutor(),
					new Configuration(),
					embeddedHaServices);

				// check that the TaskManagers are registered
				Future<Object> responseFuture = taskManager.ask(
						TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
						delayedTimeout);

				Object response = Await.result(responseFuture, delayedTimeout);

				assertTrue(response instanceof TaskManagerMessages.RegisteredAtJobManager);
			} finally {
				stopActorGatewaysGracefully(Arrays.asList(taskManager, jobManager));

				embeddedHaServices.closeAndCleanupAllData();
			}
		}};
	}

	/**
	 * Tests that the TaskManager shuts down when it cannot register at the
	 * JobManager within the given maximum duration.
	 *
	 * Unfortunately, this test does not give good error messages.
	 * (I have not figured out how to get any better message out of the
	 * Akka TestKit than "ask timeout exception".)
	 *
	 * Anyways: An "ask timeout exception" here means that the TaskManager
	 * did not shut down after its registration timeout expired.
	 */
	@Test
	public void testShutdownAfterRegistrationDurationExpired() {
		new JavaTestKit(actorSystem) {{

			ActorGateway taskManager = null;

			try {
				// registration timeout of 1 second
				Configuration tmConfig = new Configuration();
				tmConfig.setString(TaskManagerOptions.REGISTRATION_TIMEOUT, "500 ms");

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					// Give a non-existent job manager address to the task manager
					new SettableLeaderRetrievalService(
						"foobar",
						HighAvailabilityServices.DEFAULT_LEADER_ID));

				// start the taskManager actor
				taskManager = createTaskManager(
						actorSystem,
						highAvailabilityServices,
						tmConfig,
						true,
						false);

				// make sure it terminates in time, since it cannot register at a JobManager
				watch(taskManager.actor());

				final ActorGateway tm = taskManager;

				new Within(timeout) {

					@Override
					protected void run() {
						expectTerminated(tm.actor());
					}
				};
			}
			catch (Throwable e) {
				e.printStackTrace();
				fail(e.getMessage());
			} finally {
				stopActorGracefully(taskManager);
			}
		}};
	}

	/**
	 * Make sure that the TaskManager keeps trying to register, even after
	 * registration attempts have been refused.
	 */
	@Test
	public void testTaskManagerResumesConnectAfterRefusedRegistration() {
		new JavaTestKit(actorSystem) {{
			ActorGateway jm = null;
			ActorGateway taskManager =null;
			try {
				jm = TestingUtils.createForwardingActor(
					actorSystem,
					getTestActor(),
					HighAvailabilityServices.DEFAULT_LEADER_ID,
					Option.<String>empty());
				final ActorGateway jmGateway = jm;

				FiniteDuration refusedRegistrationPause = new FiniteDuration(500, TimeUnit.MILLISECONDS);
				Configuration tmConfig = new Configuration(config);
				tmConfig.setString(TaskManagerOptions.REFUSED_REGISTRATION_BACKOFF, refusedRegistrationPause.toString());

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new SettableLeaderRetrievalService(
						jm.path(),
						HighAvailabilityServices.DEFAULT_LEADER_ID));

				// we make the test actor (the test kit) the JobManager to intercept
				// the messages
				taskManager = createTaskManager(
					actorSystem,
					highAvailabilityServices,
					tmConfig,
					true,
					false);

				final ActorGateway taskManagerGateway = taskManager;

				// check and decline initial registration
				new Within(timeout) {

					@Override
					protected void run() {
						// the TaskManager should try to register
						expectMsgClass(RegisterTaskManager.class);

						// we decline the registration
						taskManagerGateway.tell(
								new RefuseRegistration(new Exception("test reason")),
								jmGateway);
					}
				};



				// the TaskManager should wait a bit an retry...
				FiniteDuration maxDelay = (FiniteDuration) refusedRegistrationPause.$times(3.0);
				new Within(maxDelay) {

					@Override
					protected void run() {
						expectMsgClass(RegisterTaskManager.class);
					}
				};
			} finally {
				stopActorGatewaysGracefully(Arrays.asList(taskManager, jm));
			}
		}};
	}

	/**
	 * Tests that the TaskManager does not send an excessive amount of registration messages to
	 * the job manager if its registration was rejected.
	 */
	@Test
	public void testTaskManagerNoExcessiveRegistrationMessages() throws Exception {
		new JavaTestKit(actorSystem) {{
			ActorGateway jm = null;
			ActorGateway taskManager =null;
			try {
				FiniteDuration timeout = new FiniteDuration(5, TimeUnit.SECONDS);

				jm = TestingUtils.createForwardingActor(
					actorSystem,
					getTestActor(),
					HighAvailabilityServices.DEFAULT_LEADER_ID,
					Option.<String>empty());

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new SettableLeaderRetrievalService(
						jm.path(),
						HighAvailabilityServices.DEFAULT_LEADER_ID));

				final ActorGateway jmGateway = jm;

				long refusedRegistrationPause = 500;
				long initialRegistrationPause = 100;
				long maxDelay = 30000;

				Configuration tmConfig = new Configuration(config);
				tmConfig.setString(TaskManagerOptions.REFUSED_REGISTRATION_BACKOFF, refusedRegistrationPause + " ms");
				tmConfig.setString(TaskManagerOptions.INITIAL_REGISTRATION_BACKOFF, initialRegistrationPause + " ms");

				// we make the test actor (the test kit) the JobManager to intercept
				// the messages
				taskManager = createTaskManager(
					actorSystem,
					highAvailabilityServices,
					tmConfig,
					true,
					false);

				final ActorGateway taskManagerGateway = taskManager;

				final Deadline deadline = timeout.fromNow();

				try {
					while (deadline.hasTimeLeft()) {
						// the TaskManager should try to register
						expectMsgClass(deadline.timeLeft(), RegisterTaskManager.class);

						// we decline the registration
						taskManagerGateway.tell(
							new RefuseRegistration(new Exception("test reason")),
							jmGateway);
					}
				} catch (AssertionError error) {
					// ignore since it simply means that we have used up all our time
				}

				RegisterTaskManager[] registerTaskManagerMessages = new ReceiveWhile<RegisterTaskManager>(RegisterTaskManager.class, timeout) {
					@Override
					protected RegisterTaskManager match(Object msg) throws Exception {
						if (msg instanceof RegisterTaskManager) {
							return (RegisterTaskManager) msg;
						} else {
							throw noMatch();
						}
					}
				}.get();

				int maxExponent = (int) Math.floor(Math.log(((double) maxDelay / initialRegistrationPause + 1))/Math.log(2));
				int exponent = (int) Math.ceil(Math.log(((double) timeout.toMillis() / initialRegistrationPause + 1))/Math.log(2));

				int exp = Math.min(maxExponent, exponent);

				long difference = timeout.toMillis() - (initialRegistrationPause * (1 << exp));

				int numberRegisterTaskManagerMessages = exp;

				if (difference > 0) {
					numberRegisterTaskManagerMessages += Math.ceil((double) difference / maxDelay);
				}

				int maxExpectedNumberOfRegisterTaskManagerMessages = numberRegisterTaskManagerMessages * 2;

				assertTrue("The number of RegisterTaskManager messages #"
					+ registerTaskManagerMessages.length
					+ " should be less than #"
					+ maxExpectedNumberOfRegisterTaskManagerMessages,
					registerTaskManagerMessages.length <= maxExpectedNumberOfRegisterTaskManagerMessages);
			} finally {
				stopActorGatewaysGracefully(Arrays.asList(taskManager, jm));
			}
		}};
	}

	/**
	 * Validate that the TaskManager attempts to re-connect after it lost the connection
	 * to the JobManager.
	 */
	@Test
	public void testTaskManagerResumesConnectAfterJobManagerFailure() {
		new JavaTestKit(actorSystem) {{
			ActorGateway fakeJobManager1Gateway = null;
			ActorGateway fakeJobManager2Gateway = null;
			ActorGateway taskManagerGateway = null;

			final String JOB_MANAGER_NAME = "ForwardingJobManager";

			try {
				fakeJobManager1Gateway = TestingUtils.createForwardingActor(
					actorSystem,
					getTestActor(),
					HighAvailabilityServices.DEFAULT_LEADER_ID,
					Option.apply(JOB_MANAGER_NAME));
				final ActorGateway fakeJM1Gateway = fakeJobManager1Gateway;

				SettableLeaderRetrievalService settableLeaderRetrievalService = new SettableLeaderRetrievalService(
					fakeJM1Gateway.path(),
					HighAvailabilityServices.DEFAULT_LEADER_ID);

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					settableLeaderRetrievalService);

				// we make the test actor (the test kit) the JobManager to intercept
				// the messages
				taskManagerGateway = createTaskManager(
						actorSystem,
						highAvailabilityServices,
						config,
						true,
						false);

				final ActorGateway tm = taskManagerGateway;

				// validate initial registration
				new Within(timeout) {

					@Override
					protected void run() {
						// the TaskManager should try to register
						expectMsgClass(RegisterTaskManager.class);

						// we accept the registration
						tm.tell(
								new AcknowledgeRegistration(
										new InstanceID(),
										45234),
								fakeJM1Gateway);
					}
				};

				// kill the first forwarding JobManager
				watch(fakeJobManager1Gateway.actor());
				stopActor(fakeJobManager1Gateway.actor());

				final ActorGateway gateway = fakeJobManager1Gateway;

				new Within(timeout) {

					@Override
					protected void run() {
						Object message = null;

						// we might also receive RegisterTaskManager and Heartbeat messages which
						// are queued up in the testing actor's mailbox
						while(!(message instanceof Terminated)) {
							message = receiveOne(timeout);
						}

						Terminated terminatedMessage = (Terminated) message;
						assertEquals(gateway.actor(), terminatedMessage.actor());
					}
				};

				fakeJobManager1Gateway = null;

				// now start the second fake JobManager and expect that
				// the TaskManager registers again
				// the second fake JM needs to have the same actor URL

				// since we cannot reliably wait until the actor is unregistered (name is
				// available again) we loop with multiple tries for 20 seconds
				long deadline = 20000000000L + System.nanoTime();
				do {
					try {
						fakeJobManager2Gateway = TestingUtils.createForwardingActor(
							actorSystem,
							getTestActor(),
							HighAvailabilityServices.DEFAULT_LEADER_ID,
							Option.apply(JOB_MANAGER_NAME));
					} catch (InvalidActorNameException e) {
						// wait and retry
						Thread.sleep(100);
					}
				} while (fakeJobManager2Gateway == null && System.nanoTime() < deadline);

				final ActorGateway fakeJM2GatewayClosure = fakeJobManager2Gateway;

				// expect the next registration
				new Within(timeout) {

					@Override
					protected void run() {
						expectMsgClass(RegisterTaskManager.class);

						// we accept the registration
						tm.tell(
								new AcknowledgeRegistration(
										new InstanceID(),
										45234),
								fakeJM2GatewayClosure);
					}
				};
			}
			catch (Throwable e) {
				e.printStackTrace();
				fail(e.getMessage());
			} finally {
				stopActorGatewaysGracefully(Arrays.asList(taskManagerGateway, fakeJobManager2Gateway));
			}
		}};
	}

	@Test
	public void testCheckForValidRegistrationSessionIDs() throws IOException {
		new JavaTestKit(actorSystem) {{

			ActorGateway taskManagerGateway = null;

			final UUID falseLeaderSessionID = UUID.randomUUID();
			final UUID trueLeaderSessionID = UUID.randomUUID();

			HighAvailabilityServices mockedHighAvailabilityServices = mock(HighAvailabilityServices.class);
			when(mockedHighAvailabilityServices.getJobManagerLeaderRetriever(Matchers.eq(HighAvailabilityServices.DEFAULT_JOB_ID)))
				.thenReturn(new StandaloneLeaderRetrievalService(getTestActor().path().toString(), trueLeaderSessionID));
			when(mockedHighAvailabilityServices.createBlobStore()).thenReturn(new VoidBlobStore());

			try {
				// we make the test actor (the test kit) the JobManager to intercept
				// the messages
				taskManagerGateway = createTaskManager(
						actorSystem,
						mockedHighAvailabilityServices,
						config,
						true,
						false);

				final ActorRef taskManager = taskManagerGateway.actor();

				new Within(timeout) {

					@Override
					protected void run() {
						taskManager.tell(TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(), getTestActor());

						// the TaskManager should try to register

						LeaderSessionMessage lsm = expectMsgClass(LeaderSessionMessage.class);

						assertTrue(lsm.leaderSessionID().equals(trueLeaderSessionID));
						assertTrue(lsm.message() instanceof RegisterTaskManager);

						final ActorRef tm = getLastSender();

						// This AcknowledgeRegistration message should be discarded because the
						// registration session ID is wrong
						tm.tell(
								new LeaderSessionMessage(
										falseLeaderSessionID,
										new AcknowledgeRegistration(
												new InstanceID(),
												1)),
								getTestActor());

						// Valid AcknowledgeRegistration message
						tm.tell(
								new LeaderSessionMessage(
										trueLeaderSessionID,
										new AcknowledgeRegistration(
												new InstanceID(),
												1)),
								getTestActor());

						Object message = null;

						while(!(message instanceof TaskManagerMessages.RegisteredAtJobManager)) {
							message = receiveOne(TestingUtils.TESTING_DURATION());
						}

						tm.tell(JobManagerMessages.getRequestLeaderSessionID(), getTestActor());

						expectMsgEquals(new JobManagerMessages.ResponseLeaderSessionID(trueLeaderSessionID));
					}
				};
			} finally {
				stopActorGracefully(taskManagerGateway);
			}
		}};
	}

	// --------------------------------------------------------------------------------------------
	//  Utility Functions
	// --------------------------------------------------------------------------------------------

	private static ActorRef startResourceManager(Configuration config, HighAvailabilityServices highAvailabilityServices) {
		return FlinkResourceManager.startResourceManagerActors(
			config,
			actorSystem,
			highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
			StandaloneResourceManager.class);
	}
}
