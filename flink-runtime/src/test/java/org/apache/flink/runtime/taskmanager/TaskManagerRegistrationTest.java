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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.standalone.StandaloneResourceManager;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import org.apache.flink.runtime.messages.RegistrationMessages.AcknowledgeRegistration;
import org.apache.flink.runtime.messages.RegistrationMessages.RegisterTaskManager;
import org.apache.flink.runtime.messages.RegistrationMessages.RefuseRegistration;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.testingUtils.TestingUtils.stopActor;
import static org.apache.flink.runtime.testingUtils.TestingUtils.createTaskManager;
import static org.apache.flink.runtime.testingUtils.TestingUtils.createJobManager;
import static org.junit.Assert.*;

/**
 * The tests in this class verify the behavior of the TaskManager
 * when connecting to the JobManager, and when the JobManager
 * is unreachable.
 */
public class TaskManagerRegistrationTest extends TestLogger {

	private static final Option<String> NONE_STRING = Option.empty();

	// use one actor system throughout all tests
	private static ActorSystem actorSystem;

	private static Configuration config;

	private static FiniteDuration timeout = new FiniteDuration(20, TimeUnit.SECONDS);

	@BeforeClass
	public static void startActorSystem() {
		config = new Configuration();
		config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, "5 s");
		config.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL, "200 ms");
		config.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE, "2 s");
		config.setDouble(ConfigConstants.AKKA_WATCH_THRESHOLD, 2.0);

		actorSystem = AkkaUtils.createLocalActorSystem(config);
	}

	@AfterClass
	public static void shutdownActorSystem() {
		if (actorSystem != null) {
			actorSystem.shutdown();
		}
	}

	/**
	 * A test that verifies that two TaskManagers correctly register at the
	 * JobManager.
	 */
	@Test
	public void testSimpleRegistration() {
		new JavaTestKit(actorSystem) {{

			ActorGateway jobManager = null;
			ActorGateway taskManager1 = null;
			ActorGateway taskManager2 = null;

			try {
				// a simple JobManager
				jobManager = createJobManager(
					actorSystem,
					actorSystem.dispatcher(),
					actorSystem.dispatcher(),
					config);
				startResourceManager(config, jobManager.actor());

				// start two TaskManagers. it will automatically try to register
				taskManager1 = createTaskManager(
						actorSystem,
						jobManager,
						config,
						true,
						false);

				taskManager2 = createTaskManager(
						actorSystem,
						jobManager,
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

				// this is a hack to work around the way Java can interact with scala case objects
				Class<?> confirmClass = TaskManagerMessages.getRegisteredAtJobManagerMessage().getClass();
				assertTrue(response1 != null && confirmClass.isAssignableFrom(response1.getClass()));
				assertTrue(response2 != null && confirmClass.isAssignableFrom(response2.getClass()));

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
				stopActor(taskManager1);
				stopActor(taskManager2);
				stopActor(jobManager);
			}
		}};
	}

	/**
	 * A test that verifies that two TaskManagers correctly register at the
	 * JobManager.
	 */
	@Test
	public void testDelayedRegistration() {
		new JavaTestKit(actorSystem) {{
			ActorGateway jobManager = null;
			ActorGateway taskManager = null;

			FiniteDuration delayedTimeout = timeout.$times(3);

			try {
				// start a TaskManager that tries to register at the JobManager before the JobManager is
				// available. we give it the regular JobManager akka URL
				taskManager = createTaskManager(
						actorSystem,
						JobManager.getLocalJobManagerAkkaURL(Option.<String>empty()),
						new Configuration(),
						true,
						false);

				// let it try for a bit
				Thread.sleep(6000);

				// now start the JobManager, with the regular akka URL
				jobManager = createJobManager(
					actorSystem,
					actorSystem.dispatcher(),
					actorSystem.dispatcher(),
					new Configuration());

				startResourceManager(config, jobManager.actor());

				startResourceManager(config, jobManager.actor());

				// check that the TaskManagers are registered
				Future<Object> responseFuture = taskManager.ask(
						TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
						delayedTimeout);

				Object response = Await.result(responseFuture, delayedTimeout);

				// this is a hack to work around the way Java can interact with scala case objects
				Class<?> confirmClass = TaskManagerMessages.getRegisteredAtJobManagerMessage().getClass();
				assertTrue(response != null && confirmClass.isAssignableFrom(response.getClass()));

			}
			catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			} finally {
				stopActor(taskManager);
				stopActor(jobManager);
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
				tmConfig.setString(ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION, "500 ms");

				// start the taskManager actor
				taskManager = createTaskManager(
						actorSystem,
						JobManager.getLocalJobManagerAkkaURL(Option.<String>empty()),
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
				stopActor(taskManager);
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
				jm = TestingUtils.createForwardingActor(actorSystem, getTestActor(), Option.<String>empty());
				final ActorGateway jmGateway = jm;

				FiniteDuration refusedRegistrationPause = new FiniteDuration(500, TimeUnit.MILLISECONDS);
				Configuration tmConfig = new Configuration(config);
				tmConfig.setString(ConfigConstants.TASK_MANAGER_REFUSED_REGISTRATION_PAUSE, refusedRegistrationPause.toString());

				// we make the test actor (the test kit) the JobManager to intercept
				// the messages
				taskManager = createTaskManager(
					actorSystem,
					jmGateway,
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
			}
			catch (Throwable e) {
				e.printStackTrace();
				fail(e.getMessage());
			} finally {
				stopActor(taskManager);
				stopActor(jm);
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

				jm = TestingUtils.createForwardingActor(actorSystem, getTestActor(), Option.<String>empty());
				final ActorGateway jmGateway = jm;

				long refusedRegistrationPause = 500;
				long initialRegistrationPause = 100;
				long maxDelay = 30000;

				Configuration tmConfig = new Configuration(config);
				tmConfig.setString(ConfigConstants.TASK_MANAGER_REFUSED_REGISTRATION_PAUSE, refusedRegistrationPause + " ms");
				tmConfig.setString(ConfigConstants.TASK_MANAGER_INITIAL_REGISTRATION_PAUSE, initialRegistrationPause + " ms");

				// we make the test actor (the test kit) the JobManager to intercept
				// the messages
				taskManager = createTaskManager(
					actorSystem,
					jmGateway,
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
				stopActor(taskManager);
				stopActor(jm);
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
						Option.apply(JOB_MANAGER_NAME));
				final ActorGateway fakeJM1Gateway = fakeJobManager1Gateway;

				// we make the test actor (the test kit) the JobManager to intercept
				// the messages
				taskManagerGateway = createTaskManager(
						actorSystem,
						fakeJobManager1Gateway,
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
						while(message == null || !(message instanceof Terminated)) {
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
				stopActor(taskManagerGateway);
				stopActor(fakeJobManager1Gateway);
				stopActor(fakeJobManager2Gateway);
			}
		}};
	}

	@Test
	public void testCheckForValidRegistrationSessionIDs() {
		new JavaTestKit(actorSystem) {{

			ActorGateway taskManagerGateway = null;

			try {
				// we make the test actor (the test kit) the JobManager to intercept
				// the messages
				taskManagerGateway = createTaskManager(
						actorSystem,
						getTestActor(),
						config,
						true,
						false);

				final ActorRef taskManager = taskManagerGateway.actor();

				final UUID falseLeaderSessionID = UUID.randomUUID();
				final UUID trueLeaderSessionID = null;

				new Within(timeout) {

					@Override
					protected void run() {
						taskManager.tell(TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(), getTestActor());

						// the TaskManager should try to register

						LeaderSessionMessage lsm = expectMsgClass(LeaderSessionMessage.class);

						assertTrue(lsm.leaderSessionID() == trueLeaderSessionID);
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
						Object confirmMessageClass = TaskManagerMessages.getRegisteredAtJobManagerMessage().getClass();

						while(message == null || !(message.getClass().equals(confirmMessageClass))) {
							message = receiveOne(TestingUtils.TESTING_DURATION());
						}

						tm.tell(JobManagerMessages.getRequestLeaderSessionID(), getTestActor());

						expectMsgEquals(new JobManagerMessages.ResponseLeaderSessionID(trueLeaderSessionID));
					}
				};
			}
			catch (Throwable e) {
				e.printStackTrace();
				fail(e.getMessage());
			} finally {
				stopActor(taskManagerGateway);
			}
		}};
	}

	// --------------------------------------------------------------------------------------------
	//  Utility Functions
	// --------------------------------------------------------------------------------------------

	private static ActorRef startJobManager(Configuration configuration) throws Exception {
		// start the actors. don't give names, so they get generated names and we
		// avoid conflicts with the actor names
		return JobManager.startJobManagerActors(
			configuration,
			actorSystem,
			actorSystem.dispatcher(),
			actorSystem.dispatcher(),
			NONE_STRING,
			NONE_STRING,
			JobManager.class,
			MemoryArchivist.class)._1();
	}

	private static ActorRef startResourceManager(Configuration config, ActorRef jobManager) {
		return FlinkResourceManager.startResourceManagerActors(
			config,
			actorSystem,
			new StandaloneLeaderRetrievalService(jobManager.path().toString()),
			StandaloneResourceManager.class);
	}
}
