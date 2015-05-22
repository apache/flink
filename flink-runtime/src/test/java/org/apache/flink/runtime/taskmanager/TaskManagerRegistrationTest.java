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
import akka.actor.Kill;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import akka.testkit.JavaTestKit;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.StreamingMode;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.RegistrationMessages.AcknowledgeRegistration;
import org.apache.flink.runtime.messages.RegistrationMessages.RegisterTaskManager;
import org.apache.flink.runtime.messages.RegistrationMessages.RefuseRegistration;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;
import scala.Some;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * The tests in this class verify the behavior of the TaskManager
 * when connecting to the JobManager, and when the JobManager
 * is unreachable.
 */
public class TaskManagerRegistrationTest {

	private static final Option<String> NONE_STRING = Option.empty();

	// use one actor system throughout all tests
	private static ActorSystem actorSystem;

	@BeforeClass
	public static void startActorSystem() {
		Configuration config = new Configuration();
		config.getString(ConfigConstants.AKKA_ASK_TIMEOUT, "5 s");
		config.getString(ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL, "200 ms");
		config.getString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE, "2 s");
		config.getDouble(ConfigConstants.AKKA_WATCH_THRESHOLD, 2.0);

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
			try {
				// a simple JobManager
				ActorRef jobManager = startJobManager();

				// start two TaskManagers. it will automatically try to register
				final ActorRef taskManager1 = startTaskManager(jobManager);
				final ActorRef taskManager2 = startTaskManager(jobManager);

				// check that the TaskManagers are registered
				Future<Object> responseFuture1 = Patterns.ask(
						taskManager1,
						TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
						5000);

				Future<Object> responseFuture2 = Patterns.ask(
						taskManager2,
						TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
						5000);

				Object response1 = Await.result(responseFuture1, new FiniteDuration(5, TimeUnit.SECONDS));
				Object response2 = Await.result(responseFuture2, new FiniteDuration(5, TimeUnit.SECONDS));

				// this is a hack to work around the way Java can interact with scala case objects
				Class<?> confirmClass = TaskManagerMessages.getRegisteredAtJobManagerMessage().getClass();
				assertTrue(response1 != null && confirmClass.isAssignableFrom(response1.getClass()));
				assertTrue(response2 != null && confirmClass.isAssignableFrom(response2.getClass()));

				// check that the JobManager has 2 TaskManagers registered
				Future<Object> numTaskManagersFuture = Patterns.ask(
						jobManager,
						JobManagerMessages.getRequestNumberRegisteredTaskManager(),
						1000);

				Integer count = (Integer) Await.result(numTaskManagersFuture, new FiniteDuration(1, TimeUnit.SECONDS));
				assertEquals(2, count.intValue());

				stopActor(taskManager1);
				stopActor(taskManager2);
				stopActor(jobManager);
			}
			catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
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
			try {
				// start a TaskManager that tries to register at the JobManager before the JobManager is
				// available. we give it the regular JobManager akka URL
				final ActorRef taskManager = startTaskManager(JobManager.getLocalJobManagerAkkaURL(),
																new Configuration());
				// let it try for a bit
				Thread.sleep(6000);

				// now start the JobManager, with the regular akka URL
				final ActorRef jobManager =
						JobManager.startJobManagerActors(new Configuration(), actorSystem, StreamingMode.BATCH_ONLY)._1();

				// check that the TaskManagers are registered
				Future<Object> responseFuture = Patterns.ask(
						taskManager,
						TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
						30000);

				Object response = Await.result(responseFuture, new FiniteDuration(30, TimeUnit.SECONDS));

				// this is a hack to work around the way Java can interact with scala case objects
				Class<?> confirmClass = TaskManagerMessages.getRegisteredAtJobManagerMessage().getClass();
				assertTrue(response != null && confirmClass.isAssignableFrom(response.getClass()));

				stopActor(taskManager);
				stopActor(jobManager);
			}
			catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
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
			try {
				// registration timeout of 1 second
				Configuration tmConfig = new Configuration();
				tmConfig.setString(ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION, "500 ms");

				// start the taskManager actor
				final ActorRef taskManager = startTaskManager(JobManager.getLocalJobManagerAkkaURL(), tmConfig);

				// make sure it terminates in time, since it cannot register at a JobManager
				watch(taskManager);
				new Within(new FiniteDuration(10, TimeUnit.SECONDS)) {

					@Override
					protected void run() {
						expectTerminated(taskManager);
					}
				};

				stopActor(taskManager);
			}
			catch (Throwable e) {
				e.printStackTrace();
				fail(e.getMessage());
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
			try {
				// we make the test actor (the test kit) the JobManager to intercept
				// the messages
				final ActorRef taskManager = startTaskManager(getTestActor());

				// check and decline initial registration
				new Within(new FiniteDuration(2, TimeUnit.SECONDS)) {

					@Override
					protected void run() {
						// the TaskManager should try to register
						expectMsgClass(RegisterTaskManager.class);

						// we decline the registration
						getLastSender().tell(new RefuseRegistration("test reason"), getTestActor());
					}
				};

				// the TaskManager should wait a bit an retry...
				FiniteDuration maxDelay = (FiniteDuration) TaskManager.DELAY_AFTER_REFUSED_REGISTRATION().$times(2.0);
				new Within(maxDelay) {

					@Override
					protected void run() {
						expectMsgClass(RegisterTaskManager.class);
					}
				};

				stopActor(taskManager);
			}
			catch (Throwable e) {
				e.printStackTrace();
				fail(e.getMessage());
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
			try {
				final Props fakeJmProps = Props.create(ForwardingActor.class, getTestActor());
				final String jobManagerName = "FAKE_JOB_MANAGER";

				final ActorRef fakeJobManager1 = actorSystem.actorOf(fakeJmProps, jobManagerName);


				// we make the test actor (the test kit) the JobManager to intercept
				// the messages
				final ActorRef taskManager = startTaskManager(fakeJobManager1);

				// validate initial registration
				new Within(new FiniteDuration(2, TimeUnit.SECONDS)) {

					@Override
					protected void run() {
						// the TaskManager should try to register
						expectMsgClass(RegisterTaskManager.class);

						// we accept the registration
						taskManager.tell(new AcknowledgeRegistration(fakeJobManager1, new InstanceID(), 45234),
										fakeJobManager1);
					}
				};

				// kill the first forwarding JobManager
				watch(fakeJobManager1);
				stopActor(fakeJobManager1);

				// wait for the killing to be completed
				new Within(new FiniteDuration(2, TimeUnit.SECONDS)) {

					@Override
					protected void run() {
						expectTerminated(fakeJobManager1);
					}
				};

				// now start the second fake JobManager and expect that
				// the TaskManager registers again
				// the second fake JM needs to have the same actor URL
				ActorRef fakeJobManager2 = null;

				// since we cannot reliably wait until the actor is unregistered (name is
				// available again) we loop with multiple tries for 20 seconds
				long deadline = 20000000000L + System.nanoTime();
				do {
					try {
						fakeJobManager2 = actorSystem.actorOf(fakeJmProps, jobManagerName);
					} catch (InvalidActorNameException e) {
						// wait and retry
						Thread.sleep(100);
					}
				} while (fakeJobManager2 == null && System.nanoTime() < deadline);

				// expect the next registration
				final ActorRef jm2Closure = fakeJobManager2;
				new Within(new FiniteDuration(10, TimeUnit.SECONDS)) {

					@Override
					protected void run() {
						expectMsgClass(RegisterTaskManager.class);

						// we accept the registration
						taskManager.tell(new AcknowledgeRegistration(jm2Closure, new InstanceID(), 45234),
								jm2Closure);
					}
				};

				stopActor(taskManager);
				stopActor(fakeJobManager2);
			}
			catch (Throwable e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}};
	}


	@Test
	public void testStartupWhenNetworkStackFailsToInitialize() {

		ServerSocket blocker = null;
		try {
			blocker = new ServerSocket(0, 50, InetAddress.getByName("localhost"));

			final Configuration cfg = new Configuration();
			cfg.setString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, "localhost");
			cfg.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, blocker.getLocalPort());
			cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 1);

			new JavaTestKit(actorSystem) {{
				try {
					// a simple JobManager
					final ActorRef jobManager = startJobManager();

					// start a task manager with a configuration that provides a blocked port
					final ActorRef taskManager = TaskManager.startTaskManagerComponentsAndActor(
							cfg, actorSystem, "localhost",
							NONE_STRING, // no actor name -> random
							new Some<String>(jobManager.path().toString()), // job manager path
							false, // init network stack !!!
							StreamingMode.BATCH_ONLY,
							TaskManager.class);

					watch(taskManager);

					expectTerminated(new FiniteDuration(20, TimeUnit.SECONDS), taskManager);

					stopActor(taskManager);
					stopActor(jobManager);
				}
				catch (Exception e) {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}};
		}
		catch (Exception e) {
			// does not work, skip test
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (blocker != null) {
				try {
					blocker.close();
				}
				catch (IOException e) {
					// ignore, best effort
				}
			}
		}
	}

	@Test
	public void testStartupWhenBlobDirectoriesAreNotWritable() {

	}

	// --------------------------------------------------------------------------------------------
	//  Utility Functions
	// --------------------------------------------------------------------------------------------

	private static ActorRef startJobManager() throws Exception {
		// start the actors. don't give names, so they get generated names and we
		// avoid conflicts with the actor names
		return JobManager.startJobManagerActors(new Configuration(), actorSystem, 
												NONE_STRING, NONE_STRING, StreamingMode.BATCH_ONLY)._1();
	}

	private static ActorRef startTaskManager(ActorRef jobManager) throws Exception {
		return startTaskManager(jobManager.path().toString(), new Configuration());
	}

	private static ActorRef startTaskManager(String jobManagerUrl, Configuration config) throws Exception {
		config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 1);

		return TaskManager.startTaskManagerComponentsAndActor(
				config, actorSystem, "localhost",
				NONE_STRING, // no actor name -> random
				new Some<String>(jobManagerUrl), // job manager path
				true, // local network stack only
				StreamingMode.BATCH_ONLY,
				TaskManager.class);
	}

	private static void stopActor(ActorRef actor) {
		actor.tell(Kill.getInstance(), ActorRef.noSender());
	}

	// --------------------------------------------------------------------------------------------
	//  Utility Actor that only forwards messages
	// --------------------------------------------------------------------------------------------

	public static class ForwardingActor extends UntypedActor {

		private final ActorRef target;

		public ForwardingActor(ActorRef target) {
			this.target = target;
		}

		@Override
		public void onReceive(Object message) throws Exception {
			target.forward(message, context());
		}
	}
}
