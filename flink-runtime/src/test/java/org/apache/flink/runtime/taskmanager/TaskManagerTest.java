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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.RegistrationMessages;
import org.apache.flink.runtime.messages.StackTraceSampleMessages.TriggerStackTraceSample;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.messages.TaskManagerMessages.FatalError;
import org.apache.flink.runtime.messages.TaskMessages;
import org.apache.flink.runtime.messages.TaskMessages.CancelTask;
import org.apache.flink.runtime.messages.TaskMessages.StopTask;
import org.apache.flink.runtime.messages.TaskMessages.SubmitTask;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesConfiguration;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.StoppableInvokable;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Kill;
import akka.actor.Props;
import akka.actor.Status;
import akka.japi.Creator;
import akka.testkit.JavaTestKit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;
import scala.util.Failure;

import static org.apache.flink.runtime.messages.JobManagerMessages.RequestPartitionProducerState;
import static org.apache.flink.runtime.messages.JobManagerMessages.ScheduleOrUpdateConsumers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("serial")
public class TaskManagerTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerTest.class);

	private static final FiniteDuration timeout = new FiniteDuration(1, TimeUnit.MINUTES);

	private static final FiniteDuration d = new FiniteDuration(60, TimeUnit.SECONDS);
	private static final Time timeD = Time.seconds(60L);

	private static ActorSystem system;

	final static UUID leaderSessionID = UUID.randomUUID();

	private TestingHighAvailabilityServices highAvailabilityServices;

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	@Before
	public void setupTest() {
		highAvailabilityServices = new TestingHighAvailabilityServices();
	}

	@After
	public void tearDownTest() throws Exception {
		if (highAvailabilityServices != null) {
			highAvailabilityServices.closeAndCleanupAllData();

			highAvailabilityServices = null;
		}
	}

	@Test
	public void testSubmitAndExecuteTask() throws IOException {
		new JavaTestKit(system){{

			ActorGateway taskManager = null;
			final ActorGateway jobManager = TestingUtils.createForwardingActor(
				system,
				getTestActor(),
				HighAvailabilityServices.DEFAULT_LEADER_ID,
				Option.<String>empty());

			highAvailabilityServices.setJobMasterLeaderRetriever(
				HighAvailabilityServices.DEFAULT_JOB_ID,
				new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

			try {
				taskManager = TestingUtils.createTaskManager(
						system,
						highAvailabilityServices,
						new Configuration(),
						true,
						false);

				final ActorGateway tm = taskManager;

				// handle the registration
				new Within(d) {
					@Override
					protected void run() {
						expectMsgClass(RegistrationMessages.RegisterTaskManager.class);

						final InstanceID iid = new InstanceID();
						assertEquals(tm.actor(), getLastSender());
						tm.tell(
								new RegistrationMessages.AcknowledgeRegistration(
										iid,
										12345),
								jobManager);
					}
				};

				final JobID jid = new JobID();
				final JobVertexID vid = new JobVertexID();
				final ExecutionAttemptID eid = new ExecutionAttemptID();
				final SerializedValue<ExecutionConfig> executionConfig = new SerializedValue<>(new ExecutionConfig());

				final TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(
					jid, "TestJob", vid, eid, executionConfig,
					"TestTask", 7, 2, 7, 0, new Configuration(), new Configuration(),
					TestInvokableCorrect.class.getName(),
					Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
					Collections.<InputGateDeploymentDescriptor>emptyList(),
					new ArrayList<PermanentBlobKey>(), Collections.emptyList(), 0);


				new Within(d) {

					@Override
					protected void run() {
						tm.tell(new SubmitTask(tdd), jobManager);

						// TaskManager should acknowledge the submission
						// heartbeats may be interleaved
						long deadline = System.currentTimeMillis() + 10000;
						do {
							Object message = receiveOne(d);
							if (message.equals(Acknowledge.get())) {
								break;
							}
						} while (System.currentTimeMillis() < deadline);

						// task should have switched to running
						Object toRunning = new TaskMessages.UpdateTaskExecutionState(
										new TaskExecutionState(jid, eid, ExecutionState.RUNNING));

						// task should have switched to finished
						Object toFinished = new TaskMessages.UpdateTaskExecutionState(
										new TaskExecutionState(jid, eid, ExecutionState.FINISHED));

						deadline = System.currentTimeMillis() + 10000;
						do {
							Object message = receiveOne(d);
							if (message.equals(toRunning)) {
								break;
							}
							else if (!(message instanceof TaskManagerMessages.Heartbeat)) {
								fail("Unexpected message: " + message);
							}
						} while (System.currentTimeMillis() < deadline);

						deadline = System.currentTimeMillis() + 10000;
						do {
							Object message = receiveOne(d);
							if (message.equals(toFinished)) {
								break;
							}
							else if (!(message instanceof TaskManagerMessages.Heartbeat)) {
								fail("Unexpected message: " + message);
							}
						} while (System.currentTimeMillis() < deadline);


					}
				};
			}
			finally {
				// shut down the actors
				TestingUtils.stopActor(taskManager);
				TestingUtils.stopActor(jobManager);
			}
		}};
	}

	@Test
	public void testJobSubmissionAndCanceling() {
		new JavaTestKit(system){{

			ActorGateway jobManager = null;
			ActorGateway taskManager = null;

			final ActorGateway testActorGateway = new AkkaActorGateway(
					getTestActor(),
					leaderSessionID);

			try {
				ActorRef jm = system.actorOf(Props.create(SimpleJobManager.class, leaderSessionID));
				jobManager = new AkkaActorGateway(jm, leaderSessionID);

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

				taskManager = TestingUtils.createTaskManager(
						system,
						highAvailabilityServices,
						new Configuration(),
						true,
						true);

				final JobID jid1 = new JobID();
				final JobID jid2 = new JobID();

				JobVertexID vid1 = new JobVertexID();
				JobVertexID vid2 = new JobVertexID();

				final ExecutionAttemptID eid1 = new ExecutionAttemptID();
				final ExecutionAttemptID eid2 = new ExecutionAttemptID();

				final TaskDeploymentDescriptor tdd1 = createTaskDeploymentDescriptor(
						jid1, "TestJob1", vid1, eid1,
						new SerializedValue<>(new ExecutionConfig()),
						"TestTask1", 5, 1, 5, 0,
						new Configuration(), new Configuration(), TestInvokableBlockingCancelable.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<>(), Collections.<URL>emptyList(), 0);

				final TaskDeploymentDescriptor tdd2 = createTaskDeploymentDescriptor(
						jid2, "TestJob2", vid2, eid2,
						new SerializedValue<>(new ExecutionConfig()),
						"TestTask2", 7, 2, 7, 0,
						new Configuration(), new Configuration(), TestInvokableBlockingCancelable.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<>(), Collections.emptyList(), 0);

				final ActorGateway tm = taskManager;

				new Within(d) {

					@Override
					protected void run() {
						try {
							Future<Object> t1Running = tm.ask(
									new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(eid1),
									timeout);
							Future<Object> t2Running = tm.ask(
									new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(eid2),
									timeout);

							tm.tell(new SubmitTask(tdd1), testActorGateway);
							tm.tell(new SubmitTask(tdd2), testActorGateway);

							expectMsgEquals(Acknowledge.get());
							expectMsgEquals(Acknowledge.get());

							Await.ready(t1Running, d);
							Await.ready(t2Running, d);

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);

							Map<ExecutionAttemptID, Task> runningTasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(2, runningTasks.size());
							Task t1 = runningTasks.get(eid1);
							Task t2 = runningTasks.get(eid2);
							assertNotNull(t1);
							assertNotNull(t2);

							assertEquals(ExecutionState.RUNNING, t1.getExecutionState());
							assertEquals(ExecutionState.RUNNING, t2.getExecutionState());

							tm.tell(new CancelTask(eid1), testActorGateway);

							expectMsgEquals(Acknowledge.get());

							Future<Object> response = tm.ask(new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid1),
									timeout);
							Await.ready(response, d);

							assertEquals(ExecutionState.CANCELED, t1.getExecutionState());

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);
							runningTasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(1, runningTasks.size());

							tm.tell(new CancelTask(eid1), testActorGateway);
							expectMsgEquals(Acknowledge.get());

							tm.tell(new CancelTask(eid2), testActorGateway);
							expectMsgEquals(Acknowledge.get());

							response = tm.ask(new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid2),
									timeout);
							Await.ready(response, d);

							assertEquals(ExecutionState.CANCELED, t2.getExecutionState());

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);
							runningTasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(0, runningTasks.size());
						} catch (Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};
			}
			catch(Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
			finally {
				TestingUtils.stopActor(taskManager);
				TestingUtils.stopActor(jobManager);
			}
		}};
	}

	@Test
	public void testJobSubmissionAndStop() throws Exception {
		new JavaTestKit(system){{

			ActorGateway jobManager = null;
			ActorGateway taskManager = null;

			final ActorGateway testActorGateway = new AkkaActorGateway(
					getTestActor(),
					leaderSessionID);

			try {
				ActorRef jm = system.actorOf(Props.create(SimpleJobManager.class, leaderSessionID));
				jobManager = new AkkaActorGateway(jm, leaderSessionID);

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

				taskManager = TestingUtils.createTaskManager(
						system,
						highAvailabilityServices,
						new Configuration(),
						true,
						true);

				final JobID jid1 = new JobID();
				final JobID jid2 = new JobID();

				JobVertexID vid1 = new JobVertexID();
				JobVertexID vid2 = new JobVertexID();

				final ExecutionAttemptID eid1 = new ExecutionAttemptID();
				final ExecutionAttemptID eid2 = new ExecutionAttemptID();

				final SerializedValue<ExecutionConfig> executionConfig = new SerializedValue<>(new ExecutionConfig());

				final TaskDeploymentDescriptor tdd1 = createTaskDeploymentDescriptor(jid1, "TestJob", vid1, eid1, executionConfig,
						"TestTask1", 5, 1, 5, 0, new Configuration(), new Configuration(), StoppableInvokable.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<>(), Collections.emptyList(), 0);

				final TaskDeploymentDescriptor tdd2 = createTaskDeploymentDescriptor(jid2, "TestJob", vid2, eid2, executionConfig,
						"TestTask2", 7, 2, 7, 0, new Configuration(), new Configuration(), TestInvokableBlockingCancelable.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<>(), Collections.emptyList(), 0);

				final ActorGateway tm = taskManager;

				new Within(d) {

					@Override
					protected void run() {
						try {
							Future<Object> t1Running = tm.ask(
									new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(eid1),
									timeout);
							Future<Object> t2Running = tm.ask(
									new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(eid2),
									timeout);

							tm.tell(new SubmitTask(tdd1), testActorGateway);
							tm.tell(new SubmitTask(tdd2), testActorGateway);

							expectMsgEquals(Acknowledge.get());
							expectMsgEquals(Acknowledge.get());

							Await.ready(t1Running, d);
							Await.ready(t2Running, d);

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);

							Map<ExecutionAttemptID, Task> runningTasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(2, runningTasks.size());
							Task t1 = runningTasks.get(eid1);
							Task t2 = runningTasks.get(eid2);
							assertNotNull(t1);
							assertNotNull(t2);

							assertEquals(ExecutionState.RUNNING, t1.getExecutionState());
							assertEquals(ExecutionState.RUNNING, t2.getExecutionState());

							tm.tell(new StopTask(eid1), testActorGateway);

							expectMsgEquals(Acknowledge.get());

							Future<Object> response = tm.ask(
									new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid1),
									timeout);
							Await.ready(response, d);

							assertEquals(ExecutionState.FINISHED, t1.getExecutionState());

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);
							runningTasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(1, runningTasks.size());

							tm.tell(new StopTask(eid1), testActorGateway);
							expectMsgEquals(Acknowledge.get());

							tm.tell(new StopTask(eid2), testActorGateway);
							expectMsgClass(Status.Failure.class);

							assertEquals(ExecutionState.RUNNING, t2.getExecutionState());

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);
							runningTasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(1, runningTasks.size());
						} catch (Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};
			}
			finally {
				TestingUtils.stopActor(taskManager);
				TestingUtils.stopActor(jobManager);
			}
		}};
	}

	@Test
	public void testGateChannelEdgeMismatch() {
		new JavaTestKit(system){{

			ActorGateway jobManager = null;
			ActorGateway taskManager = null;

			final ActorGateway testActorGateway = new AkkaActorGateway(
					getTestActor(),
					leaderSessionID);

			try {
				ActorRef jm = system.actorOf(Props.create(SimpleJobManager.class, leaderSessionID));
				jobManager = new AkkaActorGateway(jm, leaderSessionID);

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

				taskManager = TestingUtils.createTaskManager(
						system,
						highAvailabilityServices,
						new Configuration(),
						true,
						true);

				final ActorGateway tm = taskManager;

				final JobID jid = new JobID();

				JobVertexID vid1 = new JobVertexID();
				JobVertexID vid2 = new JobVertexID();

				final ExecutionAttemptID eid1 = new ExecutionAttemptID();
				final ExecutionAttemptID eid2 = new ExecutionAttemptID();

				final TaskDeploymentDescriptor tdd1 = createTaskDeploymentDescriptor(
						jid, "TestJob", vid1, eid1,
						new SerializedValue<>(new ExecutionConfig()),
						"Sender", 1, 0, 1, 0,
						new Configuration(), new Configuration(), Tasks.Sender.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<>(), Collections.emptyList(), 0);

				final TaskDeploymentDescriptor tdd2 = createTaskDeploymentDescriptor(
						jid, "TestJob", vid2, eid2,
						new SerializedValue<>(new ExecutionConfig()),
						"Receiver", 7, 2, 7, 0,
						new Configuration(), new Configuration(), Tasks.Receiver.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<>(), Collections.emptyList(), 0);

				new Within(d){

					@Override
					protected void run() {
						try {
							tm.tell(new SubmitTask(tdd1), testActorGateway);
							tm.tell(new SubmitTask(tdd2), testActorGateway);

							expectMsgEquals(Acknowledge.get());
							expectMsgEquals(Acknowledge.get());

							tm.tell(new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid1),
									testActorGateway);
							tm.tell(new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid2),
									testActorGateway);

							expectMsgEquals(true);
							expectMsgEquals(true);

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);
							Map<ExecutionAttemptID, Task> tasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(0, tasks.size());
						} catch (Exception e){
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};
			}
			catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
			finally {
				// shut down the actors
				TestingUtils.stopActor(taskManager);
				TestingUtils.stopActor(jobManager);
			}
		}};
	}

	@Test
	public void testRunJobWithForwardChannel() {
		new JavaTestKit(system){{

			ActorGateway jobManager = null;
			ActorGateway taskManager = null;

			final ActorGateway testActorGateway = new AkkaActorGateway(
					getTestActor(),
					leaderSessionID);
			try {
				final JobID jid = new JobID();

				JobVertexID vid1 = new JobVertexID();
				JobVertexID vid2 = new JobVertexID();

				final ExecutionAttemptID eid1 = new ExecutionAttemptID();
				final ExecutionAttemptID eid2 = new ExecutionAttemptID();

				ActorRef jm = system.actorOf(Props.create(new SimpleLookupJobManagerCreator(leaderSessionID)));
				jobManager = new AkkaActorGateway(jm, leaderSessionID);

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

				taskManager = TestingUtils.createTaskManager(
						system,
						highAvailabilityServices,
						new Configuration(),
						true,
						true);

				final ActorGateway tm = taskManager;

				IntermediateResultPartitionID partitionId = new IntermediateResultPartitionID();

				List<ResultPartitionDeploymentDescriptor> irpdd = new ArrayList<ResultPartitionDeploymentDescriptor>();
				irpdd.add(new ResultPartitionDeploymentDescriptor(new IntermediateDataSetID(), partitionId, ResultPartitionType.PIPELINED, 1, 1, true));

				InputGateDeploymentDescriptor ircdd =
						new InputGateDeploymentDescriptor(
								new IntermediateDataSetID(), ResultPartitionType.PIPELINED,
								0, new InputChannelDeploymentDescriptor[]{
										new InputChannelDeploymentDescriptor(new ResultPartitionID(partitionId, eid1), ResultPartitionLocation.createLocal())
								}
						);

				final TaskDeploymentDescriptor tdd1 = createTaskDeploymentDescriptor(
						jid, "TestJob", vid1, eid1,
						new SerializedValue<>(new ExecutionConfig()),
						"Sender", 1, 0, 1, 0,
						new Configuration(), new Configuration(), Tasks.Sender.class.getName(),
						irpdd, Collections.<InputGateDeploymentDescriptor>emptyList(), new ArrayList<>(),
						Collections.emptyList(), 0);

				final TaskDeploymentDescriptor tdd2 = createTaskDeploymentDescriptor(
						jid, "TestJob", vid2, eid2,
						new SerializedValue<>(new ExecutionConfig()),
						"Receiver", 7, 2, 7, 0,
						new Configuration(), new Configuration(), Tasks.Receiver.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.singletonList(ircdd),
						new ArrayList<>(), Collections.emptyList(), 0);

				new Within(d) {

					@Override
					protected void run() {
						try {
							Future<Object> t1Running = tm.ask(
									new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(eid1),
									timeout);

							Future<Object> t2Running = tm.ask(
									new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(eid2),
									timeout);

							// submit the sender task
							tm.tell(new SubmitTask(tdd1), testActorGateway);
							expectMsgEquals(Acknowledge.get());

							// wait until the sender task is running
							Await.ready(t1Running, d);

							// only now (after the sender is running), submit the receiver task
							tm.tell(new SubmitTask(tdd2), testActorGateway);
							expectMsgEquals(Acknowledge.get());

							// wait until the receiver task is running
							Await.ready(t2Running, d);

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);
							Map<ExecutionAttemptID, Task> tasks = expectMsgClass(TestingTaskManagerMessages.ResponseRunningTasks
									.class).asJava();

							Task t1 = tasks.get(eid1);
							Task t2 = tasks.get(eid2);

							// wait until the tasks are done. thread races may cause the tasks to be done before
							// we get to the check, so we need to guard the check
							if (t1 != null) {
								Future<Object> response = tm.ask(new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid1),
										timeout);
								Await.ready(response, d);
							}

							if (t2 != null) {
								Future<Object> response = tm.ask(new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid2),
										timeout);
								Await.ready(response, d);
								assertEquals(ExecutionState.FINISHED, t2.getExecutionState());
							}

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);
							tasks = expectMsgClass(TestingTaskManagerMessages.ResponseRunningTasks
									.class).asJava();

							assertEquals(0, tasks.size());
						}
						catch (Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};
			}
			catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
			finally {
				// shut down the actors
				TestingUtils.stopActor(taskManager);
				TestingUtils.stopActor(jobManager);
			}
		}};
	}

	@Test
	public void testCancellingDependentAndStateUpdateFails() {
		// this tests creates two tasks. the sender sends data, and fails to send the
		// state update back to the job manager
		// the second one blocks to be canceled
		new JavaTestKit(system){{

			ActorGateway jobManager = null;
			ActorGateway taskManager = null;

			final ActorGateway testActorGateway = new AkkaActorGateway(
					getTestActor(),
					leaderSessionID);
			try {
				final JobID jid = new JobID();

				JobVertexID vid1 = new JobVertexID();
				JobVertexID vid2 = new JobVertexID();

				final ExecutionAttemptID eid1 = new ExecutionAttemptID();
				final ExecutionAttemptID eid2 = new ExecutionAttemptID();

				ActorRef jm = system.actorOf(
						Props.create(
								new SimpleLookupFailingUpdateJobManagerCreator(
										leaderSessionID,
										eid2)
						)
				);

				jobManager = new AkkaActorGateway(jm, leaderSessionID);

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

				taskManager = TestingUtils.createTaskManager(
						system,
						highAvailabilityServices,
						new Configuration(),
						true,
						true);

				final ActorGateway tm = taskManager;

				IntermediateResultPartitionID partitionId = new IntermediateResultPartitionID();

				List<ResultPartitionDeploymentDescriptor> irpdd = new ArrayList<ResultPartitionDeploymentDescriptor>();
				irpdd.add(new ResultPartitionDeploymentDescriptor(new IntermediateDataSetID(), partitionId, ResultPartitionType.PIPELINED, 1, 1, true));

				InputGateDeploymentDescriptor ircdd =
						new InputGateDeploymentDescriptor(
								new IntermediateDataSetID(), ResultPartitionType.PIPELINED,
								0, new InputChannelDeploymentDescriptor[]{
										new InputChannelDeploymentDescriptor(new ResultPartitionID(partitionId, eid1), ResultPartitionLocation.createLocal())
								}
						);

				final TaskDeploymentDescriptor tdd1 = createTaskDeploymentDescriptor(
						jid, "TestJob", vid1, eid1,
						new SerializedValue<>(new ExecutionConfig()),
						"Sender", 1, 0, 1, 0,
						new Configuration(), new Configuration(), Tasks.Sender.class.getName(),
						irpdd, Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<>(), Collections.emptyList(), 0);

				final TaskDeploymentDescriptor tdd2 = createTaskDeploymentDescriptor(
						jid, "TestJob", vid2, eid2,
						new SerializedValue<>(new ExecutionConfig()),
						"Receiver", 7, 2, 7, 0,
						new Configuration(), new Configuration(), Tasks.BlockingReceiver.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.singletonList(ircdd),
						new ArrayList<>(), Collections.emptyList(), 0);

				new Within(d){

					@Override
					protected void run() {
						try {
							Future<Object> t1Running = tm.ask(
									new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(eid1),
									timeout);

							Future<Object> t2Running = tm.ask(
									new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(eid2),
									timeout);

							tm.tell(new SubmitTask(tdd2), testActorGateway);
							tm.tell(new SubmitTask(tdd1), testActorGateway);

							expectMsgEquals(Acknowledge.get());
							expectMsgEquals(Acknowledge.get());

							Await.ready(t1Running, d);
							Await.ready(t2Running, d);

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);
							Map<ExecutionAttemptID, Task> tasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							Task t1 = tasks.get(eid1);
							Task t2 = tasks.get(eid2);

							tm.tell(new CancelTask(eid2), testActorGateway);
							expectMsgEquals(Acknowledge.get());

							if (t2 != null) {
								Future<Object> response = tm.ask(new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid2),
										timeout);
								Await.ready(response, d);
							}

							if (t1 != null) {
								if (t1.getExecutionState() == ExecutionState.RUNNING) {
									tm.tell(new CancelTask(eid1), testActorGateway);
									expectMsgEquals(Acknowledge.get());
								}
								Future<Object> response = tm.ask(
										new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid1),
										timeout);
								Await.ready(response, d);
							}

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);
							tasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(0, tasks.size());
						}
						catch(Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};
			}
			catch(Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
			finally {
				// shut down the actors
				TestingUtils.stopActor(taskManager);
				TestingUtils.stopActor(jobManager);
			}
		}};
	}

	/**
	 * Tests that repeated remote {@link PartitionNotFoundException}s ultimately fail the receiver.
	 */
	@Test
	public void testRemotePartitionNotFound() throws Exception {

		new JavaTestKit(system){{

			ActorGateway jobManager = null;
			ActorGateway taskManager = null;

			final ActorGateway testActorGateway = new AkkaActorGateway(
					getTestActor(),
					leaderSessionID);

			try {
				final IntermediateDataSetID resultId = new IntermediateDataSetID();

				// Create the JM
				ActorRef jm = system.actorOf(Props.create(
						new SimplePartitionStateLookupJobManagerCreator(leaderSessionID, getTestActor())));

				jobManager = new AkkaActorGateway(jm, leaderSessionID);

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

				final int dataPort = NetUtils.getAvailablePort();
				Configuration config = new Configuration();
				config.setInteger(TaskManagerOptions.DATA_PORT, dataPort);
				config.setInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
				config.setInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);

				taskManager = TestingUtils.createTaskManager(
						system,
						highAvailabilityServices,
						config,
						false,
						true);

				// ---------------------------------------------------------------------------------

				final ActorGateway tm = taskManager;

				final JobID jid = new JobID();
				final JobVertexID vid = new JobVertexID();
				final ExecutionAttemptID eid = new ExecutionAttemptID();

				final ResultPartitionID partitionId = new ResultPartitionID();

				// Remote location (on the same TM though) for the partition
				final ResultPartitionLocation loc = ResultPartitionLocation
						.createRemote(new ConnectionID(
								new InetSocketAddress("localhost", dataPort), 0));

				final InputChannelDeploymentDescriptor[] icdd =
						new InputChannelDeploymentDescriptor[] {
								new InputChannelDeploymentDescriptor(partitionId, loc)};

				final InputGateDeploymentDescriptor igdd =
						new InputGateDeploymentDescriptor(resultId, ResultPartitionType.PIPELINED, 0, icdd);

				final TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(
						jid, "TestJob", vid, eid,
						new SerializedValue<>(new ExecutionConfig()),
						"Receiver", 1, 0, 1, 0,
						new Configuration(), new Configuration(),
						Tasks.AgnosticReceiver.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.singletonList(igdd),
						Collections.emptyList(),
						Collections.emptyList(), 0);

				new Within(d) {
					@Override
					protected void run() {
						// Submit the task
						tm.tell(new SubmitTask(tdd), testActorGateway);
						expectMsgClass(Acknowledge.get().getClass());

						// Wait to be notified about the final execution state by the mock JM
						TaskExecutionState msg = expectMsgClass(TaskExecutionState.class);

						// The task should fail after repeated requests
						assertEquals(ExecutionState.FAILED, msg.getExecutionState());
						Throwable t = msg.getError(ClassLoader.getSystemClassLoader());
						assertEquals("Thrown exception was not a PartitionNotFoundException: " + t.getMessage(), 
							PartitionNotFoundException.class, t.getClass());
					}
				};
			}
			catch(Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
			finally {
				TestingUtils.stopActor(taskManager);
				TestingUtils.stopActor(jobManager);
			}
		}};
	}

	@Test
	public void testTaskManagerServicesConfiguration() throws Exception {

		// set some non-default values
		final Configuration config = new Configuration();
		config.setInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
		config.setInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);
		config.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, 10);
		config.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 100);

		TaskManagerServicesConfiguration tmConfig =
			TaskManagerServicesConfiguration.fromConfiguration(config, InetAddress.getLoopbackAddress(), true);

		assertEquals(tmConfig.getNetworkConfig().partitionRequestInitialBackoff(), 100);
		assertEquals(tmConfig.getNetworkConfig().partitionRequestMaxBackoff(), 200);
		assertEquals(tmConfig.getNetworkConfig().networkBuffersPerChannel(), 10);
		assertEquals(tmConfig.getNetworkConfig().floatingNetworkBuffersPerGate(), 100);
	}

	/**
	 *  Tests that repeated local {@link PartitionNotFoundException}s ultimately fail the receiver.
	 */
	@Test
	public void testLocalPartitionNotFound() throws Exception {

		new JavaTestKit(system){{

			ActorGateway jobManager = null;
			ActorGateway taskManager = null;

			final ActorGateway testActorGateway = new AkkaActorGateway(
					getTestActor(),
					leaderSessionID);

			try {
				final IntermediateDataSetID resultId = new IntermediateDataSetID();

				// Create the JM
				ActorRef jm = system.actorOf(Props.create(
						new SimplePartitionStateLookupJobManagerCreator(leaderSessionID, getTestActor())));

				jobManager = new AkkaActorGateway(jm, leaderSessionID);

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

				final Configuration config = new Configuration();
				config.setInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
				config.setInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);

				taskManager = TestingUtils.createTaskManager(
						system,
						highAvailabilityServices,
						config,
						true,
						true);

				// ---------------------------------------------------------------------------------

				final ActorGateway tm = taskManager;

				final JobID jid = new JobID();
				final JobVertexID vid = new JobVertexID();
				final ExecutionAttemptID eid = new ExecutionAttemptID();

				final ResultPartitionID partitionId = new ResultPartitionID();

				// Local location (on the same TM though) for the partition
				final ResultPartitionLocation loc = ResultPartitionLocation.createLocal();

				final InputChannelDeploymentDescriptor[] icdd =
						new InputChannelDeploymentDescriptor[] {
								new InputChannelDeploymentDescriptor(partitionId, loc)};

				final InputGateDeploymentDescriptor igdd =
						new InputGateDeploymentDescriptor(resultId, ResultPartitionType.PIPELINED, 0, icdd);

				final TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(
						jid, "TestJob", vid, eid,
						new SerializedValue<>(new ExecutionConfig()),
						"Receiver", 1, 0, 1, 0,
						new Configuration(), new Configuration(),
						Tasks.AgnosticReceiver.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.singletonList(igdd),
						Collections.emptyList(),
						Collections.emptyList(), 0);

				new Within(new FiniteDuration(120, TimeUnit.SECONDS)) {
					@Override
					protected void run() {
						// Submit the task
						tm.tell(new SubmitTask(tdd), testActorGateway);
						expectMsgClass(Acknowledge.get().getClass());

						// Wait to be notified about the final execution state by the mock JM
						TaskExecutionState msg = expectMsgClass(TaskExecutionState.class);

						// The task should fail after repeated requests
						assertEquals(msg.getExecutionState(), ExecutionState.FAILED);

						Throwable error = msg.getError(getClass().getClassLoader());
						if (error.getClass() != PartitionNotFoundException.class) {
							error.printStackTrace();
							fail("Wrong exception: " + error.getMessage());
						}
					}
				};
			}
			catch(Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
			finally {
				TestingUtils.stopActor(taskManager);
				TestingUtils.stopActor(jobManager);
			}
		}};
	}

	@Test
	public void testLogNotFoundHandling() throws Exception {

		new JavaTestKit(system){{

			// we require a JobManager so that the BlobService is also started
			ActorGateway jobManager = null;
			ActorGateway taskManager = null;

			try {

				// Create the JM
				ActorRef jm = system.actorOf(Props.create(
					new SimplePartitionStateLookupJobManagerCreator(leaderSessionID, getTestActor())));

				jobManager = new AkkaActorGateway(jm, leaderSessionID);

				final int dataPort = NetUtils.getAvailablePort();
				Configuration config = new Configuration();
				config.setInteger(TaskManagerOptions.DATA_PORT, dataPort);
				config.setInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL, 100);
				config.setInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX, 200);
				config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "/i/dont/exist");

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

				taskManager = TestingUtils.createTaskManager(
					system,
					highAvailabilityServices,
					config,
					false,
					true);

				// ---------------------------------------------------------------------------------

				final ActorGateway tm = taskManager;

				new Within(d) {
					@Override
					protected void run() {
						Future<Object> logFuture = tm.ask(TaskManagerMessages.getRequestTaskManagerLog(), timeout);
						try {
							Await.result(logFuture, timeout);
							Assert.fail();
						} catch (Exception e) {
							Assert.assertTrue(e.getMessage().startsWith("TaskManager log files are unavailable. Log file could not be found at"));
						}
					}
				};
			} finally {
				TestingUtils.stopActor(taskManager);
				TestingUtils.stopActor(jobManager);
			}
		}};}

	// ------------------------------------------------------------------------
	// Stack trace sample
	// ------------------------------------------------------------------------

	/**
	 * Tests sampling of task stack traces.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testTriggerStackTraceSampleMessage() throws Exception {
		new JavaTestKit(system) {{
			ActorGateway taskManagerActorGateway = null;

			// We need this to be a JM that answers to update messages for
			// robustness on Travis (if jobs need to be resubmitted in (4)).
			ActorRef jm = system.actorOf(Props.create(new SimpleLookupJobManagerCreator(
				HighAvailabilityServices.DEFAULT_LEADER_ID)));
			ActorGateway jobManagerActorGateway = new AkkaActorGateway(
				jm,
				HighAvailabilityServices.DEFAULT_LEADER_ID);

			final ActorGateway testActorGateway = new AkkaActorGateway(
					getTestActor(),
					HighAvailabilityServices.DEFAULT_LEADER_ID);

			try {
				final ActorGateway jobManager = jobManagerActorGateway;

				highAvailabilityServices.setJobMasterLeaderRetriever(
					HighAvailabilityServices.DEFAULT_JOB_ID,
					new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

				final ActorGateway taskManager = TestingUtils.createTaskManager(
						system,
						highAvailabilityServices,
						new Configuration(),
						true,
						false);

				final JobID jobId = new JobID();

				// Single blocking task
				final TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(
						jobId,
						"Job",
						new JobVertexID(),
						new ExecutionAttemptID(),
						new SerializedValue<>(new ExecutionConfig()),
						"Task",
						1,
						0,
						1,
						0,
						new Configuration(),
						new Configuration(),
						BlockingNoOpInvokable.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						Collections.emptyList(),
						Collections.emptyList(),
						0);

				// Submit the task
				new Within(d) {

					@Override
					protected void run() {
						try {
							// Make sure to register
							Future<?> connectFuture = taskManager.ask(new TestingTaskManagerMessages
									.NotifyWhenRegisteredAtJobManager(jobManager.actor()), remaining());
							Await.ready(connectFuture, remaining());

							Future<Object> taskRunningFuture = taskManager.ask(
									new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(
											tdd.getExecutionAttemptId()), timeout);

							taskManager.tell(new SubmitTask(tdd));

							Await.ready(taskRunningFuture, d);
						} catch (Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};

				//
				// 1) Trigger sample for non-existing task
				//
				new Within(d) {
					@Override
					protected void run() {
						try {
							ExecutionAttemptID taskId = new ExecutionAttemptID();

							taskManager.tell(new TriggerStackTraceSample(
											112223,
											taskId,
											100,
											timeD,
											0),
									testActorGateway);

							// Receive the expected message (heartbeat races possible)
							Object[] msg = receiveN(1);
							while (!(msg[0] instanceof Status.Failure)) {
								msg = receiveN(1);
							}

							Status.Failure response = (Status.Failure) msg[0];

							assertEquals(IllegalStateException.class, response.cause().getClass());
						} catch (Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};

				//
				// 2) Trigger sample for the blocking task
				//
				new Within(d) {
					@Override
					protected void run() {
						boolean success = false;
						Throwable lastError = null;

						for (int i = 0; i < 100 && !success; i++) {
							try {
								int numSamples = 5;

								taskManager.tell(new TriggerStackTraceSample(
												19230,
												tdd.getExecutionAttemptId(),
												numSamples,
												Time.milliseconds(100L),
												0),
										testActorGateway);

								// Receive the expected message (heartbeat races possible)
								Object[] msg = receiveN(1);
								while (!(msg[0] instanceof StackTraceSampleResponse)) {
									msg = receiveN(1);
								}

								StackTraceSampleResponse response = (StackTraceSampleResponse) msg[0];

								// ---- Verify response ----
								assertEquals(19230, response.getSampleId());
								assertEquals(tdd.getExecutionAttemptId(), response.getExecutionAttemptID());

								List<StackTraceElement[]> traces = response.getSamples();

								assertEquals("Number of samples", numSamples, traces.size());

								for (StackTraceElement[] trace : traces) {
									// Look for BlockingNoOpInvokable#invoke
									for (StackTraceElement elem : trace) {
										if (elem.getClassName().equals(
												BlockingNoOpInvokable.class.getName())) {

											assertEquals("invoke", elem.getMethodName());
											success = true;
											break;
										}
									}

									assertTrue("Unexpected stack trace: " +
											Arrays.toString(trace), success);
								}
							} catch (Throwable t) {
								lastError = t;
								LOG.warn("Failed to find invokable.", t);
							}

							try {
								Thread.sleep(100);
							} catch (InterruptedException e) {
								LOG.error("Interrupted while sleeping before retry.", e);
								break;
							}
						}

						if (!success) {
							if (lastError == null) {
								fail("Failed to find invokable");
							} else {
								fail(lastError.getMessage());
							}
						}
					}
				};

				//
				// 3) Trigger sample for the blocking task with max depth
				//
				new Within(d) {
					@Override
					protected void run() {
						try {
							int numSamples = 5;
							int maxDepth = 2;

							taskManager.tell(new TriggerStackTraceSample(
											1337,
											tdd.getExecutionAttemptId(),
											numSamples,
											Time.milliseconds(100L),
											maxDepth),
									testActorGateway);

							// Receive the expected message (heartbeat races possible)
							Object[] msg = receiveN(1);
							while (!(msg[0] instanceof StackTraceSampleResponse)) {
								msg = receiveN(1);
							}

							StackTraceSampleResponse response = (StackTraceSampleResponse) msg[0];

							// ---- Verify response ----
							assertEquals(1337, response.getSampleId());
							assertEquals(tdd.getExecutionAttemptId(), response.getExecutionAttemptID());

							List<StackTraceElement[]> traces = response.getSamples();

							assertEquals("Number of samples", numSamples, traces.size());

							for (StackTraceElement[] trace : traces) {
								assertEquals("Max depth", maxDepth, trace.length);
							}
						} catch (Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};

				//
				// 4) Trigger sample for the blocking task, but cancel it during sampling
				//
				new Within(d) {
					@Override
					protected void run() {
						try {
							int maxAttempts = 10;
							int sleepTime = 100;
							for (int i = 0; i < maxAttempts; i++, sleepTime *= 2) {
								// Trigger many samples in order to cancel the task
								// during a sample
								taskManager.tell(
									new TriggerStackTraceSample(
										44,
										tdd.getExecutionAttemptId(),
										Integer.MAX_VALUE,
										Time.milliseconds(10L),
										0),
									testActorGateway);

								Thread.sleep(sleepTime);

								Future<?> removeFuture = taskManager.ask(
										new TestingJobManagerMessages.NotifyWhenJobRemoved(jobId),
										remaining());

								// Cancel the task
								taskManager.tell(new CancelTask(tdd.getExecutionAttemptId()));

								// Receive the expected message (heartbeat races possible)
								while (true) {
									Object[] msg = receiveN(1);
									if (msg[0] instanceof StackTraceSampleResponse) {
										StackTraceSampleResponse response = (StackTraceSampleResponse) msg[0];

										assertEquals(tdd.getExecutionAttemptId(), response.getExecutionAttemptID());
										assertEquals(44, response.getSampleId());

										// Done
										return;
									} else if (msg[0] instanceof Failure) {
										// Wait for removal before resubmitting
										Await.ready(removeFuture, remaining());

										Future<?> taskRunningFuture = taskManager.ask(
												new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(
														tdd.getExecutionAttemptId()), timeout);

										// Resubmit
										taskManager.tell(new SubmitTask(tdd));

										Await.ready(taskRunningFuture, remaining());

										// Retry the sample message
										break;
									} else {
										// Different message
										continue;
									}
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};
			} finally {
				TestingUtils.stopActor(taskManagerActorGateway);
				TestingUtils.stopActor(jobManagerActorGateway);
			}
		}};
	}

	@Test
	public void testTerminationOnFatalError() {
		highAvailabilityServices.setJobMasterLeaderRetriever(
			HighAvailabilityServices.DEFAULT_JOB_ID,
			new SettableLeaderRetrievalService());

		new JavaTestKit(system){{

			final ActorGateway taskManager = TestingUtils.createTaskManager(
					system,
					highAvailabilityServices, // no jobmanager
					new Configuration(),
					true,
					false);

			try {
				watch(taskManager.actor());
				taskManager.tell(new FatalError("test fatal error", new Exception("something super bad")));
				expectTerminated(d, taskManager.actor());
			}
			finally {
				taskManager.tell(Kill.getInstance());
			}
		}};
	}

	/**
	 * Test that a failing schedule or update consumers call leads to the failing of the respective
	 * task.
	 *
	 * IMPORTANT: We have to make sure that the invokable's cancel method is called, because only
	 * then the future is completed. We do this by not eagerly deploy consumer tasks and requiring
	 * the invokable to fill one memory segment. The completed memory segment will trigger the
	 * scheduling of the downstream operator since it is in pipeline mode. After we've filled the
	 * memory segment, we'll block the invokable and wait for the task failure due to the failed
	 * schedule or update consumers call.
	 */
	@Test(timeout = 10000L)
	public void testFailingScheduleOrUpdateConsumersMessage() throws Exception {
		new JavaTestKit(system) {{
			final Configuration configuration = new Configuration();

			// set the memory segment to the smallest size possible, because we have to fill one
			// memory buffer to trigger the schedule or update consumers message to the downstream
			// operators
			configuration.setString(TaskManagerOptions.MEMORY_SEGMENT_SIZE, "4096");

			final JobID jid = new JobID();
			final JobVertexID vid = new JobVertexID();
			final ExecutionAttemptID eid = new ExecutionAttemptID();
			final SerializedValue<ExecutionConfig> executionConfig = new SerializedValue<>(new ExecutionConfig());

			final ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor = new ResultPartitionDeploymentDescriptor(
				new IntermediateDataSetID(),
				new IntermediateResultPartitionID(),
				ResultPartitionType.PIPELINED,
				1,
				1,
				true);

			final TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(jid, "TestJob", vid, eid, executionConfig,
				"TestTask", 1, 0, 1, 0, new Configuration(), new Configuration(),
				TestInvokableRecordCancel.class.getName(),
				Collections.singletonList(resultPartitionDeploymentDescriptor),
				Collections.<InputGateDeploymentDescriptor>emptyList(),
				new ArrayList<>(), Collections.emptyList(), 0);


			ActorRef jmActorRef = system.actorOf(Props.create(FailingScheduleOrUpdateConsumersJobManager.class, leaderSessionID), "jobmanager");
			ActorGateway jobManager = new AkkaActorGateway(jmActorRef, leaderSessionID);

			highAvailabilityServices.setJobMasterLeaderRetriever(
				HighAvailabilityServices.DEFAULT_JOB_ID,
				new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

			final ActorGateway taskManager = TestingUtils.createTaskManager(
				system,
				highAvailabilityServices,
				configuration,
				true,
				true);

			try {
				TestInvokableRecordCancel.resetGotCanceledFuture();

				Future<Object> result = taskManager.ask(new SubmitTask(tdd), timeout);

				Await.result(result, timeout);

				CompletableFuture<Boolean> cancelFuture = TestInvokableRecordCancel.gotCanceled();

				assertEquals(true, cancelFuture.get());
			} finally {
				TestingUtils.stopActor(taskManager);
				TestingUtils.stopActor(jobManager);
			}
		}};
	}

	/**
	 * Tests that the TaskManager sends a proper exception back to the sender if the submit task
	 * message fails.
	 */
	@Test
	public void testSubmitTaskFailure() throws Exception {
		ActorGateway jobManager = null;
		ActorGateway taskManager = null;

		try {

			ActorRef jm = system.actorOf(Props.create(SimpleJobManager.class, leaderSessionID));
			jobManager = new AkkaActorGateway(jm, leaderSessionID);

			highAvailabilityServices.setJobMasterLeaderRetriever(
				HighAvailabilityServices.DEFAULT_JOB_ID,
				new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

			taskManager = TestingUtils.createTaskManager(
				system,
				highAvailabilityServices,
				new Configuration(),
				true,
				true);

			TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(
				new JobID(),
				"test job",
				new JobVertexID(),
				new ExecutionAttemptID(),
				new SerializedValue<>(new ExecutionConfig()),
				"test task",
				0, // this will make the submission fail because the number of key groups must be >= 1
				0,
				1,
				0,
				new Configuration(),
				new Configuration(),
				"Foobar",
				Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
				Collections.<InputGateDeploymentDescriptor>emptyList(),
				Collections.emptyList(),
				Collections.emptyList(),
				0);

			Future<Object> submitResponse = taskManager.ask(new SubmitTask(tdd), timeout);

			try {
				Await.result(submitResponse, timeout);

				fail("The submit task message should have failed.");
			} catch (IllegalArgumentException e) {
				// expected
			}
		} finally {
			TestingUtils.stopActor(jobManager);
			TestingUtils.stopActor(taskManager);
		}
	}

	/**
	 * Tests that the TaskManager sends a proper exception back to the sender if the stop task
	 * message fails.
	 */
	@Test
	public void testStopTaskFailure() throws Exception {
		ActorGateway jobManager = null;
		ActorGateway taskManager = null;

		try {
			final ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();

			ActorRef jm = system.actorOf(Props.create(SimpleJobManager.class, leaderSessionID));
			jobManager = new AkkaActorGateway(jm, leaderSessionID);

			highAvailabilityServices.setJobMasterLeaderRetriever(
				HighAvailabilityServices.DEFAULT_JOB_ID,
				new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

			taskManager = TestingUtils.createTaskManager(
				system,
				highAvailabilityServices,
				new Configuration(),
				true,
				true);

			TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(
				new JobID(),
				"test job",
				new JobVertexID(),
				executionAttemptId,
				new SerializedValue<>(new ExecutionConfig()),
				"test task",
				1,
				0,
				1,
				0,
				new Configuration(),
				new Configuration(),
				BlockingNoOpInvokable.class.getName(),
				Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
				Collections.<InputGateDeploymentDescriptor>emptyList(),
				Collections.emptyList(),
				Collections.emptyList(),
				0);

			Future<Object> submitResponse = taskManager.ask(new SubmitTask(tdd), timeout);

			Await.result(submitResponse, timeout);

			final Future<Object> taskRunning = taskManager.ask(new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(executionAttemptId), timeout);

			Await.result(taskRunning, timeout);

			Future<Object> stopResponse = taskManager.ask(new StopTask(executionAttemptId), timeout);

			try {
				Await.result(stopResponse, timeout);

				fail("The stop task message should have failed.");
			} catch (UnsupportedOperationException e) {
				// expected
			}
		} finally {
			TestingUtils.stopActor(jobManager);
			TestingUtils.stopActor(taskManager);
		}
	}

	/**
	 * Tests that the TaskManager sends a proper exception back to the sender if the trigger stack
	 * trace message fails.
	 */
	@Test
	public void testStackTraceSampleFailure() throws Exception {
		ActorGateway jobManager = null;
		ActorGateway taskManager = null;

		try {

			ActorRef jm = system.actorOf(Props.create(SimpleJobManager.class, leaderSessionID));
			jobManager = new AkkaActorGateway(jm, leaderSessionID);

			highAvailabilityServices.setJobMasterLeaderRetriever(
				HighAvailabilityServices.DEFAULT_JOB_ID,
				new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

			taskManager = TestingUtils.createTaskManager(
				system,
				highAvailabilityServices,
				new Configuration(),
				true,
				true);

			Future<Object> stackTraceResponse = taskManager.ask(
				new TriggerStackTraceSample(
					0,
					new ExecutionAttemptID(),
					0,
					Time.milliseconds(1L),
					0),
				timeout);

			try {
				Await.result(stackTraceResponse, timeout);

				fail("The trigger stack trace message should have failed.");
			} catch (IllegalStateException e) {
				// expected
			}
		} finally {
			TestingUtils.stopActor(jobManager);
			TestingUtils.stopActor(taskManager);
		}
	}

	/**
	 * Tests that the TaskManager sends a proper exception back to the sender if the trigger stack
	 * trace message fails.
	 */
	@Test
	public void testUpdateTaskInputPartitionsFailure() throws Exception {
		ActorGateway jobManager = null;
		ActorGateway taskManager = null;

		try {

			final ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();

			ActorRef jm = system.actorOf(Props.create(SimpleJobManager.class, leaderSessionID));
			jobManager = new AkkaActorGateway(jm, leaderSessionID);

			highAvailabilityServices.setJobMasterLeaderRetriever(
				HighAvailabilityServices.DEFAULT_JOB_ID,
				new StandaloneLeaderRetrievalService(jobManager.path(), jobManager.leaderSessionID()));

			taskManager = TestingUtils.createTaskManager(
				system,
				highAvailabilityServices,
				new Configuration(),
				true,
				true);

			TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(
				new JobID(),
				"test job",
				new JobVertexID(),
				executionAttemptId,
				new SerializedValue<>(new ExecutionConfig()),
				"test task",
				1,
				0,
				1,
				0,
				new Configuration(),
				new Configuration(),
				BlockingNoOpInvokable.class.getName(),
				Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
				Collections.<InputGateDeploymentDescriptor>emptyList(),
				Collections.emptyList(),
				Collections.emptyList(),
				0);

			Future<Object> submitResponse = taskManager.ask(new SubmitTask(tdd), timeout);

			Await.result(submitResponse, timeout);

			Future<Object> partitionUpdateResponse = taskManager.ask(
				new TaskMessages.UpdateTaskSinglePartitionInfo(
					executionAttemptId,
					new IntermediateDataSetID(),
					new InputChannelDeploymentDescriptor(new ResultPartitionID(), ResultPartitionLocation.createLocal())),
				timeout);

			try {
				Await.result(partitionUpdateResponse, timeout);

				fail("The update task input partitions message should have failed.");
			} catch (Exception e) {
				// expected
			}
		} finally {
			TestingUtils.stopActor(jobManager);
			TestingUtils.stopActor(taskManager);
		}
	}

	// --------------------------------------------------------------------------------------------

	public static class SimpleJobManager extends FlinkUntypedActor {

		private final UUID leaderSessionID;

		public SimpleJobManager(UUID leaderSessionID) {
			this.leaderSessionID = leaderSessionID;
		}

		@Override
		public void handleMessage(Object message) throws Exception {
			if (message instanceof RegistrationMessages.RegisterTaskManager) {
				final InstanceID iid = new InstanceID();
				final ActorRef self = getSelf();
				getSender().tell(
						decorateMessage(
								new RegistrationMessages.AcknowledgeRegistration(
									iid,
									12345)
						),
						self);
			}
			else if(message instanceof TaskMessages.UpdateTaskExecutionState){
				getSender().tell(true, getSelf());
			}
		}

		@Override
		protected UUID getLeaderSessionID() {
			return leaderSessionID;
		}
	}

	public static class FailingScheduleOrUpdateConsumersJobManager extends SimpleJobManager {

		public FailingScheduleOrUpdateConsumersJobManager(UUID leaderSessionId) {
			super(leaderSessionId);
		}

		@Override
		public void handleMessage(Object message) throws Exception {
			if (message instanceof ScheduleOrUpdateConsumers) {
				getSender().tell(
					decorateMessage(
						new Status.Failure(new Exception("Could not schedule or update consumers."))),
					getSelf());
			} else {
				super.handleMessage(message);
			}
		}
	}

	public static class SimpleLookupJobManager extends SimpleJobManager {

		public SimpleLookupJobManager(UUID leaderSessionID) {
			super(leaderSessionID);
		}

		@Override
		public void handleMessage(Object message) throws Exception {
			if (message instanceof ScheduleOrUpdateConsumers) {
				getSender().tell(
						decorateMessage(Acknowledge.get()),
						getSelf()
						);
			} else {
				super.handleMessage(message);
			}
		}
	}

	public static class SimpleLookupFailingUpdateJobManager extends SimpleLookupJobManager{

		private final Set<ExecutionAttemptID> validIDs;

		public SimpleLookupFailingUpdateJobManager(UUID leaderSessionID, Set<ExecutionAttemptID> ids) {
			super(leaderSessionID);
			this.validIDs = new HashSet<>(ids);
		}

		@Override
		public void handleMessage(Object message) throws Exception{
			if (message instanceof TaskMessages.UpdateTaskExecutionState) {
				TaskMessages.UpdateTaskExecutionState updateMsg =
						(TaskMessages.UpdateTaskExecutionState) message;

				if(validIDs.contains(updateMsg.taskExecutionState().getID())) {
					getSender().tell(true, getSelf());
				} else {
					getSender().tell(false, getSelf());
				}
			} else {
				super.handleMessage(message);
			}
		}
	}

	public static class SimplePartitionStateLookupJobManager extends SimpleJobManager {

		private final ActorRef testActor;

		public SimplePartitionStateLookupJobManager(UUID leaderSessionID, ActorRef testActor) {
			super(leaderSessionID);
			this.testActor = testActor;
		}

		@Override
		public void handleMessage(Object message) throws Exception {
			if (message instanceof RequestPartitionProducerState) {
				getSender().tell(decorateMessage(ExecutionState.RUNNING), getSelf());
			}
			else if (message instanceof TaskMessages.UpdateTaskExecutionState) {
				final TaskExecutionState msg = ((TaskMessages.UpdateTaskExecutionState) message)
						.taskExecutionState();

				if (msg.getExecutionState().isTerminal()) {
					testActor.tell(msg, self());
				}
			} else {
				super.handleMessage(message);
			}
		}
	}

	public static class SimpleLookupJobManagerCreator implements Creator<SimpleLookupJobManager>{

		private final UUID leaderSessionID;

		public SimpleLookupJobManagerCreator(UUID leaderSessionID) {
			this.leaderSessionID = leaderSessionID;
		}

		@Override
		public SimpleLookupJobManager create() throws Exception {
			return new SimpleLookupJobManager(leaderSessionID);
		}
	}

	public static class SimpleLookupFailingUpdateJobManagerCreator implements Creator<SimpleLookupFailingUpdateJobManager>{

		private final UUID leaderSessionID;

		private final Set<ExecutionAttemptID> validIDs;

		public SimpleLookupFailingUpdateJobManagerCreator(UUID leaderSessionID, ExecutionAttemptID ... ids) {
			this.leaderSessionID = leaderSessionID;

			validIDs = new HashSet<ExecutionAttemptID>();

			for(ExecutionAttemptID id : ids) {
				this.validIDs.add(id);
			}
		}

		@Override
		public SimpleLookupFailingUpdateJobManager create() throws Exception {
			return new SimpleLookupFailingUpdateJobManager(leaderSessionID, validIDs);
		}
	}

	public static class SimplePartitionStateLookupJobManagerCreator implements Creator<SimplePartitionStateLookupJobManager>{

		private final UUID leaderSessionID;

		private final ActorRef testActor;

		public SimplePartitionStateLookupJobManagerCreator(UUID leaderSessionID, ActorRef testActor) {
			this.leaderSessionID = leaderSessionID;

			this.testActor = testActor;
		}

		@Override
		public SimplePartitionStateLookupJobManager create() throws Exception {
			return new SimplePartitionStateLookupJobManager(leaderSessionID, testActor);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class TestInvokableCorrect extends AbstractInvokable {

		public TestInvokableCorrect(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() {}
	}
	
	public static class TestInvokableBlockingCancelable extends AbstractInvokable {

		public TestInvokableBlockingCancelable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			final Object o = new Object();
			//noinspection SynchronizationOnLocalVariableOrMethodParameter
			synchronized (o) {
				//noinspection InfiniteLoopStatement
				while (true) {
					o.wait();
				}
			}
		}
	}

	public static final class TestInvokableRecordCancel extends AbstractInvokable {

		private static final Object lock = new Object();
		private static CompletableFuture<Boolean> gotCanceledFuture = new CompletableFuture<>();

		public TestInvokableRecordCancel(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			final Object o = new Object();
			RecordWriter<IntValue> recordWriter = new RecordWriter<>(getEnvironment().getWriter(0));

			for (int i = 0; i < 1024; i++) {
				recordWriter.emit(new IntValue(42));
			}

			synchronized (o) {
				//noinspection InfiniteLoopStatement
				while (true) {
					o.wait();
				}
			}

		}

		@Override
		public void cancel() {
			synchronized (lock) {
				gotCanceledFuture.complete(true);
			}
		}

		public static void resetGotCanceledFuture() {
			synchronized (lock) {
				gotCanceledFuture = new CompletableFuture<>();
			}
		}

		public static CompletableFuture<Boolean> gotCanceled() {
			synchronized (lock) {
				return gotCanceledFuture;
			}
		}
	}

	private static TaskDeploymentDescriptor createTaskDeploymentDescriptor(
		JobID jobId,
		String jobName,
		JobVertexID jobVertexId,
		ExecutionAttemptID executionAttemptId,
		SerializedValue<ExecutionConfig> serializedExecutionConfig,
		String taskName,
		int numberOfKeyGroups,
		int subtaskIndex,
		int parallelism,
		int attemptNumber,
		Configuration jobConfiguration,
		Configuration taskConfiguration,
		String invokableClassName,
		Collection<ResultPartitionDeploymentDescriptor> producedPartitions,
		Collection<InputGateDeploymentDescriptor> inputGates,
		Collection<PermanentBlobKey> requiredJarFiles,
		Collection<URL> requiredClasspaths,
		int targetSlotNumber) throws IOException {

		JobInformation jobInformation = new JobInformation(
			jobId,
			jobName,
			serializedExecutionConfig,
			jobConfiguration,
			requiredJarFiles,
			requiredClasspaths);

		TaskInformation taskInformation = new TaskInformation(
			jobVertexId,
			taskName,
			parallelism,
			numberOfKeyGroups,
			invokableClassName,
			taskConfiguration);

		SerializedValue<JobInformation> serializedJobInformation = new SerializedValue<>(jobInformation);
		SerializedValue<TaskInformation> serializedJobVertexInformation = new SerializedValue<>(taskInformation);

		return new TaskDeploymentDescriptor(
			jobId,
			new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobInformation),
			new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobVertexInformation),
			executionAttemptId,
			new AllocationID(),
			subtaskIndex,
			attemptNumber,
			targetSlotNumber,
			null,
			producedPartitions,
			inputGates);

	}
}
