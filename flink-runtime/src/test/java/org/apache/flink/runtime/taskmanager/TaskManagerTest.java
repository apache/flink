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
import akka.actor.Props;
import akka.japi.Creator;
import akka.testkit.JavaTestKit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.messages.Messages;
import org.apache.flink.runtime.messages.RegistrationMessages;
import org.apache.flink.runtime.messages.StackTraceSampleMessages.ResponseStackTraceSampleFailure;
import org.apache.flink.runtime.messages.StackTraceSampleMessages.ResponseStackTraceSampleSuccess;
import org.apache.flink.runtime.messages.StackTraceSampleMessages.TriggerStackTraceSample;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.messages.TaskMessages;
import org.apache.flink.runtime.messages.TaskMessages.CancelTask;
import org.apache.flink.runtime.messages.TaskMessages.PartitionState;
import org.apache.flink.runtime.messages.TaskMessages.StopTask;
import org.apache.flink.runtime.messages.TaskMessages.SubmitTask;
import org.apache.flink.runtime.messages.TaskMessages.TaskOperationResult;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.StoppableInvokable;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.messages.JobManagerMessages.RequestPartitionState;
import static org.apache.flink.runtime.messages.JobManagerMessages.ScheduleOrUpdateConsumers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("serial")
public class TaskManagerTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerTest.class);

	private static final FiniteDuration timeout = new FiniteDuration(1, TimeUnit.MINUTES);

	private static final FiniteDuration d = new FiniteDuration(20, TimeUnit.SECONDS);

	private static ActorSystem system;

	final static UUID leaderSessionID = null;

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	@Test
	public void testSubmitAndExecuteTask() {
		new JavaTestKit(system){{

			ActorGateway taskManager = null;
			final ActorGateway jobManager = TestingUtils.createForwardingJobManager(
					system,
					getTestActor(),
					Option.<String>empty());

			try {
				taskManager = TestingUtils.createTaskManager(
						system,
						jobManager,
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

				final TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(jid, vid, eid, "TestTask", 2, 7, 0,
						new Configuration(), new Configuration(), TestInvokableCorrect.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<BlobKey>(), Collections.<URL>emptyList(), 0);


				new Within(d) {

					@Override
					protected void run() {
						tm.tell(new SubmitTask(tdd), jobManager);

						// TaskManager should acknowledge the submission
						// heartbeats may be interleaved
						long deadline = System.currentTimeMillis() + 10000;
						do {
							Object message = receiveOne(d);
							if (message == Messages.getAcknowledge()) {
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

				taskManager = TestingUtils.createTaskManager(
						system,
						jobManager,
						new Configuration(),
						true,
						true);

				final JobID jid1 = new JobID();
				final JobID jid2 = new JobID();

				JobVertexID vid1 = new JobVertexID();
				JobVertexID vid2 = new JobVertexID();

				final ExecutionAttemptID eid1 = new ExecutionAttemptID();
				final ExecutionAttemptID eid2 = new ExecutionAttemptID();

				final TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid1, vid1, eid1, "TestTask1", 1, 5, 0,
						new Configuration(), new Configuration(), TestInvokableBlockingCancelable.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<BlobKey>(), Collections.<URL>emptyList(), 0);

				final TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid2, vid2, eid2, "TestTask2", 2, 7, 0,
						new Configuration(), new Configuration(), TestInvokableBlockingCancelable.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<BlobKey>(), Collections.<URL>emptyList(), 0);

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

							expectMsgEquals(Messages.getAcknowledge());
							expectMsgEquals(Messages.getAcknowledge());

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

							expectMsgEquals(new TaskOperationResult(eid1, true));

							Future<Object> response = tm.ask(new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid1),
									timeout);
							Await.ready(response, d);

							assertEquals(ExecutionState.CANCELED, t1.getExecutionState());

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);
							runningTasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(1, runningTasks.size());

							tm.tell(new CancelTask(eid1), testActorGateway);
							expectMsgEquals(new TaskOperationResult(eid1, false, "No task with that execution ID was " +
									"found."));

							tm.tell(new CancelTask(eid2), testActorGateway);
							expectMsgEquals(new TaskOperationResult(eid2, true));

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
	public void testJobSubmissionAndStop() {
		new JavaTestKit(system){{

			ActorGateway jobManager = null;
			ActorGateway taskManager = null;

			final ActorGateway testActorGateway = new AkkaActorGateway(
					getTestActor(),
					leaderSessionID);

			try {
				ActorRef jm = system.actorOf(Props.create(SimpleJobManager.class, leaderSessionID));
				jobManager = new AkkaActorGateway(jm, leaderSessionID);

				taskManager = TestingUtils.createTaskManager(
						system,
						jobManager,
						new Configuration(),
						true,
						true);

				final JobID jid1 = new JobID();
				final JobID jid2 = new JobID();

				JobVertexID vid1 = new JobVertexID();
				JobVertexID vid2 = new JobVertexID();

				final ExecutionAttemptID eid1 = new ExecutionAttemptID();
				final ExecutionAttemptID eid2 = new ExecutionAttemptID();

				final TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid1, vid1, eid1, "TestTask1", 1, 5, 0,
						new Configuration(), new Configuration(), StoppableInvokable.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<BlobKey>(), Collections.<URL>emptyList(), 0);

				final TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid2, vid2, eid2, "TestTask2", 2, 7, 0,
						new Configuration(), new Configuration(), TestInvokableBlockingCancelable.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<BlobKey>(), Collections.<URL>emptyList(), 0);

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

							expectMsgEquals(Messages.getAcknowledge());
							expectMsgEquals(Messages.getAcknowledge());

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

							expectMsgEquals(new TaskOperationResult(eid1, true));

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
							expectMsgEquals(new TaskOperationResult(eid1, false, "No task with that execution ID was " +
									"found."));

							tm.tell(new StopTask(eid2), testActorGateway);
							expectMsgEquals(new TaskOperationResult(eid2, false, "UnsupportedOperationException: Stopping not supported by this task."));

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

				taskManager = TestingUtils.createTaskManager(
						system,
						jobManager,
						new Configuration(),
						true,
						true);

				final ActorGateway tm = taskManager;

				final JobID jid = new JobID();

				JobVertexID vid1 = new JobVertexID();
				JobVertexID vid2 = new JobVertexID();

				final ExecutionAttemptID eid1 = new ExecutionAttemptID();
				final ExecutionAttemptID eid2 = new ExecutionAttemptID();

				final TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid, vid1, eid1, "Sender", 0, 1, 0,
						new Configuration(), new Configuration(), Tasks.Sender.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<BlobKey>(), Collections.<URL>emptyList(), 0);

				final TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid, vid2, eid2, "Receiver", 2, 7, 0,
						new Configuration(), new Configuration(), Tasks.Receiver.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<BlobKey>(), Collections.<URL>emptyList(), 0);

				new Within(d){

					@Override
					protected void run() {
						try {
							tm.tell(new SubmitTask(tdd1), testActorGateway);
							tm.tell(new SubmitTask(tdd2), testActorGateway);

							expectMsgEquals(Messages.getAcknowledge());
							expectMsgEquals(Messages.getAcknowledge());

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

				taskManager = TestingUtils.createTaskManager(
						system,
						jobManager,
						new Configuration(),
						true,
						true);

				final ActorGateway tm = taskManager;

				IntermediateResultPartitionID partitionId = new IntermediateResultPartitionID();

				List<ResultPartitionDeploymentDescriptor> irpdd = new ArrayList<ResultPartitionDeploymentDescriptor>();
				irpdd.add(new ResultPartitionDeploymentDescriptor(new IntermediateDataSetID(), partitionId, ResultPartitionType.PIPELINED, 1, false));

				InputGateDeploymentDescriptor ircdd =
						new InputGateDeploymentDescriptor(
								new IntermediateDataSetID(),
								0, new InputChannelDeploymentDescriptor[]{
										new InputChannelDeploymentDescriptor(new ResultPartitionID(partitionId, eid1), ResultPartitionLocation.createLocal())
								}
						);

				final TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid, vid1, eid1, "Sender", 0, 1, 0,
						new Configuration(), new Configuration(), Tasks.Sender.class.getName(),
						irpdd, Collections.<InputGateDeploymentDescriptor>emptyList(), new ArrayList<BlobKey>(),
						Collections.<URL>emptyList(), 0);

				final TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid, vid2, eid2, "Receiver", 2, 7, 0,
						new Configuration(), new Configuration(), Tasks.Receiver.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.singletonList(ircdd),
						new ArrayList<BlobKey>(), Collections.<URL>emptyList(), 0);

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
							expectMsgEquals(Messages.getAcknowledge());

							// wait until the sender task is running
							Await.ready(t1Running, d);

							// only now (after the sender is running), submit the receiver task
							tm.tell(new SubmitTask(tdd2), testActorGateway);
							expectMsgEquals(Messages.getAcknowledge());

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

				taskManager = TestingUtils.createTaskManager(
						system,
						jobManager,
						new Configuration(),
						true,
						true);

				final ActorGateway tm = taskManager;

				IntermediateResultPartitionID partitionId = new IntermediateResultPartitionID();

				List<ResultPartitionDeploymentDescriptor> irpdd = new ArrayList<ResultPartitionDeploymentDescriptor>();
				irpdd.add(new ResultPartitionDeploymentDescriptor(new IntermediateDataSetID(), partitionId, ResultPartitionType.PIPELINED, 1, false));

				InputGateDeploymentDescriptor ircdd =
						new InputGateDeploymentDescriptor(
								new IntermediateDataSetID(),
								0, new InputChannelDeploymentDescriptor[]{
										new InputChannelDeploymentDescriptor(new ResultPartitionID(partitionId, eid1), ResultPartitionLocation.createLocal())
								}
						);

				final TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid, vid1, eid1, "Sender", 0, 1, 0,
						new Configuration(), new Configuration(), Tasks.Sender.class.getName(),
						irpdd, Collections.<InputGateDeploymentDescriptor>emptyList(),
						new ArrayList<BlobKey>(), Collections.<URL>emptyList(), 0);

				final TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid, vid2, eid2, "Receiver", 2, 7, 0,
						new Configuration(), new Configuration(), Tasks.BlockingReceiver.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.singletonList(ircdd),
						new ArrayList<BlobKey>(), Collections.<URL>emptyList(), 0);

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

							expectMsgEquals(Messages.getAcknowledge());
							expectMsgEquals(Messages.getAcknowledge());

							Await.ready(t1Running, d);
							Await.ready(t2Running, d);

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), testActorGateway);
							Map<ExecutionAttemptID, Task> tasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							Task t1 = tasks.get(eid1);
							Task t2 = tasks.get(eid2);

							tm.tell(new CancelTask(eid2), testActorGateway);
							expectMsgEquals(new TaskOperationResult(eid2, true));

							if (t2 != null) {
								Future<Object> response = tm.ask(new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid2),
										timeout);
								Await.ready(response, d);
							}

							if (t1 != null) {
								if (t1.getExecutionState() == ExecutionState.RUNNING) {
									tm.tell(new CancelTask(eid1), testActorGateway);
									expectMsgEquals(new TaskOperationResult(eid1, true));
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

				final int dataPort = NetUtils.getAvailablePort();
				Configuration config = new Configuration();
				config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, dataPort);

				taskManager = TestingUtils.createTaskManager(
						system,
						jobManager,
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
						new InputGateDeploymentDescriptor(resultId, 0, icdd);

				final TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
						jid, vid, eid, "Receiver", 0, 1, 0,
						new Configuration(), new Configuration(),
						Tasks.AgnosticReceiver.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.singletonList(igdd),
						Collections.<BlobKey>emptyList(),
						Collections.<URL>emptyList(), 0);

				new Within(d) {
					@Override
					protected void run() {
						// Submit the task
						tm.tell(new SubmitTask(tdd), testActorGateway);
						expectMsgClass(Messages.getAcknowledge().getClass());

						// Wait to be notified about the final execution state by the mock JM
						TaskExecutionState msg = expectMsgClass(TaskExecutionState.class);

						// The task should fail after repeated requests
						assertEquals(ExecutionState.FAILED, msg.getExecutionState());
						assertEquals(PartitionNotFoundException.class,
								msg.getError(ClassLoader.getSystemClassLoader()).getClass());
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

				final int dataPort = NetUtils.getAvailablePort();
				final Configuration config = new Configuration();

				config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, dataPort);

				taskManager = TestingUtils.createTaskManager(
						system,
						jobManager,
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
						new InputGateDeploymentDescriptor(resultId, 0, icdd);

				final TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
						jid, vid, eid, "Receiver", 0, 1, 0,
						new Configuration(), new Configuration(),
						Tasks.AgnosticReceiver.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.singletonList(igdd),
						Collections.<BlobKey>emptyList(),
						Collections.<URL>emptyList(), 0);

				new Within(new FiniteDuration(120, TimeUnit.SECONDS)) {
					@Override
					protected void run() {
						// Submit the task
						tm.tell(new SubmitTask(tdd), testActorGateway);
						expectMsgClass(Messages.getAcknowledge().getClass());

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
			ActorRef jm = system.actorOf(Props.create(new SimpleLookupJobManagerCreator(null)));
			ActorGateway jobManagerActorGateway = new AkkaActorGateway(jm, null);

			final ActorGateway testActorGateway = new AkkaActorGateway(
					getTestActor(),
					leaderSessionID);

			try {
				final ActorGateway jobManager = jobManagerActorGateway;
				final ActorGateway taskManager = TestingUtils.createTaskManager(
						system,
						jobManager,
						new Configuration(),
						true,
						false);

				// Single blocking task
				final TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
						new JobID(),
						new JobVertexID(),
						new ExecutionAttemptID(),
						"Task",
						0,
						1,
						0,
						new Configuration(),
						new Configuration(),
						Tasks.BlockingNoOpInvokable.class.getName(),
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						Collections.<BlobKey>emptyList(),
						Collections.<URL>emptyList(),
						0);

				// Make sure to register
				Future<?> connectFuture = taskManager.ask(new TestingTaskManagerMessages
						.NotifyWhenRegisteredAtJobManager(jobManager.actor()), remaining());
				Await.ready(connectFuture, remaining());

				// Submit the task
				new Within(d) {
					@Override
					protected void run() {
						try {
							Future<Object> taskRunningFuture = taskManager.ask(
									new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(
											tdd.getExecutionId()), timeout);

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
											d,
											0),
									testActorGateway);

							// Receive the expected message (heartbeat races possible)
							Object[] msg = receiveN(1);
							while (!(msg[0] instanceof ResponseStackTraceSampleFailure)) {
								msg = receiveN(1);
							}

							ResponseStackTraceSampleFailure response = (ResponseStackTraceSampleFailure) msg[0];

							assertEquals(112223, response.sampleId());
							assertEquals(taskId, response.executionId());
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
												tdd.getExecutionId(),
												numSamples,
												new FiniteDuration(100, TimeUnit.MILLISECONDS),
												0),
										testActorGateway);

								// Receive the expected message (heartbeat races possible)
								Object[] msg = receiveN(1);
								while (!(msg[0] instanceof ResponseStackTraceSampleSuccess)) {
									msg = receiveN(1);
								}

								ResponseStackTraceSampleSuccess response = (ResponseStackTraceSampleSuccess) msg[0];

								// ---- Verify response ----
								assertEquals(19230, response.sampleId());
								assertEquals(tdd.getExecutionId(), response.executionId());

								List<StackTraceElement[]> traces = response.samples();

								assertEquals("Number of samples", numSamples, traces.size());

								for (StackTraceElement[] trace : traces) {
									// Look for BlockingNoOpInvokable#invoke
									for (StackTraceElement elem : trace) {
										if (elem.getClassName().equals(
												Tasks.BlockingNoOpInvokable.class.getName())) {

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
											tdd.getExecutionId(),
											numSamples,
											new FiniteDuration(100, TimeUnit.MILLISECONDS),
											maxDepth),
									testActorGateway);

							// Receive the expected message (heartbeat races possible)
							Object[] msg = receiveN(1);
							while (!(msg[0] instanceof ResponseStackTraceSampleSuccess)) {
								msg = receiveN(1);
							}

							ResponseStackTraceSampleSuccess response = (ResponseStackTraceSampleSuccess) msg[0];

							// ---- Verify response ----
							assertEquals(1337, response.sampleId());
							assertEquals(tdd.getExecutionId(), response.executionId());

							List<StackTraceElement[]> traces = response.samples();

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
								taskManager.tell(new TriggerStackTraceSample(
												44,
												tdd.getExecutionId(),
												Integer.MAX_VALUE,
												new FiniteDuration(10, TimeUnit.MILLISECONDS),
												0),
										testActorGateway);

								Thread.sleep(sleepTime);

								Future<?> removeFuture = taskManager.ask(
										new TestingJobManagerMessages.NotifyWhenJobRemoved(tdd.getJobID()),
										remaining());

								// Cancel the task
								taskManager.tell(new CancelTask(tdd.getExecutionId()));

								// Receive the expected message (heartbeat races possible)
								while (true) {
									Object[] msg = receiveN(1);
									if (msg[0] instanceof ResponseStackTraceSampleSuccess) {
										ResponseStackTraceSampleSuccess response = (ResponseStackTraceSampleSuccess) msg[0];

										assertEquals(tdd.getExecutionId(), response.executionId());
										assertEquals(44, response.sampleId());

										// Done
										return;
									} else if (msg[0] instanceof ResponseStackTraceSampleFailure) {
										// Wait for removal before resubmitting
										Await.ready(removeFuture, remaining());

										Future<?> taskRunningFuture = taskManager.ask(
												new TestingTaskManagerMessages.NotifyWhenTaskIsRunning(
														tdd.getExecutionId()), timeout);

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

	public static class SimpleLookupJobManager extends SimpleJobManager {

		public SimpleLookupJobManager(UUID leaderSessionID) {
			super(leaderSessionID);
		}

		@Override
		public void handleMessage(Object message) throws Exception {
			if (message instanceof ScheduleOrUpdateConsumers) {
				getSender().tell(
						decorateMessage(Messages.getAcknowledge()),
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
			this.validIDs = new HashSet<ExecutionAttemptID>(ids);
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
			if (message instanceof RequestPartitionState) {
				final RequestPartitionState msg = (RequestPartitionState) message;

				PartitionState resp = new PartitionState(
						msg.taskExecutionId(),
						msg.taskResultId(),
						msg.partitionId().getPartitionId(),
						ExecutionState.RUNNING);

				getSender().tell(decorateMessage(resp), getSelf());
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

		@Override
		public void invoke() {}
	}
	
	public static final class TestInvokableBlockingCancelable extends AbstractInvokable {

		@Override
		public void invoke() throws Exception {
			Object o = new Object();
			synchronized (o) {
				o.wait();
			}
		}
	}

}
