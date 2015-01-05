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
import akka.actor.UntypedActor;
import akka.japi.Creator;
import akka.pattern.Patterns;
import akka.testkit.JavaTestKit;
import akka.util.Timeout;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.deployment.ChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.GateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.ConnectionInfoLookupResponse;
import org.apache.flink.runtime.io.network.api.RecordReader;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.RegistrationMessages;
import org.apache.flink.runtime.messages.TaskManagerMessages.CancelTask;
import org.apache.flink.runtime.messages.TaskManagerMessages.SubmitTask;
import org.apache.flink.runtime.messages.TaskManagerMessages.TaskOperationResult;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.types.IntegerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


public class TaskManagerTest {

	private static ActorSystem system;

	private static Timeout timeout = new Timeout(1, TimeUnit.MINUTES);

	@BeforeClass
	public static void setup(){
		system = ActorSystem.create("TestActorSystem", TestingUtils.testConfig());
	}

	@AfterClass
	public static void teardown(){
		JavaTestKit.shutdownActorSystem(system);
		system = null;
	}
	
	@Test
	public void testSetupTaskManager() {
		new JavaTestKit(system){{
			try {
				ActorRef jobManager = system.actorOf(Props.create(SimpleJobManager.class));

				final ActorRef tm = createTaskManager(jobManager);

				JobID jid = new JobID();
				JobVertexID vid = new JobVertexID();
				final ExecutionAttemptID eid = new ExecutionAttemptID();

				final TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(jid, vid, eid, "TestTask", 2, 7,
						new Configuration(), new Configuration(), TestInvokableCorrect.class.getName(),
						Collections.<GateDeploymentDescriptor>emptyList(),
						Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);

				new Within(duration("1 seconds")){

					@Override
					protected void run() {
						tm.tell(new SubmitTask(tdd), getRef());
						expectMsgEquals(new TaskOperationResult(eid, true));
					}
				};
			}
			catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}};
	}
	
	@Test
	public void testJobSubmissionAndCanceling() {
		new JavaTestKit(system){{
			try {

				ActorRef jobManager = system.actorOf(Props.create(SimpleJobManager.class));
				final ActorRef tm = createTaskManager(jobManager);

				final JobID jid1 = new JobID();
				final JobID jid2 = new JobID();

				JobVertexID vid1 = new JobVertexID();
				JobVertexID vid2 = new JobVertexID();

				final ExecutionAttemptID eid1 = new ExecutionAttemptID();
				final ExecutionAttemptID eid2 = new ExecutionAttemptID();

				final TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid1, vid1, eid1, "TestTask1", 1, 5,
						new Configuration(), new Configuration(), TestInvokableBlockingCancelable.class.getName(),
						Collections.<GateDeploymentDescriptor>emptyList(),
						Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);

				final TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid2, vid2, eid2, "TestTask2", 2, 7,
						new Configuration(), new Configuration(), TestInvokableBlockingCancelable.class.getName(),
						Collections.<GateDeploymentDescriptor>emptyList(),
						Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);

				final FiniteDuration d = duration("1 second");

				new Within(d) {

					@Override
					protected void run() {
						try {
							tm.tell(new SubmitTask(tdd1), getRef());
							tm.tell(new SubmitTask(tdd2), getRef());

							expectMsgEquals(new TaskOperationResult(eid1, true));
							expectMsgEquals(new TaskOperationResult(eid2, true));
							
							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), getRef());

							Map<ExecutionAttemptID, Task> runningTasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(2, runningTasks.size());
							Task t1 = runningTasks.get(eid1);
							Task t2 = runningTasks.get(eid2);
							assertNotNull(t1);
							assertNotNull(t2);

							assertEquals(ExecutionState.RUNNING, t1.getExecutionState());
							assertEquals(ExecutionState.RUNNING, t2.getExecutionState());

							tm.tell(new CancelTask(eid1), getRef());

							expectMsgEquals(new TaskOperationResult(eid1, true));

							Future<Object> response = Patterns.ask(tm, new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid1),
									timeout);
							Await.ready(response, d);

							assertEquals(ExecutionState.CANCELED, t1.getExecutionState());

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), getRef());
							runningTasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(1, runningTasks.size());

							tm.tell(new CancelTask(eid1), getRef());
							expectMsgEquals(new TaskOperationResult(eid1, false, "No task with that execution ID was " +
									"found."));

							tm.tell(new CancelTask(eid2), getRef());
							expectMsgEquals(new TaskOperationResult(eid2, true));

							response = Patterns.ask(tm, new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid2),
									timeout);
							Await.ready(response, d);

							assertEquals(ExecutionState.CANCELED, t2.getExecutionState());

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), getRef());
							runningTasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(0, runningTasks.size());
						} catch (Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};
			}catch(Exception e){
				e.printStackTrace();
				fail(e.getMessage());
			}

		}};
	}
	
	@Test
	public void testGateChannelEdgeMismatch() {
		new JavaTestKit(system){{
			try {
				ActorRef jm = system.actorOf(Props.create(SimpleJobManager.class));

				final ActorRef tm = createTaskManager(jm);

				final JobID jid = new JobID();

				JobVertexID vid1 = new JobVertexID();
				JobVertexID vid2 = new JobVertexID();

				final ExecutionAttemptID eid1 = new ExecutionAttemptID();
				final ExecutionAttemptID eid2 = new ExecutionAttemptID();

				final TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid, vid1, eid1, "Sender", 0, 1,
						new Configuration(), new Configuration(), Sender.class.getName(),
						Collections.<GateDeploymentDescriptor>emptyList(),
						Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);

				final TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid, vid2, eid2, "Receiver", 2, 7,
						new Configuration(), new Configuration(), Receiver.class.getName(),
						Collections.<GateDeploymentDescriptor>emptyList(),
						Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);

				new Within(duration("1 second")){

					@Override
					protected void run() {
						try {
							tm.tell(new SubmitTask(tdd1), getRef());
							tm.tell(new SubmitTask(tdd2), getRef());
							TaskOperationResult result = expectMsgClass(TaskOperationResult.class);
							assertFalse(result.success());
							assertEquals(eid1, result.executionID());

							result = expectMsgClass(TaskOperationResult.class);
							assertFalse(result.success());
							assertEquals(eid2, result.executionID());

							tm.tell(new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid1),
									getRef());
							tm.tell(new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid2),
									getRef());

							expectMsgEquals(true);
							expectMsgEquals(true);

							tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), getRef());
							Map<ExecutionAttemptID, Task> tasks = expectMsgClass(TestingTaskManagerMessages
									.ResponseRunningTasks.class).asJava();

							assertEquals(0, tasks.size());
						}catch (Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};
			}catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}};
	}
	
	@Test
	public void testRunJobWithForwardChannel() {
		new JavaTestKit(system){{
			final JobID jid = new JobID();

			JobVertexID vid1 = new JobVertexID();
			JobVertexID vid2 = new JobVertexID();

			final ExecutionAttemptID eid1 = new ExecutionAttemptID();
			final ExecutionAttemptID eid2 = new ExecutionAttemptID();

			final ChannelID senderId = new ChannelID();
			final ChannelID receiverId = new ChannelID();

			ActorRef jm = system.actorOf(Props.create(new SimpleLookupJobManagerCreator(receiverId)));
			final ActorRef tm = createTaskManager(jm);

			ChannelDeploymentDescriptor cdd = new ChannelDeploymentDescriptor(senderId, receiverId);

			final TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid, vid1, eid1, "Sender", 0, 1,
					new Configuration(), new Configuration(), Sender.class.getName(),
					Collections.singletonList(new GateDeploymentDescriptor(Collections.singletonList(cdd))),
					Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);

			final TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid, vid2, eid2, "Receiver", 2, 7,
					new Configuration(), new Configuration(), Receiver.class.getName(),
					Collections.<GateDeploymentDescriptor>emptyList(),
					Collections.singletonList(new GateDeploymentDescriptor(Collections.singletonList(cdd))),
					new ArrayList<BlobKey>(), 0);

			final FiniteDuration d = duration("1 second");

			new Within(d){

				@Override
				protected void run() {
					try {
						tm.tell(new SubmitTask(tdd2), getRef());
						expectMsgEquals(new TaskOperationResult(eid2, true));
						tm.tell(new SubmitTask(tdd1), getRef());
						expectMsgEquals(new TaskOperationResult(eid1, true));

						tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), getRef());
						Map<ExecutionAttemptID, Task> tasks = expectMsgClass(TestingTaskManagerMessages.ResponseRunningTasks
								.class).asJava();

						Task t1 = tasks.get(eid1);
						Task t2 = tasks.get(eid2);

			// wait until the tasks are done. rare thread races may cause the tasks to be done before
			// we get to the check, so we need to guard the check
						if (t1 != null) {
							Future<Object> response = Patterns.ask(tm, new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid1),
									timeout);
							Await.ready(response, d);
						}

						if (t2 != null) {
							Future<Object> response = Patterns.ask(tm, new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid2),
									timeout);
							Await.ready(response, d);
				assertEquals(ExecutionState.FINISHED, t2.getExecutionState());
			}

						tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), getRef());
						tasks = expectMsgClass(TestingTaskManagerMessages.ResponseRunningTasks
								.class).asJava();

						assertEquals(0, tasks.size());
					}catch (Exception e) {
						e.printStackTrace();
						fail(e.getMessage());

					}
				}
			};
		}};
	}
	
	@Test
	public void testCancellingDependentAndStateUpdateFails() {
		
		// this tests creates two tasks. the sender sends data, and fails to send the
		// state update back to the job manager
		// the second one blocks to be canceled

		new JavaTestKit(system){{
			final JobID jid = new JobID();

			JobVertexID vid1 = new JobVertexID();
			JobVertexID vid2 = new JobVertexID();

			final ExecutionAttemptID eid1 = new ExecutionAttemptID();
			final ExecutionAttemptID eid2 = new ExecutionAttemptID();

			final ChannelID senderId = new ChannelID();
			final ChannelID receiverId = new ChannelID();

			ActorRef jm = system.actorOf(Props.create(new SimpleLookupFailingUpdateJobManagerCreator(receiverId)));
			final ActorRef tm = createTaskManager(jm);

			ChannelDeploymentDescriptor cdd = new ChannelDeploymentDescriptor(senderId, receiverId);

			final TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid, vid1, eid1, "Sender", 0, 1,
					new Configuration(), new Configuration(), Sender.class.getName(),
					Collections.singletonList(new GateDeploymentDescriptor(Collections.singletonList(cdd))),
					Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);

			final TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid, vid2, eid2, "Receiver", 2, 7,
					new Configuration(), new Configuration(), ReceiverBlocking.class.getName(),
					Collections.<GateDeploymentDescriptor>emptyList(),
					Collections.singletonList(new GateDeploymentDescriptor(Collections.singletonList(cdd))),
					new ArrayList<BlobKey>(), 0);

			final FiniteDuration d = duration("1 second");

			new Within(d){
			
				@Override
				protected void run() {
					try {
						// deploy sender before receiver, so the target is online when the sender requests the connection info
						tm.tell(new SubmitTask(tdd2), getRef());
						tm.tell(new SubmitTask(tdd1), getRef());

						expectMsgEquals(new TaskOperationResult(eid2, true));
						expectMsgEquals(new TaskOperationResult(eid1, true));

						tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), getRef());
						Map<ExecutionAttemptID, Task> tasks = expectMsgClass(TestingTaskManagerMessages
								.ResponseRunningTasks.class).asJava();

						Task t1 = tasks.get(eid1);
						Task t2 = tasks.get(eid2);

						tm.tell(new CancelTask(eid2), getRef());
						expectMsgEquals(new TaskOperationResult(eid2, true));

						if (t2 != null) {
							Future<Object> response = Patterns.ask(tm, new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid2),
									timeout);
							Await.ready(response, d);
						}

						if (t1 != null) {
							if (t1.getExecutionState() == ExecutionState.RUNNING) {
								tm.tell(new CancelTask(eid1), getRef());
								expectMsgEquals(new TaskOperationResult(eid1, true));
							}
							Future<Object> response = Patterns.ask(tm, new TestingTaskManagerMessages.NotifyWhenTaskRemoved(eid1),
									timeout);
							Await.ready(response, d);
						}

						tm.tell(TestingTaskManagerMessages.getRequestRunningTasksMessage(), getRef());
						tasks = expectMsgClass(TestingTaskManagerMessages
								.ResponseRunningTasks.class).asJava();

						assertEquals(0, tasks.size());
					}catch(Exception e){
						e.printStackTrace();
						fail(e.getMessage());
					}
				}
			};
		}};
	}

	
	// --------------------------------------------------------------------------------------------

	public static class SimpleJobManager extends UntypedActor{

		@Override
		public void onReceive(Object message) throws Exception {
			if(message instanceof RegistrationMessages.RegisterTaskManager){
				final InstanceID iid = new InstanceID();
				getSender().tell(new RegistrationMessages.AcknowledgeRegistration(iid, -1),
						getSelf());
			}else if(message instanceof JobManagerMessages.UpdateTaskExecutionState){
				getSender().tell(true, getSelf());
			}
		}
	}

	public static class SimpleLookupJobManager extends SimpleJobManager{
		private final ChannelID receiverID;

		public SimpleLookupJobManager(ChannelID receiverID){
			this.receiverID = receiverID;
		}
		@Override
		public void onReceive(Object message) throws Exception {
			if(message instanceof JobManagerMessages.LookupConnectionInformation){
				getSender().tell(new JobManagerMessages.ConnectionInformation(ConnectionInfoLookupResponse
						.createReceiverFoundAndReady(receiverID)), getSelf());
			}else{
				super.onReceive(message);
			}
		}
	}

	public static class SimpleLookupFailingUpdateJobManager extends SimpleLookupJobManager{

		public SimpleLookupFailingUpdateJobManager(ChannelID receiverID) {
			super(receiverID);
		}

		@Override
		public void onReceive(Object message) throws Exception{
			if(message instanceof JobManagerMessages.UpdateTaskExecutionState){
				getSender().tell(false, getSelf());
			}else{
				super.onReceive(message);
			}
		}
	}

	@SuppressWarnings("serial")
	public static class SimpleLookupJobManagerCreator implements Creator<SimpleLookupJobManager>{
		private final ChannelID receiverID;

		public SimpleLookupJobManagerCreator(ChannelID receiverID){
			this.receiverID = receiverID;
		}

		@Override
		public SimpleLookupJobManager create() throws Exception {
			return new SimpleLookupJobManager(receiverID);
		}
	}

	@SuppressWarnings("serial")
	public static class SimpleLookupFailingUpdateJobManagerCreator implements
			Creator<SimpleLookupFailingUpdateJobManager>{
		private final ChannelID receiverID;

		public SimpleLookupFailingUpdateJobManagerCreator(ChannelID receiverID){
			this.receiverID = receiverID;
		}

		@Override
		public SimpleLookupFailingUpdateJobManager create() throws Exception {
			return new SimpleLookupFailingUpdateJobManager(receiverID);
		}
	}

	public static ActorRef createTaskManager(ActorRef jm) {
		Configuration cfg = new Configuration();
		cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 10);
		GlobalConfiguration.includeConfiguration(cfg);
		String akkaURL = jm.path().toString();
		cfg.setString(ConfigConstants.JOB_MANAGER_AKKA_URL, akkaURL);

		ActorRef taskManager = TestingUtils.startTestingTaskManagerWithConfiguration("localhost", cfg, system);

		Future<Object> response = Patterns.ask(taskManager, 
				TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(), timeout);

		try {
			FiniteDuration d = new FiniteDuration(20, TimeUnit.SECONDS);
			Await.ready(response, d);
		}catch(Exception e){
			throw new RuntimeException("Exception while waiting for the task manager registration.", e);
		}

		return taskManager;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class TestInvokableCorrect extends AbstractInvokable {

		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() {}
	}
	
	public static final class TestInvokableBlockingCancelable extends AbstractInvokable {

		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() throws Exception {
			Object o = new Object();
			synchronized (o) {
				o.wait();
			}
		}
	}
	
	public static final class Sender extends AbstractInvokable {

		private RecordWriter<IntegerRecord> writer;
		
		@Override
		public void registerInputOutput() {
			writer = new RecordWriter<IntegerRecord>(this);
		}

		@Override
		public void invoke() throws Exception {
			writer.initializeSerializers();
			writer.emit(new IntegerRecord(42));
			writer.emit(new IntegerRecord(1337));
			writer.flush();
		}
	}
	
	public static final class Receiver extends AbstractInvokable {

		private RecordReader<IntegerRecord> reader;
		
		@Override
		public void registerInputOutput() {
			reader = new RecordReader<IntegerRecord>(this, IntegerRecord.class);
		}

		@Override
		public void invoke() throws Exception {
			IntegerRecord i1 = reader.next();
			IntegerRecord i2 = reader.next();
			IntegerRecord i3 = reader.next();
			
			if (i1.getValue() != 42 || i2.getValue() != 1337 || i3 != null) {
				throw new Exception("Wrong Data Received");
			}
		}
	}
	
	public static final class ReceiverBlocking extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
			new RecordReader<IntegerRecord>(this, IntegerRecord.class);
		}

		@Override
		public void invoke() throws Exception {
			synchronized(this) {
				wait();
			}
		}
	}
}
