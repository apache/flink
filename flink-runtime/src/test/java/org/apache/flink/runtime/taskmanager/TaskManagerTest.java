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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.deployment.ChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.GateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.ConnectionInfoLookupResponse;
import org.apache.flink.runtime.io.network.api.RecordReader;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.io.network.bufferprovider.GlobalBufferPool;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.types.IntegerRecord;

import org.junit.Test;

import org.mockito.Matchers;


public class TaskManagerTest {
	
	@Test
	public void testSetupTaskManager() {
		JobManager jobManager = null;
		TaskManager tm = null;
		try {
			jobManager = getJobManagerMockBase();
			tm = createTaskManager(jobManager);

			JobID jid = new JobID();
			JobVertexID vid = new JobVertexID();
			ExecutionAttemptID eid = new ExecutionAttemptID();
			
			TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(jid, vid, eid, "TestTask", 2, 7,
					new Configuration(), new Configuration(), TestInvokableCorrect.class.getName(),
					Collections.<GateDeploymentDescriptor>emptyList(), 
					Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);
			
			TaskOperationResult result = tm.submitTask(tdd);
			assertTrue(result.isSuccess());
			assertEquals(eid, result.getExecutionId());

			jobManager.shutdown();
			tm.shutdown();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			if(jobManager != null){
				jobManager.shutdown();
			}

			if(tm != null){
				tm.shutdown();
			}
		}
	}
	
	@Test
	public void testJobSubmissionAndCanceling() {
		JobManager jobManager = null;
		TaskManager tm = null;
		try {
			jobManager = getJobManagerMockBase();
			tm = createTaskManager(jobManager);

			JobID jid1 = new JobID();
			JobID jid2 = new JobID();
			
			JobVertexID vid1 = new JobVertexID();
			JobVertexID vid2 = new JobVertexID();
			
			ExecutionAttemptID eid1 = new ExecutionAttemptID();
			ExecutionAttemptID eid2 = new ExecutionAttemptID();
			
			TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid1, vid1, eid1, "TestTask1", 1, 5,
					new Configuration(), new Configuration(), TestInvokableBlockingCancelable.class.getName(),
					Collections.<GateDeploymentDescriptor>emptyList(), 
					Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);
			
			TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid2, vid2, eid2, "TestTask2", 2, 7,
					new Configuration(), new Configuration(), TestInvokableBlockingCancelable.class.getName(),
					Collections.<GateDeploymentDescriptor>emptyList(), 
					Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);

			TaskOperationResult result1 = tm.submitTask(tdd1);
			TaskOperationResult result2 = tm.submitTask(tdd2);
			
			assertTrue(result1.isSuccess());
			assertTrue(result2.isSuccess());
			assertEquals(eid1, result1.getExecutionId());
			assertEquals(eid2, result2.getExecutionId());
			
			Map<ExecutionAttemptID, Task> tasks = tm.getAllRunningTasks();
			assertEquals(2, tasks.size());
			
			Task t1 = tasks.get(eid1);
			Task t2 = tasks.get(eid2);
			assertNotNull(t1);
			assertNotNull(t2);
			
			assertEquals(ExecutionState.RUNNING, t1.getExecutionState());
			assertEquals(ExecutionState.RUNNING, t2.getExecutionState());
			
			// cancel one task
			assertTrue(tm.cancelTask(eid1).isSuccess());
			t1.getEnvironment().getExecutingThread().join();
			assertEquals(ExecutionState.CANCELED, t1.getExecutionState());
			
			tasks = tm.getAllRunningTasks();
			assertEquals(1, tasks.size());
			
			// try to cancel a non existing task
			assertFalse(tm.cancelTask(eid1).isSuccess());
			
			// cancel the second task
			assertTrue(tm.cancelTask(eid2).isSuccess());
			t2.getEnvironment().getExecutingThread().join();
			assertEquals(ExecutionState.CANCELED, t2.getExecutionState());
			
			tasks = tm.getAllRunningTasks();
			assertEquals(0, tasks.size());
			
			assertNetworkResourcesReleased(tm);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			if(jobManager != null){
				jobManager.shutdown();
			}

			if(tm != null){
				tm.shutdown();
			}
		}
	}
	
	@Test
	public void testGateChannelEdgeMismatch() {
		JobManager jobManager = null;
		TaskManager tm = null;
		try {
			jobManager = getJobManagerMockBase();
			tm = createTaskManager(jobManager);

			JobID jid = new JobID();;
			
			JobVertexID vid1 = new JobVertexID();
			JobVertexID vid2 = new JobVertexID();
			
			ExecutionAttemptID eid1 = new ExecutionAttemptID();
			ExecutionAttemptID eid2 = new ExecutionAttemptID();
			
			TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid, vid1, eid1, "Sender", 0, 1,
					new Configuration(), new Configuration(), Sender.class.getName(),
					Collections.<GateDeploymentDescriptor>emptyList(),
					Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);
			
			TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid, vid2, eid2, "Receiver", 2, 7,
					new Configuration(), new Configuration(), Receiver.class.getName(),
					Collections.<GateDeploymentDescriptor>emptyList(),
					Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);

			assertFalse(tm.submitTask(tdd1).isSuccess());
			assertFalse(tm.submitTask(tdd2).isSuccess());
			
			Map<ExecutionAttemptID, Task> tasks = tm.getAllRunningTasks();
			assertEquals(0, tasks.size());
			assertNetworkResourcesReleased(tm);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			if(jobManager != null){
				jobManager.shutdown();
			}

			if(tm != null){
				tm.shutdown();
			}
		}
	}
	
	@Test
	public void testRunJobWithForwardChannel() {
		JobManager jobManager = null;
		TaskManager tm = null;

		try {
			JobID jid = new JobID();
			
			JobVertexID vid1 = new JobVertexID();
			JobVertexID vid2 = new JobVertexID();
			
			ExecutionAttemptID eid1 = new ExecutionAttemptID();
			ExecutionAttemptID eid2 = new ExecutionAttemptID();
			
			ChannelID senderId = new ChannelID();
			ChannelID receiverId = new ChannelID();
			
			jobManager = getJobManagerMockBase();
			when(jobManager.lookupConnectionInfo(Matchers.any(InstanceConnectionInfo.class), Matchers.eq(jid), Matchers.eq(senderId)))
				.thenReturn(ConnectionInfoLookupResponse.createReceiverFoundAndReady(receiverId));
			
			tm = createTaskManager(jobManager);
			
			ChannelDeploymentDescriptor cdd = new ChannelDeploymentDescriptor(senderId, receiverId);
			
			TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid, vid1, eid1, "Sender", 0, 1,
					new Configuration(), new Configuration(), Sender.class.getName(),
					Collections.singletonList(new GateDeploymentDescriptor(Collections.singletonList(cdd))), 
					Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);
			
			TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid, vid2, eid2, "Receiver", 2, 7,
					new Configuration(), new Configuration(), Receiver.class.getName(),
					Collections.<GateDeploymentDescriptor>emptyList(),
					Collections.singletonList(new GateDeploymentDescriptor(Collections.singletonList(cdd))),
					new ArrayList<BlobKey>(), 0);

			// deploy sender before receiver, so the target is online when the sender requests the connection info
			TaskOperationResult result2 = tm.submitTask(tdd2);
			TaskOperationResult result1 = tm.submitTask(tdd1);

			assertTrue(result1.isSuccess());
			assertTrue(result2.isSuccess());
			assertEquals(eid1, result1.getExecutionId());
			assertEquals(eid2, result2.getExecutionId());
			
			Map<ExecutionAttemptID, Task> tasks = tm.getAllRunningTasks();
			
			Task t1 = tasks.get(eid1);
			Task t2 = tasks.get(eid2);
			
			// wait until the tasks are done. rare thread races may cause the tasks to be done before
			// we get to the check, so we need to guard the check
			if (t1 != null) {
				t1.getEnvironment().getExecutingThread().join();
				assertEquals(ExecutionState.FINISHED, t1.getExecutionState());
			}
			if (t2 != null) {
				t2.getEnvironment().getExecutingThread().join();
				assertEquals(ExecutionState.FINISHED, t2.getExecutionState());
			}
			
			tasks = tm.getAllRunningTasks();
			assertEquals(0, tasks.size());
			
			// make sure that the global buffer pool has all buffers back
			assertNetworkResourcesReleased(tm);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			if(jobManager != null){
				jobManager.shutdown();
			}

			if(tm != null){
				tm.shutdown();
			}
		}
	}
	
	@Test
	public void testCancellingDependentAndStateUpdateFails() {
		
		// this tests creates two tasks. the sender sends data, and fails to send the
		// state update back to the job manager
		// the second one blocks to be canceled

		JobManager jobManager = null;
		TaskManager tm = null;

		try {
			JobID jid = new JobID();
			
			JobVertexID vid1 = new JobVertexID();
			JobVertexID vid2 = new JobVertexID();
			
			ExecutionAttemptID eid1 = new ExecutionAttemptID();
			ExecutionAttemptID eid2 = new ExecutionAttemptID();
			
			ChannelID senderId = new ChannelID();
			ChannelID receiverId = new ChannelID();
			
			jobManager = getJobManagerMockBase();
			when(jobManager.updateTaskExecutionState(any(TaskExecutionState.class))).thenReturn(false);
			when(jobManager.lookupConnectionInfo(Matchers.any(InstanceConnectionInfo.class), Matchers.eq(jid), Matchers.eq(senderId)))
				.thenReturn(ConnectionInfoLookupResponse.createReceiverFoundAndReady(receiverId));
			
			tm = createTaskManager(jobManager);
			
			ChannelDeploymentDescriptor cdd = new ChannelDeploymentDescriptor(senderId, receiverId);
			
			TaskDeploymentDescriptor tdd1 = new TaskDeploymentDescriptor(jid, vid1, eid1, "Sender", 0, 1,
					new Configuration(), new Configuration(), Sender.class.getName(),
					Collections.singletonList(new GateDeploymentDescriptor(Collections.singletonList(cdd))), 
					Collections.<GateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);
			
			TaskDeploymentDescriptor tdd2 = new TaskDeploymentDescriptor(jid, vid2, eid2, "Receiver", 2, 7,
					new Configuration(), new Configuration(), ReceiverBlocking.class.getName(),
					Collections.<GateDeploymentDescriptor>emptyList(),
					Collections.singletonList(new GateDeploymentDescriptor(Collections.singletonList(cdd))),
					new ArrayList<BlobKey>(), 0);
			
			// deploy sender before receiver, so the target is online when the sender requests the connection info
			TaskOperationResult result2 = tm.submitTask(tdd2);
			TaskOperationResult result1 = tm.submitTask(tdd1);
			
			assertTrue(result1.isSuccess());
			assertTrue(result2.isSuccess());
			
			Map<ExecutionAttemptID, Task> tasks = tm.getAllRunningTasks();
			
			Task t1 = tasks.get(eid1);
			Task t2 = tasks.get(eid2);
			
			// cancel task 2. task one should either fail, or be done
			tm.cancelTask(eid2);
			
			// wait until the task second task is canceled
			if (t2 != null) {
				t2.getEnvironment().getExecutingThread().join();
			}
			
			if (t1 != null) {
				if (t1.getExecutionState() == ExecutionState.RUNNING) {
					tm.cancelTask(eid1);
				}
				t1.getEnvironment().getExecutingThread().join();
			}
			
			// the task that failed to send the finished state 
			assertEquals(0, tasks.size());
			assertNetworkResourcesReleased(tm);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			if(jobManager != null){
				jobManager.shutdown();
			}

			if(tm != null){
				tm.shutdown();
			}
		}
	}
	
	
	private static void assertNetworkResourcesReleased(TaskManager tm) {
		GlobalBufferPool gbp = tm.getChannelManager().getGlobalBufferPool();
		assertEquals(gbp.numBuffers(), gbp.numAvailableBuffers());
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static JobManager getJobManagerMockBase() throws Exception {
		JobManager jm = mock(JobManager.class);
		
		final InstanceID iid = new InstanceID();
		
		when(jm.registerTaskManager(Matchers.any(InstanceConnectionInfo.class), Matchers.any(HardwareDescription.class), Matchers.anyInt()))
			.thenReturn(iid);
		
		when(jm.sendHeartbeat(iid)).thenReturn(true);
		
		when(jm.updateTaskExecutionState(any(TaskExecutionState.class))).thenReturn(true);
		
		return jm;
	}
	
	public static TaskManager createTaskManager(JobManager jm) throws Exception {
		InetAddress localhost = InetAddress.getLocalHost();
		InetSocketAddress jmMockAddress = new InetSocketAddress(localhost, 55443);
		
		Configuration cfg = new Configuration();
		cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 10);
		GlobalConfiguration.includeConfiguration(cfg);
		
		return new TaskManager(ExecutionMode.LOCAL, jm, jm, jm, jm, jmMockAddress, localhost);
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
