/**
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

package org.apache.flink.runtime.executiongraph;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.execution.ExecutionState2;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.protocols.TaskOperationProtocol;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ExecutionGraphTestUtils {

	// --------------------------------------------------------------------------------------------
	//  state modifications
	// --------------------------------------------------------------------------------------------
	
	public static void setVertexState(ExecutionVertex2 vertex, ExecutionState2 state) {
		try {
			Field f = ExecutionVertex2.class.getDeclaredField("state");
			f.setAccessible(true);
			f.set(vertex, state);
		}
		catch (Exception e) {
			throw new RuntimeException("Modifying the state failed", e);
		}
	}
	
	public static void setVertexResource(ExecutionVertex2 vertex, AllocatedSlot slot) {
		try {
			Field f = ExecutionVertex2.class.getDeclaredField("assignedSlot");
			f.setAccessible(true);
			f.set(vertex, slot);
		}
		catch (Exception e) {
			throw new RuntimeException("Modifying the slot failed", e);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  utility mocking methods
	// --------------------------------------------------------------------------------------------
	
	public static Instance getInstance(final TaskOperationProtocol top) throws Exception {
		HardwareDescription hardwareDescription = new HardwareDescription(4, 2L*1024*1024*1024, 1024*1024*1024, 512*1024*1024);
		InetAddress address = InetAddress.getByName("127.0.0.1");
		InstanceConnectionInfo connection = new InstanceConnectionInfo(address, 10000, 10001);
		
		return new Instance(connection, new InstanceID(), hardwareDescription, 1) {
			@Override
			public TaskOperationProtocol getTaskManagerProxy() {
				return top;
			}
		};
	}
	
	public static ExecutionJobVertex getJobVertexNotExecuting(JobVertexID id) throws JobException {
		ExecutionJobVertex ejv = getJobVertexBase(id);
		
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) {
				return null;
			}
		}).when(ejv).execute(Matchers.any(Runnable.class));
		
		return ejv;
	}
	
	public static ExecutionJobVertex getJobVertexExecutingSynchronously(JobVertexID id) throws JobException {
		ExecutionJobVertex ejv = getJobVertexBase(id);
		
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) {
				Runnable r = (Runnable) invocation.getArguments()[0];
				r.run();
				return null;
			}
		}).when(ejv).execute(Matchers.any(Runnable.class));
		
		return ejv;
	}
	
	public static ExecutionJobVertex getJobVertexExecutingAsynchronously(JobVertexID id) throws JobException {
		ExecutionJobVertex ejv = getJobVertexBase(id);
		
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) {
				Runnable r = (Runnable) invocation.getArguments()[0];
				new Thread(r).start();
				return null;
			}
		}).when(ejv).execute(Matchers.any(Runnable.class));
		
		return ejv;
	}
	
	public static ExecutionJobVertex getJobVertexExecutingTriggered(JobVertexID id, final ActionQueue queue) throws JobException {
		ExecutionJobVertex ejv = getJobVertexBase(id);
		
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) {
				
				final Runnable action = (Runnable) invocation.getArguments()[0];
				queue.queueAction(action);
				return null;
			}
		}).when(ejv).execute(Matchers.any(Runnable.class));
		
		return ejv;
	}
	
	private static ExecutionJobVertex getJobVertexBase(JobVertexID id) throws JobException {
		AbstractJobVertex ajv = new AbstractJobVertex("TestVertex", id);
		ajv.setInvokableClass(mock(AbstractInvokable.class).getClass());
		
		ExecutionGraph graph = new ExecutionGraph(new JobID(), "test job", new Configuration());
		
		return spy(new ExecutionJobVertex(graph, ajv, 1));
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class ActionQueue {
		
		private final LinkedBlockingQueue<Runnable> runnables = new LinkedBlockingQueue<Runnable>();
		
		public void triggerNextAction() {
			Runnable r = runnables.remove();
			r.run();
		}
		
		public Runnable popNextAction() {
			Runnable r = runnables.remove();
			return r;
		}

		public void queueAction(Runnable r) {
			this.runnables.add(r);
		}
	}
}
