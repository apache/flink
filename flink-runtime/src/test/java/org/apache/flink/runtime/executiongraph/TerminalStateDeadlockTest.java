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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.instance.DummyActorGateway;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;

import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Test;

import scala.concurrent.duration.FiniteDuration;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TerminalStateDeadlockTest {
	
	private final Field stateField;
	private final Field resourceField;
	private final Field execGraphStateField;
	private final Field execGraphSchedulerField;
	
	private final SimpleSlot resource;


	public TerminalStateDeadlockTest() {
		try {
			// the reflection fields to access the private fields
			this.stateField = Execution.class.getDeclaredField("state");
			this.stateField.setAccessible(true);

			this.resourceField = Execution.class.getDeclaredField("assignedResource");
			this.resourceField.setAccessible(true);

			this.execGraphStateField = ExecutionGraph.class.getDeclaredField("state");
			this.execGraphStateField.setAccessible(true);

			this.execGraphSchedulerField = ExecutionGraph.class.getDeclaredField("scheduler");
			this.execGraphSchedulerField.setAccessible(true);
			
			// the dummy resource
			InetAddress address = InetAddress.getByName("127.0.0.1");
			InstanceConnectionInfo ci = new InstanceConnectionInfo(address, 12345);
				
			HardwareDescription resources = new HardwareDescription(4, 4000000, 3000000, 2000000);
			Instance instance = new Instance(DummyActorGateway.INSTANCE, ci, new InstanceID(), resources, 4);

			this.resource = instance.allocateSimpleSlot(new JobID());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
			
			// silence the compiler
			throw new RuntimeException();
		}
	}
	
	 
	
	// ------------------------------------------------------------------------
	
	@Test
	public void testProvokeDeadlock() {
		try {
			final JobID jobId = resource.getJobID();
			final JobVertexID vid1 = new JobVertexID();
			final JobVertexID vid2 = new JobVertexID();

			
			final Configuration jobConfig = new Configuration();
			
			final List<JobVertex> vertices;
			{
				JobVertex v1 = new JobVertex("v1", vid1);
				JobVertex v2 = new JobVertex("v2", vid2);
				v1.setParallelism(1);
				v2.setParallelism(1);
				v1.setInvokableClass(DummyInvokable.class);
				v2.setInvokableClass(DummyInvokable.class);
				vertices = Arrays.asList(v1, v2);
			}
			
			final Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
			
			final Executor executor = Executors.newFixedThreadPool(4);
			
			// try a lot!
			for (int i = 0; i < 20000; i++) {
				final TestExecGraph eg = new TestExecGraph(jobId);
				eg.attachJobGraph(vertices);

				final Execution e1 = eg.getJobVertex(vid1).getTaskVertices()[0].getCurrentExecutionAttempt();
				final Execution e2 = eg.getJobVertex(vid2).getTaskVertices()[0].getCurrentExecutionAttempt();

				initializeExecution(e1);
				initializeExecution(e2);

				execGraphStateField.set(eg, JobStatus.FAILING);
				execGraphSchedulerField.set(eg, scheduler);
				
				Runnable r1 = new Runnable() {
					@Override
					public void run() {
						e1.cancelingComplete();
					}
				};
				Runnable r2 = new Runnable() {
					@Override
					public void run() {
						e2.cancelingComplete();
					}
				};
				
				executor.execute(r1);
				executor.execute(r2);
				
				eg.waitTillDone();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private void initializeExecution(Execution exec) throws IllegalAccessException {
		// set state to canceling
		stateField.set(exec, ExecutionState.CANCELING);
		
		// assign a resource
		resourceField.set(exec, resource);
	}
	
	
	static class TestExecGraph extends ExecutionGraph {

		private static final long serialVersionUID = -7606144898417942044L;
		
		private static final Configuration EMPTY_CONFIG = new Configuration();

		private static final FiniteDuration TIMEOUT = new FiniteDuration(30, TimeUnit.SECONDS);
		
		private volatile boolean done;

		TestExecGraph(JobID jobId) {
			super(
				TestingUtils.defaultExecutionContext(),
				jobId,
				"test graph",
				EMPTY_CONFIG,
				new ExecutionConfig(),
				TIMEOUT,
				new FixedDelayRestartStrategy(1, 0));
		}

		@Override
		public void scheduleForExecution(Scheduler scheduler) {
			// notify that we are done with the "restarting"
			synchronized (this) {
				done = true;
				this.notifyAll();
			}
		}
		
		public void waitTillDone() {
			try {
				synchronized (this) {
					while (!done) {
						this.wait();
					}
				}
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
