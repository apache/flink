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

import java.lang.reflect.Field;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.StoppableTask;
import org.apache.flink.runtime.memory.MemoryManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import scala.concurrent.duration.FiniteDuration;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TaskDeploymentDescriptor.class, JobID.class, FiniteDuration.class })
public class TaskStopTest {
	private Task task;

	public void doMocking(AbstractInvokable taskMock) throws Exception {

		TaskInfo taskInfoMock = mock(TaskInfo.class);
		when(taskInfoMock.getTaskNameWithSubtasks()).thenReturn("dummyName");

		TaskDeploymentDescriptor tddMock = mock(TaskDeploymentDescriptor.class);
		when(tddMock.getTaskInfo()).thenReturn(taskInfoMock);
		when(tddMock.getApplicationID()).thenReturn(new ApplicationID());
		when(tddMock.getJobID()).thenReturn(mock(JobID.class));
		when(tddMock.getVertexID()).thenReturn(mock(JobVertexID.class));
		when(tddMock.getExecutionId()).thenReturn(mock(ExecutionAttemptID.class));
		when(tddMock.getJobConfiguration()).thenReturn(mock(Configuration.class));
		when(tddMock.getTaskConfiguration()).thenReturn(mock(Configuration.class));
		when(tddMock.getInvokableClassName()).thenReturn("className");

		task = new Task(tddMock, mock(MemoryManager.class), mock(IOManager.class), mock(NetworkEnvironment.class),
				mock(BroadcastVariableManager.class), mock(ActorGateway.class), mock(ActorGateway.class),
				mock(FiniteDuration.class), mock(LibraryCacheManager.class), mock(FileCache.class),
				mock(TaskManagerRuntimeInfo.class));
		Field f = task.getClass().getDeclaredField("invokable");
		f.setAccessible(true);
		f.set(task, taskMock);

		Field f2 = task.getClass().getDeclaredField("executionState");
		f2.setAccessible(true);
		f2.set(task, ExecutionState.RUNNING);
	}

	@Test(timeout = 10000)
	public void testStopExecution() throws Exception {
		StoppableTestTask taskMock = new StoppableTestTask();
		doMocking(taskMock);

		task.stopExecution();

		while (!taskMock.stopCalled) {
			Thread.sleep(100);
		}
	}

	@Test(expected = RuntimeException.class)
	public void testStopExecutionFail() throws Exception {
		AbstractInvokable taskMock = mock(AbstractInvokable.class);
		doMocking(taskMock);

		task.stopExecution();
	}

	private final static class StoppableTestTask extends AbstractInvokable implements StoppableTask {
		public volatile boolean stopCalled = false;

		@Override
		public void invoke() throws Exception {
		}

		@Override
		public void stop() {
			this.stopCalled = true;
		}
	}

}
