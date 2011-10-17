/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.jobmanager.scheduler.queue;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class checks the functionality of the {@link QueueExecutionListener} class
 * 
 * @author marrus
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({QueueExecutionListener.class, ExecutionVertex.class})
public class QueueExecutionListenerTest {

	@Mock
	private AllocatedResource allocatedResource;
	@Mock
	private ExecutionGroupVertex executionGroupVertex;
	@Mock
	private ExecutionGraph executionGraph;
	@Mock
	private ExecutionVertex executionVertex;
	@Mock
	private QueueScheduler localScheduler;

	private ExecutionVertexID vertexID;
	private JobID jobID;

	@Before
	public void setUp()
	{
		initMocks(this);
		jobID = new JobID();
		vertexID = new ExecutionVertexID();
		
		when(executionVertex.getExecutionGraph()).thenReturn(executionGraph);
		when(executionVertex.getAllocatedResource()).thenReturn(allocatedResource);
		when(executionVertex.getGroupVertex()).thenReturn(executionGroupVertex);
		when(executionGroupVertex.getCurrentNumberOfGroupMembers()).thenReturn(0);

	}
	
	/**
	 * This test checks the correctness of the executionStateChanged() method.
	 */
	@Test
	public void testExecutionStateChangedToRunning() {

		final QueueExecutionListener toTest = new QueueExecutionListener(localScheduler, executionVertex);
		// State change to RUNNING
		when(executionGraph.getJobStatus()).thenReturn(InternalJobStatus.RUNNING);
		ExecutionState newExecutionState = ExecutionState.FINISHING;
		toTest.executionStateChanged(jobID, vertexID, newExecutionState, "");
		verify(localScheduler, times(0)).checkAndReleaseAllocatedResource(executionGraph, allocatedResource);
		verify(localScheduler, times(0)).removeJobFromSchedule(executionGraph);
	}

	@Test
	public void testExecutionStateChangedToFinished() {

		final QueueExecutionListener toTest = new QueueExecutionListener(localScheduler, executionVertex);
		ExecutionState newExecutionState = ExecutionState.FINISHED;
		when(executionGraph.getJobStatus()).thenReturn(InternalJobStatus.FINISHED);
		toTest.executionStateChanged(jobID, vertexID, newExecutionState, "");
		verify(localScheduler).checkAndReleaseAllocatedResource(executionGraph, allocatedResource);
	
	}
	
	/**
	 * This test checks the correctness of the executionStateChanged() method.
	 */
	@Test
	public void testExecutionStateChangedToFailed() {

		final QueueExecutionListener toTest = new QueueExecutionListener(localScheduler, executionVertex);
		// State change to RUNNING
		// execution state changed to fails, vertex should be rescheduled
		ExecutionState newExecutionState = ExecutionState.FAILED;
		when(executionVertex.hasRetriesLeft()).thenReturn(true);
		toTest.executionStateChanged(jobID, vertexID, newExecutionState, "");
		verify(executionVertex).updateExecutionState(ExecutionState.SCHEDULED);

	}

	
}
