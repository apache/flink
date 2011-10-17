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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertexIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceRequestMap;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.local.LocalInstance;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.DeploymentManager;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingException;

/**
 * @author marrus
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *         This class checks the functionality of the {@link QueueScheduler} class
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({QueueScheduler.class, ExecutionStage.class, ExecutionVertex.class})
@SuppressStaticInitializationFor("eu.stratosphere.nephele.jobmanager.scheduler.queue.QueueScheduler")
public class QueueSchedulerTest {

	@Mock
	private ExecutionGraph executionGraph;

	@Mock
	private ExecutionStage stage1;
	
	@Mock
	private ExecutionVertex vertex1;

	@Mock
	private ExecutionGraphIterator graphIterator;

	@Mock
	private ExecutionGraphIterator graphIterator2;
	
	@Mock
	private ExecutionGroupVertexIterator groupVertexIterator;

	@Mock
	private ExecutionGroupVertex groupVertex; 
	
	@Mock
	private InstanceManager instanceManager;

	@Mock
	private Log loggerMock;

	private InstanceRequestMap requiredInstanceTypes;
	private HashMap<InstanceType, InstanceTypeDescription> availableInstances;
	private DeploymentManager deploymentManager;

	private InstanceTypeDescription desc;

	private InstanceType type;

	/**
	 * Setting up the mocks and necessary internal states
	 */
	@Before
	public void before() {
		MockitoAnnotations.initMocks(this);
		Whitebox.setInternalState(QueueScheduler.class, this.loggerMock);
		
		type = new InstanceType();
		desc = InstanceTypeDescriptionFactory.construct(type, new HardwareDescription(), 4);
		requiredInstanceTypes = new InstanceRequestMap();
		requiredInstanceTypes.setNumberOfInstances(type, 3);
		availableInstances = new HashMap<InstanceType, InstanceTypeDescription>();
		availableInstances.put(type, desc);
		deploymentManager = new TestDeploymentManager();

		try {
			whenNew(InstanceRequestMap.class).withNoArguments().thenReturn(requiredInstanceTypes);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * Checks the behavior of the scheduleJob() method
	 */
	@Test
	public void testSchedulJob() {

		when(this.executionGraph.getNumberOfStages()).thenReturn(1);
		when(this.executionGraph.getStage(0)).thenReturn(this.stage1);
		when(this.executionGraph.getCurrentExecutionStage()).thenReturn(this.stage1);
		when(this.instanceManager.getMapOfAvailableInstanceTypes()).thenReturn(availableInstances);
		when(this.stage1.getExecutionGraph()).thenReturn(this.executionGraph);

		// correct walk through method
		final QueueScheduler toTest = new QueueScheduler(deploymentManager, this.instanceManager);
		try {
			toTest.schedulJob(this.executionGraph);
			final Deque<ExecutionGraph> jobQueue = Whitebox.getInternalState(toTest, "jobQueue");
			assertEquals("Job should be in list", true, jobQueue.contains(this.executionGraph));
			jobQueue.remove(this.executionGraph);

		} catch (SchedulingException e) {
			fail();
			e.printStackTrace();
		}

	}
	
	/**
	 * Checks the behavior of the scheduleJob() method when not resources are available
	 */
	@Test
	public void testScheduleJobWithInsufficientResources() {

		when(this.executionGraph.getNumberOfStages()).thenReturn(1);
		when(this.executionGraph.getStage(0)).thenReturn(this.stage1);
		when(this.executionGraph.getCurrentExecutionStage()).thenReturn(this.stage1);
		when(this.instanceManager.getMapOfAvailableInstanceTypes()).thenReturn(availableInstances);
		when(this.stage1.getExecutionGraph()).thenReturn(this.executionGraph);
		
		// not enough available Instances
		final QueueScheduler toTest = new QueueScheduler(deploymentManager, this.instanceManager);
		desc = InstanceTypeDescriptionFactory.construct(type, new HardwareDescription(), 2);
		availableInstances.put(type, desc);
		try {
			toTest.schedulJob(this.executionGraph);
			fail();

		} catch (SchedulingException e) {
			final Deque<ExecutionGraph> jobQueue = Whitebox.getInternalState(toTest, "jobQueue");
			assertEquals("Job should not be in list", false, jobQueue.contains(this.executionGraph));

		}
	}
	
	
	/**
	 * Checks the behavior of the scheduleJob() method when the instance is unknown
	 */
	@Test
	public void testScheduleJobWithUnknownInstance() {

		when(this.executionGraph.getNumberOfStages()).thenReturn(1);
		when(this.executionGraph.getStage(0)).thenReturn(this.stage1);
		when(this.executionGraph.getCurrentExecutionStage()).thenReturn(this.stage1);
		when(this.instanceManager.getMapOfAvailableInstanceTypes()).thenReturn(availableInstances);
		when(this.stage1.getExecutionGraph()).thenReturn(this.executionGraph);

		final QueueScheduler toTest = new QueueScheduler(deploymentManager, this.instanceManager);
		
		// Instance unknown
		availableInstances.clear();
		try {
			toTest.schedulJob(this.executionGraph);
			fail();
		} catch (SchedulingException e) {
			Deque<ExecutionGraph> jobQueue = Whitebox.getInternalState(toTest, "jobQueue");
			assertEquals("Job should not be in list", false, jobQueue.contains(this.executionGraph));

		}
	}


	/**
	 * Checks the behavior of the resourceAllocated() method
	 * 
	 * @throws Exception
	 * TODO: refactored into 4 tests with less verifications each. Check tests separately for semantical correctness and delete this test.
	 * 
	 */
//	@Test
//	public void testResourceAllocated() throws Exception {
//
//		final DeploymentManager deploymentManager = new TestDeploymentManager();
//
//		final QueueScheduler toTest = spy(new QueueScheduler(deploymentManager, this.instanceManager));
//		final JobID jobid = new JobID();
//		final AllocatedResource resource = mock(AllocatedResource.class);
//		final List<AllocatedResource> resources = new ArrayList<AllocatedResource>();
//		resources.add(resource);
//		final InstanceType instanceType = new InstanceType();
//		InstanceConnectionInfo instanceConnectionInfo = mock(InstanceConnectionInfo.class);
//		when(instanceConnectionInfo.toString()).thenReturn("");
//		LocalInstance instance = spy(new LocalInstance(instanceType, instanceConnectionInfo, null, null, null));
//
//		// given resource is null
//		toTest.resourcesAllocated(null, null);
//		verify(this.loggerMock).error(Matchers.anyString());
//
//		// jobs have have been canceled
//		final Method methodToMock = MemberMatcher.method(QueueScheduler.class, JobID.class);
//		PowerMockito.when(toTest, methodToMock).withArguments(Matchers.any(JobID.class)).thenReturn(null);
//		when(resource.getInstance()).thenReturn(instance);
//
//		toTest.resourcesAllocated(jobid, resources);
//		try {
//			verify(this.instanceManager).releaseAllocatedResource(Matchers.any(JobID.class),
//				Matchers.any(Configuration.class), Matchers.any(AllocatedResource.class));
//		} catch (InstanceException e1) {
//			e1.printStackTrace();
//		}
//
//		// vertex resource is null
//		PowerMockito.when(toTest, methodToMock).withArguments(Matchers.any(JobID.class))
//			.thenReturn(this.executionGraph);
//		when(this.graphIterator.next()).thenReturn(this.vertex1);
//		when(this.graphIterator.hasNext()).thenReturn(true, true, true, true, false);
//		when(this.graphIterator2.next()).thenReturn(this.vertex1);
//		when(this.graphIterator2.hasNext()).thenReturn(true, true, true, true, false);
//		when(this.vertex1.getExecutionState()).thenReturn(ExecutionState.SCHEDULED);
//		try {
//			whenNew(ExecutionGraphIterator.class).withArguments(Matchers.any(ExecutionGraph.class),
//				Matchers.anyBoolean()).thenReturn(this.graphIterator);
//			whenNew(ExecutionGraphIterator.class).withArguments(Matchers.any(ExecutionGraph.class), Matchers.anyInt(),
//				Matchers.anyBoolean(), Matchers.anyBoolean()).thenReturn(this.graphIterator2);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//		when(this.executionGraph.getJobID()).thenReturn(jobid);
//		Deque<ExecutionGraph> jobQueue = Whitebox.getInternalState(toTest, "jobQueue");
//		jobQueue.add(this.executionGraph);
//		Whitebox.setInternalState(toTest, "jobQueue", jobQueue);
//		when(this.vertex1.getAllocatedResource()).thenReturn(null);
//		when(resource.getInstance()).thenReturn(instance);
//
//		toTest.resourcesAllocated(jobid, resources);
//		verify(this.loggerMock, times(2)).error(Matchers.anyString());
//
//		// correct walk through method
//		when(this.graphIterator2.hasNext()).thenReturn(true, true, true, true, false);
//		when(this.graphIterator.hasNext()).thenReturn(true, true, true, true, false);
//		when(this.vertex1.getAllocatedResource()).thenReturn(resource);
//		when(resource.getInstanceType()).thenReturn(instanceType);
//
//		toTest.resourcesAllocated(jobid, resources);
//		verify(this.vertex1, times(4)).updateExecutionState(ExecutionState.READY);
//
//	}

	@Test
	public void testResourceAllocatedWithNullResource() throws Exception {

		final DeploymentManager deploymentManager = new TestDeploymentManager();

		final QueueScheduler toTest = spy(new QueueScheduler(deploymentManager, this.instanceManager));
		final JobID jobid = new JobID();
		final AllocatedResource resource = mock(AllocatedResource.class);
		final List<AllocatedResource> resources = new ArrayList<AllocatedResource>();
		resources.add(resource);
		final InstanceType instanceType = new InstanceType();
		InstanceConnectionInfo instanceConnectionInfo = mock(InstanceConnectionInfo.class);
		when(instanceConnectionInfo.toString()).thenReturn("");
		LocalInstance instance = spy(new LocalInstance(instanceType, instanceConnectionInfo, null, null, null));

		// given resource is null
		toTest.resourcesAllocated(null, null);
		verify(this.loggerMock).error(Matchers.anyString());

	}
	
	@Test
	public void testResourceAllocatedHavingCancelledJobs() throws Exception {

		final DeploymentManager deploymentManager = new TestDeploymentManager();

		final QueueScheduler toTest = spy(new QueueScheduler(deploymentManager, this.instanceManager));
		final JobID jobid = new JobID();
		final AllocatedResource resource = mock(AllocatedResource.class);
		final List<AllocatedResource> resources = new ArrayList<AllocatedResource>();
		resources.add(resource);
		final InstanceType instanceType = new InstanceType();
		InstanceConnectionInfo instanceConnectionInfo = mock(InstanceConnectionInfo.class);
		when(instanceConnectionInfo.toString()).thenReturn("");
		LocalInstance instance = spy(new LocalInstance(instanceType, instanceConnectionInfo, null, null, null));

		// jobs have have been canceled
		final Method methodToMock = MemberMatcher.method(QueueScheduler.class, JobID.class);
		PowerMockito.when(toTest, methodToMock).withArguments(Matchers.any(JobID.class)).thenReturn(null);
		when(resource.getInstance()).thenReturn(instance);

		toTest.resourcesAllocated(jobid, resources);
		try {
			verify(this.instanceManager).releaseAllocatedResource(Matchers.any(JobID.class),
				Matchers.any(Configuration.class), Matchers.any(AllocatedResource.class));
		} catch (InstanceException e1) {
			e1.printStackTrace();
		}

	}
	
	@Test
	public void testResourceAllocatedWithNullVertexResource() throws Exception {

		final DeploymentManager deploymentManager = new TestDeploymentManager();

		final QueueScheduler toTest = spy(new QueueScheduler(deploymentManager, this.instanceManager));
		final JobID jobid = new JobID();
		final AllocatedResource resource = mock(AllocatedResource.class);
		final List<AllocatedResource> resources = new ArrayList<AllocatedResource>();
		resources.add(resource);
		final InstanceType instanceType = new InstanceType();
		InstanceConnectionInfo instanceConnectionInfo = mock(InstanceConnectionInfo.class);
		when(instanceConnectionInfo.toString()).thenReturn("");
		LocalInstance instance = spy(new LocalInstance(instanceType, instanceConnectionInfo, null, null, null));

		// jobs have have been canceled
		final Method methodToMock = MemberMatcher.method(QueueScheduler.class, JobID.class);
		PowerMockito.when(toTest, methodToMock).withArguments(Matchers.any(JobID.class)).thenReturn(null);
		when(resource.getInstance()).thenReturn(instance);

		// vertex resource is null
		PowerMockito.when(toTest, methodToMock).withArguments(Matchers.any(JobID.class))
			.thenReturn(this.executionGraph);
		when(this.graphIterator.next()).thenReturn(this.vertex1);
		when(this.graphIterator.hasNext()).thenReturn(true, true, true, true, false);
		when(this.graphIterator2.next()).thenReturn(this.vertex1);
		when(this.graphIterator2.hasNext()).thenReturn(true, true, true, true, false);
		when(this.vertex1.getExecutionState()).thenReturn(ExecutionState.SCHEDULED);
		try {
			whenNew(ExecutionGraphIterator.class).withArguments(Matchers.any(ExecutionGraph.class),
				Matchers.anyBoolean()).thenReturn(this.graphIterator);
			whenNew(ExecutionGraphIterator.class).withArguments(Matchers.any(ExecutionGraph.class), Matchers.anyInt(),
				Matchers.anyBoolean(), Matchers.anyBoolean()).thenReturn(this.graphIterator2);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		when(this.executionGraph.getJobID()).thenReturn(jobid);
		Deque<ExecutionGraph> jobQueue = Whitebox.getInternalState(toTest, "jobQueue");
		jobQueue.add(this.executionGraph);
		Whitebox.setInternalState(toTest, "jobQueue", jobQueue);
		when(this.vertex1.getAllocatedResource()).thenReturn(null);
		when(resource.getInstance()).thenReturn(instance);

		toTest.resourcesAllocated(jobid, resources);
		verify(this.instanceManager).releaseAllocatedResource(Matchers.any(JobID.class),
				Matchers.any(Configuration.class), Matchers.any(AllocatedResource.class));
		verify(this.loggerMock).error(Matchers.anyString());

	}
	
	@Test
	public void testNormalResourceAllocated() throws Exception {

		final DeploymentManager deploymentManager = new TestDeploymentManager();
		final InstanceType instanceType = new InstanceType();
		
		final QueueScheduler toTest = spy(new QueueScheduler(deploymentManager, this.instanceManager));
		final JobID jobid = new JobID();
		final AllocatedResource resource = mock(AllocatedResource.class);
		when(resource.getInstanceType()).thenReturn(instanceType);
		final List<AllocatedResource> resources = new ArrayList<AllocatedResource>();
		resources.add(resource);
		InstanceConnectionInfo instanceConnectionInfo = mock(InstanceConnectionInfo.class);
		when(instanceConnectionInfo.toString()).thenReturn("");
		LocalInstance instance = spy(new LocalInstance(instanceType, instanceConnectionInfo, null, null, null));

		final Method methodToMock = MemberMatcher.method(QueueScheduler.class, JobID.class);
		PowerMockito.when(toTest, methodToMock).withArguments(Matchers.any(JobID.class)).thenReturn(null);
		when(resource.getInstance()).thenReturn(instance);
		try {
			whenNew(ExecutionGraphIterator.class).withArguments(Matchers.any(ExecutionGraph.class),
				Matchers.anyBoolean()).thenReturn(this.graphIterator);
			whenNew(ExecutionGraphIterator.class).withArguments(Matchers.any(ExecutionGraph.class), Matchers.anyInt(),
				Matchers.anyBoolean(), Matchers.anyBoolean()).thenReturn(this.graphIterator2);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		PowerMockito.when(toTest, methodToMock).withArguments(Matchers.any(JobID.class))
		.thenReturn(this.executionGraph);
		when(this.graphIterator.next()).thenReturn(this.vertex1);
		when(this.graphIterator.hasNext()).thenReturn(true, true, true, true, false);
		when(this.graphIterator2.next()).thenReturn(this.vertex1);
		when(this.graphIterator2.hasNext()).thenReturn(true, true, true, true, false);
		when(this.vertex1.getExecutionState()).thenReturn(ExecutionState.SCHEDULED);
	
		whenNew(ExecutionGroupVertexIterator.class).withArguments(this.executionGraph, true, 0).thenReturn(this.groupVertexIterator);
		when(this.groupVertexIterator.hasNext()).thenReturn(true, true, true, true, false);
		when(this.groupVertexIterator.next()).thenReturn(this.groupVertex);
		when(this.groupVertex.getCurrentNumberOfGroupMembers()).thenReturn(0, 1, 2, 3);
		when(this.groupVertex.getGroupMember(Mockito.anyInt())).thenReturn(this.vertex1);
		when(this.vertex1.getAllocatedResource()).thenReturn(resource);
		when(resource.getInstanceType()).thenReturn(instanceType);
		when(this.executionGraph.getCurrentExecutionStage()).thenReturn(this.stage1);
		when(this.stage1.getNumberOfStageMembers()).thenReturn(0);
		
		toTest.resourcesAllocated(jobid, resources);
		verify(this.vertex1, times(4)).updateExecutionState(ExecutionState.ASSIGNED);

	}
	
}
