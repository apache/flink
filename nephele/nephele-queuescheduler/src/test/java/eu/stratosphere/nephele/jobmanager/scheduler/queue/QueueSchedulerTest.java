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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.local.LocalInstance;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingException;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingListener;


/**
 * @author marrus
 *This class checks the functionality of the {@link QueueScheduler} class
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(QueueScheduler.class)
@SuppressStaticInitializationFor("eu.stratosphere.nephele.jobmanager.scheduler.queue.QueueScheduler")

public class QueueSchedulerTest {
	@Mock
	ExecutionGraph executionGraph;
	@Mock
	ExecutionVertex vertex1;
	@Mock
	ExecutionVertex vertex2;
	@Mock
	ExecutionGraphIterator graphIterator;
	@Mock
	ExecutionGraphIterator graphIterator2;
	@Mock
	SchedulingListener schedulingListener;
	@Mock
	InstanceManager instanceManager;
	@Mock 
	Environment environment;
	@Mock
	Log loggerMock;
	@Mock
	Deque<ExecutionGraph> queue;

	/**
	 * Setting up the mocks and necessary internal states
	 */
	@Before
	public void before() {
	    MockitoAnnotations.initMocks(this);
	    Whitebox.setInternalState(QueueScheduler.class, this.loggerMock);
	}
	
	/**
	 * Checks the behavior of the scheduleJob() method
	 */
	@Test
	public void testSchedulJob(){
		InstanceType type = new InstanceType();
		InstanceTypeDescription desc = InstanceTypeDescriptionFactory.construct(type, new HardwareDescription(), 4);
		HashMap<InstanceType, Integer> requiredInstanceTypes = new HashMap<InstanceType, Integer>();
		requiredInstanceTypes.put(type , 3);
		HashMap<InstanceType, InstanceTypeDescription> availableInstances = new HashMap<InstanceType, InstanceTypeDescription>();
		availableInstances.put(type, desc );
		
		try {
			whenNew( HashMap.class).withNoArguments().thenReturn(requiredInstanceTypes);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		when(this.executionGraph.getNumberOfStages()).thenReturn(1);
		when( this.instanceManager.getMapOfAvailableInstanceTypes()).thenReturn(availableInstances);

		//correct walk through method
		QueueScheduler toTest = new QueueScheduler(this.schedulingListener, this.instanceManager);
		try {
			toTest.schedulJob(this.executionGraph);
			Deque<ExecutionGraph> jobQueue = Whitebox.getInternalState(toTest, "jobQueue");
			assertEquals("Job should be in list", true, jobQueue.contains(this.executionGraph));
			jobQueue.remove(this.executionGraph);
			
		} catch (SchedulingException e) {
			fail();
			e.printStackTrace();
		}
		
		//not enough available Instances
		desc = InstanceTypeDescriptionFactory.construct(type, new HardwareDescription(), 2);
		availableInstances.put(type, desc );
		try {
			toTest.schedulJob(this.executionGraph);
			fail();
			
		} catch (SchedulingException e) {
			Deque<ExecutionGraph> jobQueue = Whitebox.getInternalState(toTest, "jobQueue");
			assertEquals("Job should not be in list", false, jobQueue.contains(this.executionGraph));
			
		}
		//Instance unknown
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
	 * Checks the behavior of the getVerticesReadyToBeExecuted() method
	 */
	@Test
	public void testGetVerticesReadyToBeExecuted(){
		
		QueueScheduler toTest = new QueueScheduler(this.schedulingListener, this.instanceManager);
		InstanceType type = new InstanceType();
		HashMap<InstanceType, Integer> requiredInstanceTypes = new HashMap<InstanceType, Integer>();
		requiredInstanceTypes.put(type , 3);
		//mock iterator
		HashSet<ExecutionVertex> set = new HashSet<ExecutionVertex>();
		try {
			whenNew(ExecutionGraphIterator.class)
				.withArguments(Matchers.any(ExecutionGraph.class),Matchers.anyInt(),Matchers.anyBoolean(), Matchers.anyBoolean()).thenReturn(this.graphIterator);
			whenNew(HashSet.class).withNoArguments().thenReturn(set);
			whenNew( HashMap.class).withNoArguments().thenReturn(requiredInstanceTypes);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		when(this.graphIterator.next()).thenReturn(this.vertex1);
		when(this.graphIterator.hasNext()).thenReturn(false, true, true, true, true, false);
		when(this.vertex1.getExecutionState()).thenReturn(ExecutionState.ASSIGNED);
		
		//no graphs in List, empty list return
		
		Set<ExecutionVertex> output = toTest.getVerticesReadyToBeExecuted();
		assertEquals(true, output.isEmpty());
		
		
		//graph in list
		this.queue = Whitebox.getInternalState(toTest, "jobQueue");
		this.queue.add(this.executionGraph);
		output =toTest.getVerticesReadyToBeExecuted();
		verify(this.vertex1, times(4)).setExecutionState(ExecutionState.READY);
		assertEquals(true, output.contains(this.vertex1));
	
		
	}
	
	
	/**
	 * Checks the behavior of the resourceAllocated() method
	 * @throws Exception
	 */
	@Test
	public void testResourceAllocated() throws Exception{
		
		QueueScheduler toTest = spy(new QueueScheduler(this.schedulingListener, this.instanceManager));
		JobID jobid = mock(JobID.class);
		AllocatedResource resource = mock(AllocatedResource.class);
		InstanceType instanceType = new InstanceType();
		InstanceConnectionInfo instanceConnectionInfo = mock(InstanceConnectionInfo.class);
		when(instanceConnectionInfo.toString()).thenReturn("");
		LocalInstance instance = spy(new LocalInstance(instanceType, instanceConnectionInfo, null, null, null));
		
		//given resource is null
		toTest.resourceAllocated(null,null);
		verify(this.loggerMock).error(Matchers.anyString());
		 
		//jobs have have been canceled
		final Method methodToMock = MemberMatcher.method(QueueScheduler.class, JobID.class);
		 PowerMockito.when(toTest, methodToMock).withArguments(Matchers.any(JobID.class)).thenReturn(null);
		 when(resource.getInstance()).thenReturn(instance);
		
		 toTest.resourceAllocated(jobid,resource);
		try {
			verify(this.instanceManager).releaseAllocatedResource(Matchers.any(JobID.class), Matchers.any(Configuration.class), Matchers.any(AllocatedResource.class));
		} catch (InstanceException e1) {
			e1.printStackTrace();
		}
		
		
		//vertex resource is null
		PowerMockito.when(toTest, methodToMock).withArguments(Matchers.any(JobID.class)).thenReturn(this.executionGraph);
		when(this.graphIterator.next()).thenReturn(this.vertex1);
		when(this.graphIterator.hasNext()).thenReturn(true, true, true, true, false);
		when(this.graphIterator2.next()).thenReturn(this.vertex1);
		when(this.graphIterator2.hasNext()).thenReturn(true, true, true, true, false);
		when(this.vertex1.getExecutionState()).thenReturn(ExecutionState.ASSIGNING);
		try {
			whenNew(ExecutionGraphIterator.class).withArguments(Matchers.any(ExecutionGraph.class),Matchers.anyBoolean()).thenReturn(this.graphIterator);
			whenNew(ExecutionGraphIterator.class).withArguments(Matchers.any(ExecutionGraph.class),Matchers.anyInt(),Matchers.anyBoolean(),Matchers.anyBoolean()).thenReturn(this.graphIterator2);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
		when(this.executionGraph.getJobID()).thenReturn(jobid);
		Deque<ExecutionGraph> jobQueue = Whitebox.getInternalState(toTest, "jobQueue");
		jobQueue.add(this.executionGraph);
		Whitebox.setInternalState(toTest, "jobQueue", jobQueue);
		when(this.vertex1.getAllocatedResource()).thenReturn(null);
		when(resource.getInstance()).thenReturn(instance);
		
		toTest.resourceAllocated(jobid,resource);
		verify(this.loggerMock).warn(Matchers.anyString());
		
		//correct walk through method
		when(this.graphIterator2.hasNext()).thenReturn(true, true, true, true, false);
		when(this.graphIterator.hasNext()).thenReturn(true, true, true, true, false);
		when(this.vertex1.getAllocatedResource()).thenReturn(resource);
		when(resource.getInstanceType()).thenReturn(instanceType);
		
		
		toTest.resourceAllocated(jobid,resource);
		verify(this.vertex1, times(4)).setExecutionState(ExecutionState.ASSIGNED);
		
		
		
	}
	
	
}
