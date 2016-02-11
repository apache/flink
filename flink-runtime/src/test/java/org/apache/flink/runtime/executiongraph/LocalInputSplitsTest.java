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

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.io.StrictlyLocalAssignment;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Test;

import scala.concurrent.duration.FiniteDuration;

public class LocalInputSplitsTest {
	
	private static final FiniteDuration TIMEOUT = new FiniteDuration(100, TimeUnit.SECONDS);
	
	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testNotEnoughSubtasks() {
		int numHosts = 3;
		int slotsPerHost = 1;
		int parallelism = 2;
		
		TestLocatableInputSplit[] splits = new TestLocatableInputSplit[] {
				new TestLocatableInputSplit(1, "host1"),
				new TestLocatableInputSplit(2, "host2"),
				new TestLocatableInputSplit(3, "host3")
		};
		
		// This should fail with an exception, since the parallelism of 2 does not
		// support strictly local assignment onto 3 hosts
		try {
			runTests(numHosts, slotsPerHost, parallelism, splits);
			fail("should throw an exception");
		}
		catch (JobException e) {
			// what a great day!
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testDisallowMultipleLocations() {
		int numHosts = 2;
		int slotsPerHost = 1;
		int parallelism = 2;
		
		TestLocatableInputSplit[] splits = new TestLocatableInputSplit[] {
				new TestLocatableInputSplit(1, new String[] { "host1", "host2" } ),
				new TestLocatableInputSplit(2, new String[] { "host1", "host2" } )
		};
		
		// This should fail with an exception, since strictly local assignment
		// currently supports only one choice of host
		try {
			runTests(numHosts, slotsPerHost, parallelism, splits);
			fail("should throw an exception");
		}
		catch (JobException e) {
			// dandy!
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testNonExistentHost() {
		int numHosts = 2;
		int slotsPerHost = 1;
		int parallelism = 2;
		
		TestLocatableInputSplit[] splits = new TestLocatableInputSplit[] {
				new TestLocatableInputSplit(1, "host1"),
				new TestLocatableInputSplit(2, "bogus_host" )
		};
		
		// This should fail with an exception, since one of the hosts does not exist
		try {
			runTests(numHosts, slotsPerHost, parallelism, splits);
			fail("should throw an exception");
		}
		catch (JobException e) {
			// dandy!
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testEqualSplitsPerHostAndSubtask() {
		int numHosts = 5;
		int slotsPerHost = 2;
		int parallelism = 10;
		
		TestLocatableInputSplit[] splits = new TestLocatableInputSplit[] {
				new TestLocatableInputSplit(7, "host4"),
				new TestLocatableInputSplit(8, "host4"),
				new TestLocatableInputSplit(1, "host1"),
				new TestLocatableInputSplit(2, "host1"),
				new TestLocatableInputSplit(3, "host2"),
				new TestLocatableInputSplit(4, "host2"),
				new TestLocatableInputSplit(5, "host3"),
				new TestLocatableInputSplit(6, "host3"),
				new TestLocatableInputSplit(9, "host5"),
				new TestLocatableInputSplit(10, "host5")
		};
		
		try {
			String[] hostsForTasks = runTests(numHosts, slotsPerHost, parallelism, splits);
			
			assertEquals("host1", hostsForTasks[0]);
			assertEquals("host1", hostsForTasks[1]);
			assertEquals("host2", hostsForTasks[2]);
			assertEquals("host2", hostsForTasks[3]);
			assertEquals("host3", hostsForTasks[4]);
			assertEquals("host3", hostsForTasks[5]);
			assertEquals("host4", hostsForTasks[6]);
			assertEquals("host4", hostsForTasks[7]);
			assertEquals("host5", hostsForTasks[8]);
			assertEquals("host5", hostsForTasks[9]);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testNonEqualSplitsPerhost() {
		int numHosts = 3;
		int slotsPerHost = 2;
		int parallelism = 5;
		
		TestLocatableInputSplit[] splits = new TestLocatableInputSplit[] {
				new TestLocatableInputSplit(1, "host3"),
				new TestLocatableInputSplit(2, "host1"),
				new TestLocatableInputSplit(3, "host1"),
				new TestLocatableInputSplit(4, "host1"),
				new TestLocatableInputSplit(5, "host1"),
				new TestLocatableInputSplit(6, "host1"),
				new TestLocatableInputSplit(7, "host2"),
				new TestLocatableInputSplit(8, "host2")
		};
		
		try {
			String[] hostsForTasks = runTests(numHosts, slotsPerHost, parallelism, splits);
			
			assertEquals("host1", hostsForTasks[0]);
			assertEquals("host1", hostsForTasks[1]);
			assertEquals("host2", hostsForTasks[2]);
			assertEquals("host2", hostsForTasks[3]);
			assertEquals("host3", hostsForTasks[4]);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testWithSubtasksEmpty() {
		int numHosts = 3;
		int slotsPerHost = 5;
		int parallelism = 7;
		
		// host one gets three subtasks (but two remain empty)
		// host two get two subtasks where one gets two splits, the other one split
		// host three gets two subtasks where one gets five splits, the other gets four splits
		
		TestLocatableInputSplit[] splits = new TestLocatableInputSplit[] {
				new TestLocatableInputSplit(1, "host1"),
				new TestLocatableInputSplit(2, "host2"),
				new TestLocatableInputSplit(3, "host2"),
				new TestLocatableInputSplit(4, "host2"),
				new TestLocatableInputSplit(5, "host3"),
				new TestLocatableInputSplit(6, "host3"),
				new TestLocatableInputSplit(7, "host3"),
				new TestLocatableInputSplit(8, "host3"),
				new TestLocatableInputSplit(9, "host3"),
				new TestLocatableInputSplit(10, "host3"),
				new TestLocatableInputSplit(11, "host3"),
				new TestLocatableInputSplit(12, "host3"),
				new TestLocatableInputSplit(13, "host3")
		};
		
		try {
			String[] hostsForTasks = runTests(numHosts, slotsPerHost, parallelism, splits);
			
			assertEquals("host1", hostsForTasks[0]);
			
			assertEquals("host2", hostsForTasks[1]);
			assertEquals("host2", hostsForTasks[2]);

			assertEquals("host3", hostsForTasks[3]);
			assertEquals("host3", hostsForTasks[4]);
			
			// the current assignment leaves those with empty constraints
			assertTrue(hostsForTasks[5].equals("host1") || hostsForTasks[5].equals("host2") || hostsForTasks[5].equals("host3"));
			assertTrue(hostsForTasks[6].equals("host1") || hostsForTasks[6].equals("host2") || hostsForTasks[6].equals("host3"));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMultipleInstancesPerHost() {

		TestLocatableInputSplit[] splits = new TestLocatableInputSplit[] {
				new TestLocatableInputSplit(1, "host1"),
				new TestLocatableInputSplit(2, "host1"),
				new TestLocatableInputSplit(3, "host2"),
				new TestLocatableInputSplit(4, "host2"),
				new TestLocatableInputSplit(5, "host3"),
				new TestLocatableInputSplit(6, "host3")
		};
		
		try {
			JobVertex vertex = new JobVertex("test vertex");
			vertex.setParallelism(6);
			vertex.setInvokableClass(DummyInvokable.class);
			vertex.setInputSplitSource(new TestInputSplitSource(splits));
			
			JobGraph jobGraph = new JobGraph("test job", vertex);
			
			ExecutionGraph eg = new ExecutionGraph(
				TestingUtils.defaultExecutionContext(), 
				jobGraph.getJobID(),
				jobGraph.getName(),  
				jobGraph.getJobConfiguration(),
				TIMEOUT,
				new NoRestartStrategy());
			
			eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());
			eg.setQueuedSchedulingAllowed(false);
			
			// create a scheduler with 6 instances where always two are on the same host
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
			Instance i1 = getInstance(new byte[] {10,0,1,1}, 12345, "host1", 1);
			Instance i2 = getInstance(new byte[] {10,0,1,1}, 12346, "host1", 1);
			Instance i3 = getInstance(new byte[] {10,0,1,2}, 12345, "host2", 1);
			Instance i4 = getInstance(new byte[] {10,0,1,2}, 12346, "host2", 1);
			Instance i5 = getInstance(new byte[] {10,0,1,3}, 12345, "host3", 1);
			Instance i6 = getInstance(new byte[] {10,0,1,3}, 12346, "host4", 1);
			scheduler.newInstanceAvailable(i1);
			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i3);
			scheduler.newInstanceAvailable(i4);
			scheduler.newInstanceAvailable(i5);
			scheduler.newInstanceAvailable(i6);
			
			eg.scheduleForExecution(scheduler);
			
			ExecutionVertex[] tasks = eg.getVerticesTopologically().iterator().next().getTaskVertices();
			assertEquals(6, tasks.length);
			
			Instance taskInstance1 = tasks[0].getCurrentAssignedResource().getInstance();
			Instance taskInstance2 = tasks[1].getCurrentAssignedResource().getInstance();
			Instance taskInstance3 = tasks[2].getCurrentAssignedResource().getInstance();
			Instance taskInstance4 = tasks[3].getCurrentAssignedResource().getInstance();
			Instance taskInstance5 = tasks[4].getCurrentAssignedResource().getInstance();
			Instance taskInstance6 = tasks[5].getCurrentAssignedResource().getInstance();
			
			assertTrue (taskInstance1 == i1 || taskInstance1 == i2);
			assertTrue (taskInstance2 == i1 || taskInstance2 == i2);
			assertTrue (taskInstance3 == i3 || taskInstance3 == i4);
			assertTrue (taskInstance4 == i3 || taskInstance4 == i4);
			assertTrue (taskInstance5 == i5 || taskInstance5 == i6);
			assertTrue (taskInstance6 == i5 || taskInstance6 == i6);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static String[] runTests(int numHosts, int slotsPerHost, int parallelism, 
			TestLocatableInputSplit[] splits)
		throws Exception
	{
		JobVertex vertex = new JobVertex("test vertex");
		vertex.setParallelism(parallelism);
		vertex.setInvokableClass(DummyInvokable.class);
		vertex.setInputSplitSource(new TestInputSplitSource(splits));
		
		JobGraph jobGraph = new JobGraph("test job", vertex);
		
		ExecutionGraph eg = new ExecutionGraph(
			TestingUtils.defaultExecutionContext(),
			jobGraph.getJobID(),
			jobGraph.getName(),  
			jobGraph.getJobConfiguration(),
			TIMEOUT,
			new NoRestartStrategy());
		
		eg.setQueuedSchedulingAllowed(false);
		
		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());
		
		Scheduler scheduler = getScheduler(numHosts, slotsPerHost);
		eg.scheduleForExecution(scheduler);
		
		ExecutionVertex[] tasks = eg.getVerticesTopologically().iterator().next().getTaskVertices();
		assertEquals(parallelism, tasks.length);
		
		String[] hostsForTasks = new String[parallelism];
		for (int i = 0; i < parallelism; i++) {
			hostsForTasks[i] = tasks[i].getCurrentAssignedResourceLocation().getHostname();
		}
		
		return hostsForTasks;
	}
	
	private static Scheduler getScheduler(int numInstances, int numSlotsPerInstance) throws Exception {
		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		
		for (int i = 0; i < numInstances; i++) {
			byte[] ipAddress = new byte[] { 10, 0, 1, (byte) (1 + i) };
			int dataPort = 12001 + i;
			String host = "host" + (i+1);
			
			Instance instance = getInstance(ipAddress, dataPort, host, numSlotsPerInstance);
			scheduler.newInstanceAvailable(instance);
		}
		return scheduler;
	}
	
	private static Instance getInstance(byte[] ipAddress, int dataPort, String hostname, int slots) throws Exception {
		HardwareDescription hardwareDescription = new HardwareDescription(4, 2L*1024*1024*1024, 1024*1024*1024, 512*1024*1024);
		
		InstanceConnectionInfo connection = mock(InstanceConnectionInfo.class);
		when(connection.address()).thenReturn(InetAddress.getByAddress(ipAddress));
		when(connection.dataPort()).thenReturn(dataPort);
		when(connection.getInetAdress()).thenReturn(InetAddress.getByAddress(ipAddress).toString());
		when(connection.getHostname()).thenReturn(hostname);
		when(connection.getFQDNHostname()).thenReturn(hostname);
		
		return new Instance(
				new ExecutionGraphTestUtils.SimpleActorGateway(
						TestingUtils.defaultExecutionContext()),
				connection,
				new InstanceID(),
				hardwareDescription,
				slots);
	}

	// --------------------------------------------------------------------------------------------
	
	// custom class to ensure behavior works for subclasses of LocatableInputSplit
	private static class TestLocatableInputSplit extends LocatableInputSplit {
		
		private static final long serialVersionUID = 1L;

		public TestLocatableInputSplit(int splitNumber, String hostname) {
			super(splitNumber, hostname);
		}
		
		public TestLocatableInputSplit(int splitNumber, String[] hostnames) {
			super(splitNumber, hostnames);
		}
	}
	
	private static class TestInputSplitSource implements InputSplitSource<TestLocatableInputSplit>,
		StrictlyLocalAssignment
	{
		private static final long serialVersionUID = 1L;
		
		private final TestLocatableInputSplit[] splits;
		
		public TestInputSplitSource(TestLocatableInputSplit[] splits) {
			this.splits = splits;
		}

		@Override
		public TestLocatableInputSplit[] createInputSplits(int minNumSplits) {
			return splits;
		}
		
		@Override
		public InputSplitAssigner getInputSplitAssigner(TestLocatableInputSplit[] inputSplits) {
			fail("This method should not be called on StrictlyLocalAssignment splits.");
			return null; // silence the compiler
		}
	}
}
