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
import static org.mockito.Mockito.*;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.DummyActorGateway;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Test;

import scala.concurrent.duration.FiniteDuration;

public class VertexLocationConstraintTest {

	private static final FiniteDuration timeout = new FiniteDuration(100, TimeUnit.SECONDS);
	
	@Test
	public void testScheduleWithConstraint1() {
		try {
			final byte[] address1 = { 10, 0, 1, 4 };
			final byte[] address2 = { 10, 0, 1, 5 };
			final byte[] address3 = { 10, 0, 1, 6 };
			
			final String hostname1 = "host1";
			final String hostname2 = "host2";
			final String hostname3 = "host3";
			
			// prepare the scheduler
			Instance instance1 = getInstance(address1, 6789, hostname1);
			Instance instance2 = getInstance(address2, 6789, hostname2);
			Instance instance3 = getInstance(address3, 6789, hostname3);
			
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
			scheduler.newInstanceAvailable(instance1);
			scheduler.newInstanceAvailable(instance2);
			scheduler.newInstanceAvailable(instance3);
			
			// prepare the execution graph
			JobVertex jobVertex = new JobVertex("test vertex", new JobVertexID());
			jobVertex.setInvokableClass(DummyInvokable.class);
			jobVertex.setParallelism(2);
			JobGraph jg = new JobGraph("test job", new ExecutionConfig(), jobVertex);
			
			ExecutionGraph eg = new ExecutionGraph(
					TestingUtils.defaultExecutionContext(),
					jg.getJobID(),
					jg.getName(),
					jg.getJobConfiguration(),
					new ExecutionConfig(),
					timeout,
					new NoRestartStrategy());
			eg.attachJobGraph(Collections.singletonList(jobVertex));
			
			ExecutionJobVertex ejv = eg.getAllVertices().get(jobVertex.getID());
			ExecutionVertex[] vertices = ejv.getTaskVertices();
			
			vertices[0].setLocationConstraintHosts(Arrays.asList(instance1, instance2));
			vertices[1].setLocationConstraintHosts(Collections.singletonList(instance3));
			
			vertices[0].setScheduleLocalOnly(true);
			vertices[1].setScheduleLocalOnly(true);
			
			ejv.scheduleAll(scheduler, false);
			
			SimpleSlot slot1 = vertices[0].getCurrentAssignedResource();
			SimpleSlot slot2 = vertices[1].getCurrentAssignedResource();
			
			assertNotNull(slot1);
			assertNotNull(slot2);
			
			Instance target1 = slot1.getInstance();
			Instance target2 = slot2.getInstance();
			
			assertNotNull(target1);
			assertNotNull(target2);
			
			assertTrue(target1 == instance1 || target1 == instance2);
			assertTrue(target2 == instance3);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testScheduleWithConstraint2() {
		
		// same test as above, which swapped host names to guard against "accidentally worked" because of
		// the order in which requests are handles by internal data structures
		
		try {
			final byte[] address1 = { 10, 0, 1, 4 };
			final byte[] address2 = { 10, 0, 1, 5 };
			final byte[] address3 = { 10, 0, 1, 6 };
			
			final String hostname1 = "host1";
			final String hostname2 = "host2";
			final String hostname3 = "host3";
			
			// prepare the scheduler
			Instance instance1 = getInstance(address1, 6789, hostname1);
			Instance instance2 = getInstance(address2, 6789, hostname2);
			Instance instance3 = getInstance(address3, 6789, hostname3);
			
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
			scheduler.newInstanceAvailable(instance1);
			scheduler.newInstanceAvailable(instance2);
			scheduler.newInstanceAvailable(instance3);
			
			// prepare the execution graph
			JobVertex jobVertex = new JobVertex("test vertex", new JobVertexID());
			jobVertex.setInvokableClass(DummyInvokable.class);
			jobVertex.setParallelism(2);
			JobGraph jg = new JobGraph("test job", new ExecutionConfig(), jobVertex);
			
			ExecutionGraph eg = new ExecutionGraph(
					TestingUtils.defaultExecutionContext(),
					jg.getJobID(),
					jg.getName(),
					jg.getJobConfiguration(),
					new ExecutionConfig(),
					timeout,
					new NoRestartStrategy());
			eg.attachJobGraph(Collections.singletonList(jobVertex));
			
			ExecutionJobVertex ejv = eg.getAllVertices().get(jobVertex.getID());
			ExecutionVertex[] vertices = ejv.getTaskVertices();
			
			vertices[0].setLocationConstraintHosts(Collections.singletonList(instance3));
			vertices[1].setLocationConstraintHosts(Arrays.asList(instance1, instance2));
			
			vertices[0].setScheduleLocalOnly(true);
			vertices[1].setScheduleLocalOnly(true);
			
			ejv.scheduleAll(scheduler, false);
			
			SimpleSlot slot1 = vertices[0].getCurrentAssignedResource();
			SimpleSlot slot2 = vertices[1].getCurrentAssignedResource();
			
			assertNotNull(slot1);
			assertNotNull(slot2);
			
			Instance target1 = slot1.getInstance();
			Instance target2 = slot2.getInstance();
			
			assertNotNull(target1);
			assertNotNull(target2);
			
			assertTrue(target1 == instance3);
			assertTrue(target2 == instance1 || target2 == instance2);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testScheduleWithConstraintAndSlotSharing() {
		try {
			final byte[] address1 = { 10, 0, 1, 4 };
			final byte[] address2 = { 10, 0, 1, 5 };
			final byte[] address3 = { 10, 0, 1, 6 };
			
			final String hostname1 = "host1";
			final String hostname2 = "host2";
			final String hostname3 = "host3";
			
			// prepare the scheduler
			Instance instance1 = getInstance(address1, 6789, hostname1);
			Instance instance2 = getInstance(address2, 6789, hostname2);
			Instance instance3 = getInstance(address3, 6789, hostname3);
			
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
			scheduler.newInstanceAvailable(instance1);
			scheduler.newInstanceAvailable(instance2);
			scheduler.newInstanceAvailable(instance3);
			
			// prepare the execution graph
			JobVertex jobVertex1 = new JobVertex("v1", new JobVertexID());
			JobVertex jobVertex2 = new JobVertex("v2", new JobVertexID());
			jobVertex1.setInvokableClass(DummyInvokable.class);
			jobVertex2.setInvokableClass(DummyInvokable.class);
			jobVertex1.setParallelism(2);
			jobVertex2.setParallelism(3);
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			jobVertex1.setSlotSharingGroup(sharingGroup);
			jobVertex2.setSlotSharingGroup(sharingGroup);
			
			JobGraph jg = new JobGraph("test job", new ExecutionConfig(), jobVertex1, jobVertex2);
			
			ExecutionGraph eg = new ExecutionGraph(
					TestingUtils.defaultExecutionContext(),
					jg.getJobID(),
					jg.getName(),
					jg.getJobConfiguration(),
					new ExecutionConfig(),
					timeout,
					new NoRestartStrategy());
			eg.attachJobGraph(Arrays.asList(jobVertex1, jobVertex2));
			
			ExecutionJobVertex ejv = eg.getAllVertices().get(jobVertex1.getID());
			ExecutionVertex[] vertices = ejv.getTaskVertices();
			
			vertices[0].setLocationConstraintHosts(Arrays.asList(instance1, instance2));
			vertices[1].setLocationConstraintHosts(Collections.singletonList(instance3));
			
			vertices[0].setScheduleLocalOnly(true);
			vertices[1].setScheduleLocalOnly(true);
			
			ejv.scheduleAll(scheduler, false);
			
			SimpleSlot slot1 = vertices[0].getCurrentAssignedResource();
			SimpleSlot slot2 = vertices[1].getCurrentAssignedResource();
			
			assertNotNull(slot1);
			assertNotNull(slot2);
			
			Instance target1 = slot1.getInstance();
			Instance target2 = slot2.getInstance();
			
			assertNotNull(target1);
			assertNotNull(target2);
			
			assertTrue(target1 == instance1 || target1 == instance2);
			assertTrue(target2 == instance3);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testScheduleWithUnfulfillableConstraint() {
		
		// same test as above, which swapped host names to guard against "accidentally worked" because of
		// the order in which requests are handles by internal data structures
		
		try {
			final byte[] address1 = { 10, 0, 1, 4 };
			final byte[] address2 = { 10, 0, 1, 5 };
			
			final String hostname1 = "host1";
			final String hostname2 = "host2";
			
			// prepare the scheduler
			Instance instance1 = getInstance(address1, 6789, hostname1);
			Instance instance2 = getInstance(address2, 6789, hostname2);
			
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
			scheduler.newInstanceAvailable(instance1);
			
			// prepare the execution graph
			JobVertex jobVertex = new JobVertex("test vertex", new JobVertexID());
			jobVertex.setInvokableClass(DummyInvokable.class);
			jobVertex.setParallelism(1);
			JobGraph jg = new JobGraph("test job", new ExecutionConfig(), jobVertex);
			
			ExecutionGraph eg = new ExecutionGraph(
					TestingUtils.defaultExecutionContext(),
					jg.getJobID(),
					jg.getName(),
					jg.getJobConfiguration(),
					new ExecutionConfig(),
					timeout,
					new NoRestartStrategy());
			eg.attachJobGraph(Collections.singletonList(jobVertex));
			
			ExecutionJobVertex ejv = eg.getAllVertices().get(jobVertex.getID());
			ExecutionVertex[] vertices = ejv.getTaskVertices();
			
			vertices[0].setLocationConstraintHosts(Collections.singletonList(instance2));
			vertices[0].setScheduleLocalOnly(true);
			
			try {
				ejv.scheduleAll(scheduler, false);
				fail("This should fail with a NoResourceAvailableException");
			}
			catch (NoResourceAvailableException e) {
				// bam! we are good...
				assertTrue(e.getMessage().contains(hostname2));
			}
			catch (Exception e) {
				fail("Wrong exception type");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testScheduleWithUnfulfillableConstraintInSharingGroup() {
		
		// same test as above, which swapped host names to guard against "accidentally worked" because of
		// the order in which requests are handles by internal data structures
		
		try {
			final byte[] address1 = { 10, 0, 1, 4 };
			final byte[] address2 = { 10, 0, 1, 5 };
			
			final String hostname1 = "host1";
			final String hostname2 = "host2";
			
			// prepare the scheduler
			Instance instance1 = getInstance(address1, 6789, hostname1);
			Instance instance2 = getInstance(address2, 6789, hostname2);
			
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
			scheduler.newInstanceAvailable(instance1);
			
			// prepare the execution graph
			JobVertex jobVertex1 = new JobVertex("v1", new JobVertexID());
			JobVertex jobVertex2 = new JobVertex("v2", new JobVertexID());
			
			jobVertex1.setInvokableClass(DummyInvokable.class);
			jobVertex2.setInvokableClass(DummyInvokable.class);
			
			jobVertex1.setParallelism(1);
			jobVertex2.setParallelism(1);
			
			JobGraph jg = new JobGraph("test job", new ExecutionConfig(), jobVertex1, jobVertex2);
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			jobVertex1.setSlotSharingGroup(sharingGroup);
			jobVertex2.setSlotSharingGroup(sharingGroup);
			
			ExecutionGraph eg = new ExecutionGraph(
					TestingUtils.defaultExecutionContext(),
					jg.getJobID(),
					jg.getName(),
					jg.getJobConfiguration(),
					new ExecutionConfig(),
					timeout,
					new NoRestartStrategy());
			eg.attachJobGraph(Arrays.asList(jobVertex1, jobVertex2));
			
			ExecutionJobVertex ejv = eg.getAllVertices().get(jobVertex1.getID());
			ExecutionVertex[] vertices = ejv.getTaskVertices();
			
			vertices[0].setLocationConstraintHosts(Collections.singletonList(instance2));
			vertices[0].setScheduleLocalOnly(true);
			
			try {
				ejv.scheduleAll(scheduler, false);
				fail("This should fail with a NoResourceAvailableException");
			}
			catch (NoResourceAvailableException e) {
				// bam! we are good...
				assertTrue(e.getMessage().contains(hostname2));
			}
			catch (Exception e) {
				fail("Wrong exception type");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testArchivingClearsFields() {
		try {
			JobVertex vertex = new JobVertex("test vertex", new JobVertexID());
			JobGraph jg = new JobGraph("test job", new ExecutionConfig(), vertex);
			
			ExecutionGraph eg = new ExecutionGraph(
					TestingUtils.defaultExecutionContext(),
					jg.getJobID(),
					jg.getName(),
					jg.getJobConfiguration(),
					new ExecutionConfig(),
					timeout,
					new NoRestartStrategy());
			eg.attachJobGraph(Collections.singletonList(vertex));
			
			ExecutionVertex ev = eg.getAllVertices().get(vertex.getID()).getTaskVertices()[0];
			
			Instance instance = ExecutionGraphTestUtils.getInstance(DummyActorGateway.INSTANCE);
			ev.setLocationConstraintHosts(Collections.singletonList(instance));
			
			assertNotNull(ev.getPreferredLocations());
			assertEquals(instance, ev.getPreferredLocations().iterator().next());
			
			// transition to a final state
			eg.fail(new Exception());
			
			eg.prepareForArchiving();
			
			assertTrue(ev.getPreferredLocations() == null || !ev.getPreferredLocations().iterator().hasNext());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static Instance getInstance(byte[] ipAddress, int dataPort, String hostname) throws Exception {
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
				ResourceID.generate(),
				new InstanceID(),
				hardwareDescription,
				1);
	}
}
