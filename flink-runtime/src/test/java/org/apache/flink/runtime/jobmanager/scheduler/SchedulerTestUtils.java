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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SchedulerTestUtils {
	
	private static final AtomicInteger port = new AtomicInteger(10000);

	// --------------------------------------------------------------------------------------------
	
	public static Instance getRandomInstance(int numSlots) {
		if (numSlots <= 0) {
			throw new IllegalArgumentException();
		}
		
		final ResourceID resourceID = ResourceID.generate();
		final InetAddress address;
		try {
			address = InetAddress.getByName("127.0.0.1");
		}
		catch (UnknownHostException e) {
			throw new RuntimeException("Test could not create IP address for localhost loopback.");
		}
		
		int dataPort = port.getAndIncrement();
		
		TaskManagerLocation ci = new TaskManagerLocation(resourceID, address, dataPort);
		
		final long GB = 1024L*1024*1024;
		HardwareDescription resources = new HardwareDescription(4, 4*GB, 3*GB, 2*GB);
		
		return new Instance(
			new SimpleAckingTaskManagerGateway(),
			ci,
			new InstanceID(),
			resources,
			numSlots);
	}
	
	
	public static Execution getDummyTask() {
		ExecutionJobVertex executionJobVertex = mock(ExecutionJobVertex.class);

		ExecutionVertex vertex = mock(ExecutionVertex.class);
		when(vertex.getJobId()).thenReturn(new JobID());
		when(vertex.toString()).thenReturn("TEST-VERTEX");
		when(vertex.getJobVertex()).thenReturn(executionJobVertex);
		when(vertex.getJobvertexId()).thenReturn(new JobVertexID());

		Execution execution = mock(Execution.class);
		when(execution.getVertex()).thenReturn(vertex);
		
		return execution;
	}

	public static Execution getTestVertex(Instance... preferredInstances) {
		List<TaskManagerLocation> locations = new ArrayList<>(preferredInstances.length);
		for (Instance i : preferredInstances) {
			locations.add(i.getTaskManagerLocation());
		}
		return getTestVertex(locations);
	}

	public static Execution getTestVertex(TaskManagerLocation... preferredLocations) {
		return getTestVertex(Arrays.asList(preferredLocations));
	}
	
	
	public static Execution getTestVertex(Iterable<TaskManagerLocation> preferredLocations) {
		Collection<CompletableFuture<TaskManagerLocation>> preferredLocationFutures = new ArrayList<>(4);

		for (TaskManagerLocation preferredLocation : preferredLocations) {
			preferredLocationFutures.add(CompletableFuture.completedFuture(preferredLocation));
		}

		return getTestVertex(preferredLocationFutures);
	}

	public static Execution getTestVertex(Collection<CompletableFuture<TaskManagerLocation>> preferredLocationFutures) {
		ExecutionJobVertex executionJobVertex = mock(ExecutionJobVertex.class);
		ExecutionVertex vertex = mock(ExecutionVertex.class);

		when(vertex.getPreferredLocationsBasedOnInputs()).thenReturn(preferredLocationFutures);
		when(vertex.getPreferredLocations()).thenReturn(preferredLocationFutures);
		when(vertex.getJobId()).thenReturn(new JobID());
		when(vertex.toString()).thenReturn("TEST-VERTEX");
		when(vertex.getJobVertex()).thenReturn(executionJobVertex);
		when(vertex.getJobvertexId()).thenReturn(new JobVertexID());

		Execution execution = mock(Execution.class);
		when(execution.getVertex()).thenReturn(vertex);
		when(execution.calculatePreferredLocations(any(LocationPreferenceConstraint.class))).thenCallRealMethod();

		return execution;
	}
	
	public static Execution getTestVertex(JobVertexID jid, int taskIndex, int numTasks, SlotSharingGroup slotSharingGroup) {
		ExecutionJobVertex executionJobVertex = mock(ExecutionJobVertex.class);
		ExecutionVertex vertex = mock(ExecutionVertex.class);

		when(executionJobVertex.getSlotSharingGroup()).thenReturn(slotSharingGroup);
		when(vertex.getPreferredLocationsBasedOnInputs()).thenReturn(Collections.emptyList());
		when(vertex.getJobId()).thenReturn(new JobID());
		when(vertex.getJobvertexId()).thenReturn(jid);
		when(vertex.getParallelSubtaskIndex()).thenReturn(taskIndex);
		when(vertex.getTotalNumberOfParallelSubtasks()).thenReturn(numTasks);
		when(vertex.getMaxParallelism()).thenReturn(numTasks);
		when(vertex.toString()).thenReturn("TEST-VERTEX");
		when(vertex.getTaskNameWithSubtaskIndex()).thenReturn("TEST-VERTEX");
		when(vertex.getJobVertex()).thenReturn(executionJobVertex);

		Execution execution = mock(Execution.class);
		when(execution.getVertex()).thenReturn(vertex);
		
		return execution;
	}

	public static Execution getTestVertexWithLocation(
			JobVertexID jid,
			int taskIndex,
			int numTasks,
			SlotSharingGroup slotSharingGroup,
			TaskManagerLocation... locations) {

		ExecutionJobVertex executionJobVertex = mock(ExecutionJobVertex.class);

		when(executionJobVertex.getSlotSharingGroup()).thenReturn(slotSharingGroup);

		ExecutionVertex vertex = mock(ExecutionVertex.class);

		Collection<CompletableFuture<TaskManagerLocation>> preferredLocationFutures = new ArrayList<>(locations.length);

		for (TaskManagerLocation location : locations) {
			preferredLocationFutures.add(CompletableFuture.completedFuture(location));
		}

		when(vertex.getJobVertex()).thenReturn(executionJobVertex);
		when(vertex.getPreferredLocationsBasedOnInputs()).thenReturn(preferredLocationFutures);
		when(vertex.getJobId()).thenReturn(new JobID());
		when(vertex.getJobvertexId()).thenReturn(jid);
		when(vertex.getParallelSubtaskIndex()).thenReturn(taskIndex);
		when(vertex.getTotalNumberOfParallelSubtasks()).thenReturn(numTasks);
		when(vertex.getMaxParallelism()).thenReturn(numTasks);
		when(vertex.toString()).thenReturn("TEST-VERTEX");

		Execution execution = mock(Execution.class);
		when(execution.getVertex()).thenReturn(vertex);

		return execution;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static boolean areAllDistinct(Object ... obj) {
		if (obj == null) {
			return true;
		}
		
		HashSet<Object> set = new HashSet<Object>();
		Collections.addAll(set, obj);
		
		return set.size() == obj.length;
	}
}
