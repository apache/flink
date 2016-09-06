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

package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.junit.Test;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KvStateLocationRegistryTest {

	/**
	 * Simple test registering/unregistereing state and looking it up again.
	 */
	@Test
	public void testRegisterAndLookup() throws Exception {
		String[] registrationNames = new String[] {
				"TAsIrGnc7MULwVupNKZ0",
				"086133IrGn0Ii2853237" };

		ExecutionJobVertex[] vertices = new ExecutionJobVertex[] {
				createJobVertex(32),
				createJobVertex(13) };

		// IDs for each key group of each vertex
		KvStateID[][] ids = new KvStateID[vertices.length][];
		for (int i = 0; i < ids.length; i++) {
			ids[i] = new KvStateID[vertices[i].getParallelism()];
			for (int j = 0; j < vertices[i].getParallelism(); j++) {
				ids[i][j] = new KvStateID();
			}
		}

		KvStateServerAddress server = new KvStateServerAddress(InetAddress.getLocalHost(), 12032);

		// Create registry
		Map<JobVertexID, ExecutionJobVertex> vertexMap = createVertexMap(vertices);
		KvStateLocationRegistry registry = new KvStateLocationRegistry(new JobID(), vertexMap);

		// Register
		for (int i = 0; i < vertices.length; i++) {
			int numKeyGroups = vertices[i].getParallelism();
			for (int keyGroupIndex = 0; keyGroupIndex < numKeyGroups; keyGroupIndex++) {
				// Register
				registry.notifyKvStateRegistered(
						vertices[i].getJobVertexId(),
						keyGroupIndex,
						registrationNames[i],
						ids[i][keyGroupIndex],
						server);
			}
		}

		// Verify all registrations
		for (int i = 0; i < vertices.length; i++) {
			KvStateLocation location = registry.getKvStateLocation(registrationNames[i]);
			assertNotNull(location);

			int parallelism = vertices[i].getParallelism();
			for (int keyGroupIndex = 0; keyGroupIndex < parallelism; keyGroupIndex++) {
				assertEquals(ids[i][keyGroupIndex], location.getKvStateID(keyGroupIndex));
				assertEquals(server, location.getKvStateServerAddress(keyGroupIndex));
			}
		}

		// Unregister
		for (int i = 0; i < vertices.length; i++) {
			int numKeyGroups = vertices[i].getParallelism();
			JobVertexID jobVertexId = vertices[i].getJobVertexId();
			for (int keyGroupIndex = 0; keyGroupIndex < numKeyGroups; keyGroupIndex++) {
				registry.notifyKvStateUnregistered(jobVertexId, keyGroupIndex, registrationNames[i]);
			}
		}

		for (int i = 0; i < registrationNames.length; i++) {
			assertNull(registry.getKvStateLocation(registrationNames[i]));
		}
	}

	/**
	 * Tests that registrations with duplicate names throw an Exception.
	 */
	@Test
	public void testRegisterDuplicateName() throws Exception {
		ExecutionJobVertex[] vertices = new ExecutionJobVertex[] {
				createJobVertex(32),
				createJobVertex(13) };

		Map<JobVertexID, ExecutionJobVertex> vertexMap = createVertexMap(vertices);

		String registrationName = "duplicated-name";
		KvStateLocationRegistry registry = new KvStateLocationRegistry(new JobID(), vertexMap);

		// First operator registers
		registry.notifyKvStateRegistered(
				vertices[0].getJobVertexId(),
				0,
				registrationName,
				new KvStateID(),
				new KvStateServerAddress(InetAddress.getLocalHost(), 12328));

		try {
			// Second operator registers same name
			registry.notifyKvStateRegistered(
					vertices[1].getJobVertexId(),
					0,
					registrationName,
					new KvStateID(),
					new KvStateServerAddress(InetAddress.getLocalHost(), 12032));

			fail("Did not throw expected Exception after duplicated name");
		} catch (IllegalStateException ignored) {
			// Expected
		}
	}

	/**
	 * Tests exception on unregistration before registration.
	 */
	@Test
	public void testUnregisterBeforeRegister() throws Exception {
		ExecutionJobVertex vertex = createJobVertex(4);
		Map<JobVertexID, ExecutionJobVertex> vertexMap = createVertexMap(vertex);

		KvStateLocationRegistry registry = new KvStateLocationRegistry(new JobID(), vertexMap);
		try {
			registry.notifyKvStateUnregistered(vertex.getJobVertexId(), 0, "any-name");
			fail("Did not throw expected Exception, because of missing registration");
		} catch (IllegalArgumentException ignored) {
			// Expected
		}
	}

	/**
	 * Tests failures during unregistration.
	 */
	@Test
	public void testUnregisterFailures() throws Exception {
		String name = "IrGnc73237TAs";

		ExecutionJobVertex[] vertices = new ExecutionJobVertex[] {
				createJobVertex(32),
				createJobVertex(13) };

		Map<JobVertexID, ExecutionJobVertex> vertexMap = new HashMap<>();
		for (ExecutionJobVertex vertex : vertices) {
			vertexMap.put(vertex.getJobVertexId(), vertex);
		}

		KvStateLocationRegistry registry = new KvStateLocationRegistry(new JobID(), vertexMap);

		// First operator registers name
		registry.notifyKvStateRegistered(
				vertices[0].getJobVertexId(),
				0,
				name,
				new KvStateID(),
				mock(KvStateServerAddress.class));

		try {
			// Unregister not registered keyGroupIndex
			int notRegisteredKeyGroupIndex = 2;

			registry.notifyKvStateUnregistered(
					vertices[0].getJobVertexId(),
					notRegisteredKeyGroupIndex,
					name);

			fail("Did not throw expected Exception");
		} catch (IllegalArgumentException expected) {
		}

		try {
			// Wrong operator tries to unregister
			registry.notifyKvStateUnregistered(
					vertices[1].getJobVertexId(),
					0,
					name);

			fail("Did not throw expected Exception");
		} catch (IllegalArgumentException expected) {
		}
	}

	// ------------------------------------------------------------------------
	
	private ExecutionJobVertex createJobVertex(int parallelism) {
		JobVertexID id = new JobVertexID();
		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);

		when(vertex.getJobVertexId()).thenReturn(id);
		when(vertex.getParallelism()).thenReturn(parallelism);

		return vertex;
	}

	private Map<JobVertexID, ExecutionJobVertex> createVertexMap(ExecutionJobVertex... vertices) {
		Map<JobVertexID, ExecutionJobVertex> vertexMap = new HashMap<>();
		for (ExecutionJobVertex vertex : vertices) {
			vertexMap.put(vertex.getJobVertexId(), vertex);
		}
		return vertexMap;
	}
}
