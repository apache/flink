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

package eu.stratosphere.nephele.instance.cluster;

import java.net.InetSocketAddress;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Sets;

import static org.junit.Assert.*;

import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.nephele.instance.cluster.AllocatedSlice;
import eu.stratosphere.nephele.instance.cluster.ClusterInstance;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * Tests for {@link ClusterInstance}.
 * 
 * @author Dominic Battre
 * @author warneke
 */
public class HostInClusterTest {

	private ClusterInstance createDefaultHost() {
		
		InetSocketAddress socket = new InetSocketAddress("localhost", 1234);
		String identifier = "identifier";
		final int numComputeUnits = 8 * 2500;
		final int numCores = 8;
		final int memorySize = 32 * 1024;
		final int diskCapacity = 200;
		final int pricePerHour = 10;
		final InstanceType capacity = InstanceTypeFactory.construct(identifier, numComputeUnits, numCores, memorySize,
			diskCapacity,
			pricePerHour);

		final InstanceConnectionInfo instanceConnectionInfo = new InstanceConnectionInfo(socket.getAddress(), socket
			.getPort(), 1235);

		final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(numCores,
			memorySize * 1024L * 1024L, memorySize * 1024L * 1024L);

		final NetworkTopology topology = NetworkTopology.createEmptyTopology();
		ClusterInstance host = new ClusterInstance(instanceConnectionInfo, capacity, topology.getRootNode(), topology,
			hardwareDescription);
		
		return host;
	}

	@Test
	public void testHeartBeat() {
		// check that heart beat is triggered correctly.

		ClusterInstance host = createDefaultHost();

		host.reportHeartBeat();
		assertTrue("we have not waited 1 second since last heart beat", host.isStillAlive(1000));
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
		}
		assertFalse("we have waited for more than 10 milliseconds", host.isStillAlive(10));
	}

	@Test
	public void testAccounting() {
		// check whether the accounting of capacity works correctly

		final ClusterInstance host = createDefaultHost();
		final JobID jobID = new JobID();

		final int numComputeUnits = 8 * 2500 / 8;
		final int numCores = 8 / 8;
		final int memorySize = 32 * 1024 / 8;
		final int diskCapacity = 200 / 8;
		final InstanceType type = InstanceTypeFactory.construct("dummy", numComputeUnits, numCores, memorySize,
			diskCapacity, -1);

		for (int run = 0; run < 2; ++run) {
			// do this twice to check that everything is correctly freed
			AllocatedSlice[] slices = new AllocatedSlice[8];
			for (int i = 0; i < 8; ++i) {
				slices[i] = host.createSlice(type, jobID);
				assertNotNull(slices[i]);
				assertEquals(numComputeUnits, slices[i].getType().getNumberOfComputeUnits());
				assertEquals(numCores, slices[i].getType().getNumberOfCores());
				assertEquals(memorySize, slices[i].getType().getMemorySize());
				assertEquals(diskCapacity, slices[i].getType().getDiskCapacity());
			}

			// now no resources should be left
			assertNull(host.createSlice(InstanceTypeFactory.construct("dummy", 1, 0, 0, 0, 0), jobID));
			assertNull(host.createSlice(InstanceTypeFactory.construct("dummy", 0, 1, 0, 0, 0), jobID));
			assertNull(host.createSlice(InstanceTypeFactory.construct("dummy", 0, 0, 1, 0, 0), jobID));
			assertNull(host.createSlice(InstanceTypeFactory.construct("dummy", 0, 0, 0, 1, 0), jobID));

			for (int i = 0; i < 8; ++i) {
				host.removeAllocatedSlice(slices[i].getAllocationID());
			}
		}
	}

	@Test
	public void testTermination() {
		// check whether the accounting of capacity works correctly if terminateAllInstances is called

		final ClusterInstance host = createDefaultHost();
		final JobID jobID = new JobID();

		final int numComputeUnits = 8 * 2500 / 8;
		final int numCores = 8 / 8;
		final int memorySize = 32 * 1024 / 8;
		final int diskCapacity = 200 / 8;
		final InstanceType type = InstanceTypeFactory.construct("dummy", numComputeUnits, numCores, memorySize,
			diskCapacity, -1);

		for (int run = 0; run < 2; ++run) {
			// do this twice to check that everything is correctly freed
			AllocatedSlice[] slices = new AllocatedSlice[8];
			for (int i = 0; i < 8; ++i) {
				slices[i] = host.createSlice(type, jobID);
				assertNotNull(slices[i]);
				assertEquals(numComputeUnits, slices[i].getType().getNumberOfComputeUnits());
				assertEquals(numCores, slices[i].getType().getNumberOfCores());
				assertEquals(memorySize, slices[i].getType().getMemorySize());
				assertEquals(diskCapacity, slices[i].getType().getDiskCapacity());
			}

			// now no resources should be left
			assertNull(host.createSlice(InstanceTypeFactory.construct("dummy", 1, 0, 0, 0, 0), jobID));
			assertNull(host.createSlice(InstanceTypeFactory.construct("dummy", 0, 1, 0, 0, 0), jobID));
			assertNull(host.createSlice(InstanceTypeFactory.construct("dummy", 0, 0, 1, 0, 0), jobID));
			assertNull(host.createSlice(InstanceTypeFactory.construct("dummy", 0, 0, 0, 1, 0), jobID));

			List<AllocatedSlice> removedSlices = host.removeAllAllocatedSlices();

			assertEquals(Sets.newHashSet(removedSlices), Sets.newHashSet(slices));
		}
	}
}
