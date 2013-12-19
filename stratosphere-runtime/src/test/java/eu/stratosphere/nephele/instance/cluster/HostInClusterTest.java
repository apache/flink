/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance.cluster;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.*;

import eu.stratosphere.configuration.ConfigConstants;
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
 */
public class HostInClusterTest {

	/**
	 * Creates a cluster instance of a special test type.
	 * 
	 * @return a cluster instance of a special test type
	 */
	private ClusterInstance createTestClusterInstance() {

		final int ipcPort = ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT;
		final int dataPort = ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT;
		InetAddress inetAddress = null;
		try {
			inetAddress = InetAddress.getByName("127.0.0.1");
		} catch (UnknownHostException e) {
			fail(e.getMessage());
		}

		final String identifier = "testtype";
		final int numComputeUnits = 8;
		final int numCores = 8;
		final int memorySize = 32 * 1024;
		final int diskCapacity = 200;
		final int pricePerHour = 10;

		final InstanceType capacity = InstanceTypeFactory.construct(identifier, numComputeUnits, numCores, memorySize,
			diskCapacity,
			pricePerHour);

		final InstanceConnectionInfo instanceConnectionInfo = new InstanceConnectionInfo(inetAddress, ipcPort, dataPort);

		final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(numCores,
			memorySize * 1024L * 1024L, memorySize * 1024L * 1024L);

		final NetworkTopology topology = NetworkTopology.createEmptyTopology();
		ClusterInstance host = new ClusterInstance(instanceConnectionInfo, capacity, topology.getRootNode(), topology,
			hardwareDescription);

		return host;
	}

	/**
	 * Checks whether the tracking of heart beats is correct.
	 */
	@Test
	public void testHeartBeat() {
		// check that heart beat is triggered correctly.

		ClusterInstance host = createTestClusterInstance();

		host.reportHeartBeat();

		assertTrue("we have not waited 1 second since last heart beat", host.isStillAlive(1000));

		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			fail(e.getMessage());
		}

		assertFalse("we have waited for more than 10 milliseconds", host.isStillAlive(10));
	}

	/**
	 * This test covers the internal accounting used by the cluster manager.
	 */
	@Test
	public void testAccounting() {
		// check whether the accounting of capacity works correctly
		final ClusterInstance host = createTestClusterInstance();
		final JobID jobID = new JobID();
		final int numComputeUnits = 8 / 8;
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

	/**
	 * This test checks if allocated slices inside a cluster instance are removed correctly.
	 */
	@Test
	public void testTermination() {
		
		// check whether the accounting of capacity works correctly if terminateAllInstances is called
		final ClusterInstance host = createTestClusterInstance();
		final JobID jobID = new JobID();
		final int numComputeUnits = 8 / 8;
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

			final Set<AllocatedSlice> slicesSet = new HashSet<AllocatedSlice>();
			for(int i = 0; i < slices.length; ++i) {
				slicesSet.add(slices[i]);
			}
			
			final Set<AllocatedSlice> removedSlicesSet = new HashSet<AllocatedSlice>(removedSlices);
			
			//Check if both sets are equal
			assertEquals(slicesSet.size(), removedSlices.size());
			final Iterator<AllocatedSlice> it = slicesSet.iterator();
			while(it.hasNext()) {
				assertTrue(removedSlicesSet.remove(it.next()));
			}
			
			assertEquals(0, removedSlicesSet.size());
		}
	}
}
