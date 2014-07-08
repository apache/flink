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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import eu.stratosphere.nephele.instance.*;
import org.junit.Test;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * Tests for {@link eu.stratosphere.nephele.instance.Instance}.
 * 
 */
public class HostInClusterTest {

	/**
	 * Creates a cluster instance of a special test type.
	 * 
	 * @return a cluster instance of a special test type
	 */
	private Instance createTestClusterInstance() {

		final int ipcPort = ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT;
		final int dataPort = ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT;
		InetAddress inetAddress = null;
		try {
			inetAddress = InetAddress.getByName("127.0.0.1");
		} catch (UnknownHostException e) {
			fail(e.getMessage());
		}

		final int numCores = 8;
		final int memorySize = 32 * 1024;

		final InstanceConnectionInfo instanceConnectionInfo = new InstanceConnectionInfo(inetAddress, ipcPort, dataPort);

		final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(numCores,
			memorySize * 1024L * 1024L, memorySize * 1024L * 1024L);

		final NetworkTopology topology = NetworkTopology.createEmptyTopology();
		Instance host = new Instance(instanceConnectionInfo, topology.getRootNode(), topology,
			hardwareDescription, 8);

		return host;
	}

	/**
	 * Checks whether the tracking of heart beats is correct.
	 */
	@Test
	public void testHeartBeat() {
		// check that heart beat is triggered correctly.

		Instance host = createTestClusterInstance();

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
		final Instance host = createTestClusterInstance();
		final JobID jobID = new JobID();
		for (int run = 0; run < 2; ++run) {
			// do this twice to check that everything is correctly freed
			AllocatedResource[] allocatedSlots = new AllocatedResource[8];
			for (int i = 0; i < 8; ++i) {
				try {
					allocatedSlots[i] = host.allocateSlot(jobID);
				}catch(InstanceException ex){
					fail(ex.getMessage());
				}

				assertNotNull(allocatedSlots[i]);
			}
			// now no resources should be left
			boolean instanceException = false;

			try{
				host.allocateSlot(jobID);
			}catch(InstanceException ex){
				instanceException = true;
			}

			assertTrue(instanceException);

			for (int i = 0; i < 8; ++i) {
				host.releaseSlot(allocatedSlots[i].getAllocationID());
			}
		}
	}

	/**
	 * This test checks if allocated slices inside a cluster instance are removed correctly.
	 */
	@Test
	public void testTermination() {
		
		// check whether the accounting of capacity works correctly if terminateAllInstances is called
		final Instance host = createTestClusterInstance();
		final JobID jobID = new JobID();
		for (int run = 0; run < 2; ++run) {
			// do this twice to check that everything is correctly freed
			AllocatedResource[] allocatedResources = new AllocatedResource[8];
			for (int i = 0; i < 8; ++i) {
				try {
					allocatedResources[i] = host.allocateSlot(jobID);
				}catch (InstanceException ex){
					fail(ex.getMessage());
				}

				assertNotNull(allocatedResources[i]);
			}

			boolean instanceException = false;
			// now no resources should be left
			try {
				host.allocateSlot(jobID);
			} catch (InstanceException ex){
				instanceException = true;
			}

			assertTrue(instanceException);
			Collection<AllocatedSlot> allocatedSlots = host.removeAllocatedSlots();
			Set<AllocationID> removedAllocationIDs = new HashSet<AllocationID>();

			for(AllocatedSlot slot: allocatedSlots){
				removedAllocationIDs.add(slot.getAllocationID());
			}

			final Set<AllocationID> allocationIDs = new HashSet<AllocationID>();
			for(int i = 0; i < allocatedResources.length; ++i) {
				allocationIDs.add(allocatedResources[i].getAllocationID());
			}
			

			//Check if both sets are equal
			assertEquals(allocationIDs.size(), removedAllocationIDs.size());
			final Iterator<AllocationID> it = allocationIDs.iterator();
			while(it.hasNext()) {
				assertTrue(removedAllocationIDs.remove(it.next()));
			}
			
			assertEquals(0, removedAllocationIDs.size());
		}
	}
}
