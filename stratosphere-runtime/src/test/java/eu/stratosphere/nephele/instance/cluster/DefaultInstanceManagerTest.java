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
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import eu.stratosphere.nephele.instance.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.util.LogUtils;

/**
 * Tests for {@link eu.stratosphere.nephele.instance.DefaultInstanceManager}.
 */
public class DefaultInstanceManagerTest {

	@BeforeClass
	public static void initLogging() {
		LogUtils.initializeDefaultTestConsoleLogger();
	}
	
	
	@Test
	public void testInstanceRegistering() {
		try {
			DefaultInstanceManager cm = new DefaultInstanceManager();
			TestInstanceListener testInstanceListener = new TestInstanceListener();
			cm.setInstanceListener(testInstanceListener);
			
			
			int ipcPort = ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT;
			int dataPort = ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT;

			HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(2, 2L * 1024L * 1024L * 1024L,
																				2L * 1024L * 1024L * 1024L);

			String hostname = "192.168.198.1";
			InetAddress address = InetAddress.getByName("192.168.198.1");
			
			InstanceConnectionInfo ici1 = new InstanceConnectionInfo(address, hostname, null, ipcPort + 0, dataPort + 0);
			InstanceConnectionInfo ici2 = new InstanceConnectionInfo(address, hostname, null, ipcPort + 15, dataPort + 15);
			InstanceConnectionInfo ici3 = new InstanceConnectionInfo(address, hostname, null, ipcPort + 30, dataPort + 30);
			
			// register three instances
			cm.registerTaskManager(ici1, hardwareDescription, 1);
			cm.registerTaskManager(ici2, hardwareDescription, 1);
			cm.registerTaskManager(ici3, hardwareDescription, 1);
			

			assertEquals(3, cm.getNumberOfSlots());

			cm.shutdown();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test erroneous: " + e.getMessage());
		}
	}

	@Test
	public void testAllocationDeallocation() {
		try {
			DefaultInstanceManager cm = new DefaultInstanceManager();
			TestInstanceListener testInstanceListener = new TestInstanceListener();
			cm.setInstanceListener(testInstanceListener);
			
			
			int ipcPort = ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT;
			int dataPort = ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT;

			HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(2, 2L * 1024L * 1024L * 1024L,
																				2L * 1024L * 1024L * 1024L);

			String hostname = "192.168.198.1";
			InetAddress address = InetAddress.getByName("192.168.198.1");
			
			InstanceConnectionInfo ici1 = new InstanceConnectionInfo(address, hostname, null, ipcPort + 0, dataPort + 0);
			InstanceConnectionInfo ici2 = new InstanceConnectionInfo(address, hostname, null, ipcPort + 15, dataPort + 15);
			
			// register three instances
			cm.registerTaskManager(ici1, hardwareDescription, 1);
			cm.registerTaskManager(ici2, hardwareDescription, 1);

			assertEquals(2, cm.getNumberOfSlots());
			
			
			// allocate something
			JobID jobID = new JobID();
			Configuration conf = new Configuration();
			cm.requestInstance(jobID, conf, 2);
			
			DefaultInstanceManagerTestUtils.waitForInstances(jobID, testInstanceListener, 3, 1000);
			
			List<AllocatedResource> allocatedResources = testInstanceListener.getAllocatedResourcesForJob(jobID);
			assertEquals(2, allocatedResources.size());
			
			Iterator<AllocatedResource> it = allocatedResources.iterator();
			Set<AllocationID> allocationIDs = new HashSet<AllocationID>();
			while (it.hasNext()) {
				AllocatedResource allocatedResource = it.next();

				if (allocationIDs.contains(allocatedResource.getAllocationID())) {
					fail("Discovered allocation ID " + allocatedResource.getAllocationID() + " at least twice");
				} else {
					allocationIDs.add(allocatedResource.getAllocationID());
				}
			}

			// Try to allocate more resources which must result in an error
			try {
				cm.requestInstance(jobID, conf, 3);

				fail("ClusterManager allowed to request more instances than actually available");

			} catch (InstanceException ie) {
				// Exception is expected and correct behavior here
			}

			// Release all allocated resources
			it = allocatedResources.iterator();
			while (it.hasNext()) {
				final AllocatedResource allocatedResource = it.next();
				cm.releaseAllocatedResource(allocatedResource);
			}
			
			// Now further allocations should be possible
			
			cm.requestInstance(jobID, conf, 1);
			
			
			cm.shutdown();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test erroneous: " + e.getMessage());
		}
	}

	/**
	 * This test checks the clean-up routines of the cluster manager.
	 */
	@Test
	public void testCleanUp() {
		try {
			
			final int CLEANUP_INTERVAL = 2;

			// configure a short cleanup interval
			Configuration config = new Configuration();
			config.setInteger("instancemanager.cluster.cleanupinterval", CLEANUP_INTERVAL);
			GlobalConfiguration.includeConfiguration(config);

			DefaultInstanceManager cm = new DefaultInstanceManager();
			TestInstanceListener testInstanceListener = new TestInstanceListener();
			cm.setInstanceListener(testInstanceListener);


			int ipcPort = ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT;
			int dataPort = ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT;

			HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(2, 2L * 1024L * 1024L * 1024L,
																				2L * 1024L * 1024L * 1024L);

			String hostname = "192.168.198.1";
			InetAddress address = InetAddress.getByName("192.168.198.1");

			InstanceConnectionInfo ici1 = new InstanceConnectionInfo(address, hostname, null, ipcPort + 0, dataPort + 0);
			InstanceConnectionInfo ici2 = new InstanceConnectionInfo(address, hostname, null, ipcPort + 15, dataPort + 15);
			InstanceConnectionInfo ici3 = new InstanceConnectionInfo(address, hostname, null, ipcPort + 30, dataPort + 30);

			// register three instances
			cm.registerTaskManager(ici1, hardwareDescription, 1);
			cm.registerTaskManager(ici2, hardwareDescription, 1);
			cm.registerTaskManager(ici3, hardwareDescription, 1);

			assertEquals(3, cm.getNumberOfSlots());

			// request some instances
			JobID jobID = new JobID();
			Configuration conf = new Configuration();

			cm.requestInstance(jobID, conf, 1);

			DefaultInstanceManagerTestUtils.waitForInstances(jobID, testInstanceListener, 1, 1000);
			assertEquals(1, testInstanceListener.getNumberOfAllocatedResourcesForJob(jobID));

			// wait for the cleanup to kick in
			Thread.sleep(2000 * CLEANUP_INTERVAL);

			// check that the instances are gone
			DefaultInstanceManagerTestUtils.waitForInstances(jobID, testInstanceListener, 0, 1000);
			assertEquals(0, testInstanceListener.getNumberOfAllocatedResourcesForJob(jobID));


			assertEquals(0, cm.getNumberOfSlots());

			cm.shutdown();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test erroneous: " + e.getMessage());
		}
	}

}
