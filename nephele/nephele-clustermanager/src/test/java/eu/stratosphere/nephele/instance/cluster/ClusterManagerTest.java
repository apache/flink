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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.*;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.cluster.ClusterManager;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Tests for {@link ClusterManager}.
 * 
 * @author warneke
 */
public class ClusterManagerTest {

	/**
	 * The system property key to retrieve the user directory.
	 */
	private static final String USER_DIR_KEY = "user.dir";

	/**
	 * The directory containing the correct configuration file to be used during the tests.
	 */
	private static final String CORRECT_CONF_DIR = "/correct-conf";

	/**
	 * The name of the small instance type.
	 */
	private static final String SMALL_INSTANCE_TYPE_NAME = "small";

	/**
	 * The name of the medium instance type.
	 */
	private static final String MEDIUM_INSTANCE_TYPE_NAME = "medium";

	/**
	 * The name of the large instance type.
	 */
	private static final String LARGE_INSTANCE_TYPE_NAME = "large";

	/**
	 * The maximum time to wait for instance arrivals in milliseconds.
	 */
	private static final long MAX_WAIT_TIME = 1000L;

	/**
	 * The clean up interval in milliseconds.
	 */
	private static final long CLEAN_UP_INTERVAL = 5000L;

	/**
	 * This test covers the parsing of instance types from the configuration and the default instance type.
	 */
	@Test
	public void testInstanceConfiguration() {

		GlobalConfiguration.loadConfiguration(System.getProperty(USER_DIR_KEY) + CORRECT_CONF_DIR);

		final TestInstanceListener testInstanceListener = new TestInstanceListener();
		final ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(testInstanceListener);
		try {

			final InstanceType defaultIT = cm.getDefaultInstanceType();

			assertNotNull(defaultIT);
			assertEquals(SMALL_INSTANCE_TYPE_NAME, defaultIT.getIdentifier());
			assertEquals(2, defaultIT.getNumberOfComputeUnits());
			assertEquals(2, defaultIT.getNumberOfCores());
			assertEquals(2048, defaultIT.getMemorySize());
			assertEquals(10, defaultIT.getDiskCapacity());
			assertEquals(10, defaultIT.getPricePerHour());

			// Check number of different instance types available to cluster manager
			final Map<InstanceType, InstanceTypeDescription> instanceTypeDescriptions = cm
				.getMapOfAvailableInstanceTypes();
			assertEquals(3, instanceTypeDescriptions.size());

		} finally {
			if (cm != null) {
				cm.shutdown();
			}
		}
	}

	/**
	 * This test covers the matching of instances to instance types It addresses the automatic matching through the
	 * hardware description as well as user-defined instance type matching.
	 */
	@Test
	public void testInstanceMatching() {

		GlobalConfiguration.loadConfiguration(System.getProperty(USER_DIR_KEY) + CORRECT_CONF_DIR);

		final TestInstanceListener testInstanceListener = new TestInstanceListener();
		final ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(testInstanceListener);

		Map<InstanceType, InstanceTypeDescription> instanceTypeDescriptions = null;

		try {

			final int ipcPort = ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT;
			final int dataPort = ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT;

			HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(2,
				2L * 1024L * 1024L * 1024L,
				2L * 1024L * 1024L * 1024L);

			InstanceConnectionInfo ici = new InstanceConnectionInfo(InetAddress.getByName("192.168.198.1"), ipcPort,
				dataPort);

			// Although the hardware description indicates an instance of type "small", the cluster manager is supposed
			// to take the user-defined instance type "high"
			cm.reportHeartBeat(ici, hardwareDescription);

			instanceTypeDescriptions = cm.getMapOfAvailableInstanceTypes();
			assertEquals(3, instanceTypeDescriptions.size());

			Iterator<Map.Entry<InstanceType, InstanceTypeDescription>> it = instanceTypeDescriptions.entrySet()
				.iterator();

			while (it.hasNext()) {

				final Map.Entry<InstanceType, InstanceTypeDescription> entry = it.next();

				if (LARGE_INSTANCE_TYPE_NAME.equals(entry.getKey().getIdentifier())) {
					assertEquals(1, entry.getValue().getMaximumNumberOfAvailableInstances());
				} else if (MEDIUM_INSTANCE_TYPE_NAME.equals(entry.getKey().getIdentifier())) {
					assertEquals(2, entry.getValue().getMaximumNumberOfAvailableInstances());
				} else if (SMALL_INSTANCE_TYPE_NAME.equals(entry.getKey().getIdentifier())) {
					assertEquals(4, entry.getValue().getMaximumNumberOfAvailableInstances());
				} else {
					fail("Unexpected instance type: " + entry.getKey());
				}
			}

			// Now we add a second cluster instance which is expected to identified as of type "small" as a result of
			// its hardware description
			hardwareDescription = HardwareDescriptionFactory.construct(3, 2L * 1024L * 1024L * 1024L,
				1024L * 1024L * 1024L);
			ici = new InstanceConnectionInfo(InetAddress.getByName("192.168.198.3"), ipcPort, dataPort);
			cm.reportHeartBeat(ici, hardwareDescription);

			instanceTypeDescriptions = cm.getMapOfAvailableInstanceTypes();

			assertEquals(3, instanceTypeDescriptions.size());

			it = instanceTypeDescriptions.entrySet().iterator();

			while (it.hasNext()) {

				final Map.Entry<InstanceType, InstanceTypeDescription> entry = it.next();

				if (LARGE_INSTANCE_TYPE_NAME.equals(entry.getKey().getIdentifier())) {
					assertEquals(1, entry.getValue().getMaximumNumberOfAvailableInstances());
				} else if (MEDIUM_INSTANCE_TYPE_NAME.equals(entry.getKey().getIdentifier())) {
					assertEquals(2, entry.getValue().getMaximumNumberOfAvailableInstances());
				} else if (SMALL_INSTANCE_TYPE_NAME.equals(entry.getKey().getIdentifier())) {
					assertEquals(5, entry.getValue().getMaximumNumberOfAvailableInstances());
				} else {
					fail("Unexpected instance type: " + entry.getKey());
				}

			}

		} catch (UnknownHostException e) {
			fail(e.getMessage());
		} finally {
			if (cm != null) {
				cm.shutdown();
			}
		}
	}

	/**
	 * This test checks the correctness of extracting instance types from the configuration, mapping IPs to instance
	 * types from the slave file, instance slicing and allocation/deallocation.
	 */
	@Test
	public void testAllocationDeallocation() {

		GlobalConfiguration.loadConfiguration(System.getProperty(USER_DIR_KEY) + CORRECT_CONF_DIR);
		final TestInstanceListener testInstanceListener = new TestInstanceListener();
		final ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(testInstanceListener);

		try {

			final InstanceConnectionInfo instanceConnectionInfo = new InstanceConnectionInfo(
				InetAddress.getByName("192.168.198.1"), 1234, 1235);
			final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(8,
				8L * 1024L * 1024L * 1024L, 8L * 1024L * 1024L * 1024L);
			cm.reportHeartBeat(instanceConnectionInfo, hardwareDescription);

			// now we should be able to request two instances of type small and one of type medium
			final JobID jobID = new JobID();
			final Configuration conf = new Configuration();

			try {
				cm.requestInstance(jobID, conf, cm.getInstanceTypeByName(SMALL_INSTANCE_TYPE_NAME));
				cm.requestInstance(jobID, conf, cm.getInstanceTypeByName(SMALL_INSTANCE_TYPE_NAME));
				cm.requestInstance(jobID, conf, cm.getInstanceTypeByName(MEDIUM_INSTANCE_TYPE_NAME));

			} catch (InstanceException ie) {
				fail(ie.getMessage());
			}

			ClusterManagerTestUtils.waitForInstances(jobID, testInstanceListener, 3, MAX_WAIT_TIME);

			final List<AllocatedResource> allocatedResources = testInstanceListener.getAllocatedResourcesForJob(jobID);
			assertEquals(3, allocatedResources.size());
			Iterator<AllocatedResource> it = allocatedResources.iterator();
			final Set<AllocationID> allocationIDs = new HashSet<AllocationID>();
			while (it.hasNext()) {
				final AllocatedResource allocatedResource = it.next();
				if (!LARGE_INSTANCE_TYPE_NAME.equals(allocatedResource.getInstance().getType().getIdentifier())) {
					fail("Allocated unexpected instance of type "
						+ allocatedResource.getInstance().getType().getIdentifier());
				}

				if (allocationIDs.contains(allocatedResource.getAllocationID())) {
					fail("Discovered allocation ID " + allocatedResource.getAllocationID() + " at least twice");
				} else {
					allocationIDs.add(allocatedResource.getAllocationID());
				}
			}

			// Try to allocate more resources which must result in an error
			try {

				cm.requestInstance(jobID, conf, cm.getInstanceTypeByName(MEDIUM_INSTANCE_TYPE_NAME));

				fail("ClusterManager allowed to request more instances than actually available");

			} catch (InstanceException ie) {
				// Exception is expected and correct behavior here
			}

			// Release all allocated resources
			it = allocatedResources.iterator();
			try {
				while (it.hasNext()) {
					final AllocatedResource allocatedResource = it.next();
					cm.releaseAllocatedResource(jobID, conf, allocatedResource);
				}
			} catch (InstanceException ie) {
				fail(ie.getMessage());
			}

			// Now further allocations should be possible
			try {
				cm.requestInstance(jobID, conf, cm.getInstanceTypeByName(LARGE_INSTANCE_TYPE_NAME));
			} catch (InstanceException ie) {
				fail(ie.getMessage());
			}

		} catch (UnknownHostException e) {
			fail(e.getMessage());
		} finally {
			if (cm != null) {
				cm.shutdown();
			}
		}
	}

	/**
	 * This test checks the clean-up routines of the cluster manager.
	 */
	@Test
	public void testCleanUp() {

		GlobalConfiguration.loadConfiguration(System.getProperty(USER_DIR_KEY) + CORRECT_CONF_DIR);

		final TestInstanceListener testInstanceListener = new TestInstanceListener();
		final ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(testInstanceListener);

		try {

			final InstanceConnectionInfo instanceConnectionInfo = new InstanceConnectionInfo(
				InetAddress.getByName("192.168.198.3"), 1234, 1235);
			final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(8,
				8L * 1024L * 1024L * 1024L, 8L * 1024L * 1024L * 1024L);
			cm.reportHeartBeat(instanceConnectionInfo, hardwareDescription);

			final JobID jobID = new JobID();
			final Configuration conf = new Configuration();

			try {

				cm.requestInstance(jobID, conf, cm.getInstanceTypeByName(LARGE_INSTANCE_TYPE_NAME));

			} catch (InstanceException ie) {
				fail(ie.getMessage());
			}

			ClusterManagerTestUtils.waitForInstances(jobID, testInstanceListener, 1, MAX_WAIT_TIME);
			assertEquals(1, testInstanceListener.getNumberOfAllocatedResourcesForJob(jobID));

			try {
				Thread.sleep(CLEAN_UP_INTERVAL);
			} catch (InterruptedException ie) {
				fail(ie.getMessage());
			}

			ClusterManagerTestUtils.waitForInstances(jobID, testInstanceListener, 0, MAX_WAIT_TIME);
			assertEquals(0, testInstanceListener.getNumberOfAllocatedResourcesForJob(jobID));

		} catch (UnknownHostException e) {
			fail(e.getMessage());
		} finally {
			if (cm != null) {
				cm.shutdown();
			}
		}
	}
}
