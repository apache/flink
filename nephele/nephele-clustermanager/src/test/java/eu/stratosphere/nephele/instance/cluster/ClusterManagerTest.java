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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import static org.junit.Assert.*;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.cluster.ClusterManager;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Tests for {@link ClusterManager}.
 * 
 * @author Dominic Battre
 */
public class ClusterManagerTest {

	private static final class MyInstanceListener implements InstanceListener {

		final Map<JobID, List<AllocatedResource>> resourcesOfJobs = new HashMap<JobID, List<AllocatedResource>>();

		@Override
		public void allocatedResourceDied(JobID jobID, AllocatedResource allocatedResource) {
			/*--this.numberOfAllocatedInstances;
			assertTrue(this.numberOfAllocatedInstances >= 0);

			assertTrue(this.resourcesOfJobs.containsEntry(jobID, allocatedResource));
			this.resourcesOfJobs.remove(jobID, allocatedResource);*/
		}

		@Override
		public void resourceAllocated(JobID jobID, AllocatedResource allocatedResource) {
			/*
			 * assertTrue(this.numberOfAllocatedInstances >= 0);
			 * ++this.numberOfAllocatedInstances;
			 * assertFalse(this.resourcesOfJobs.containsEntry(jobID, allocatedResource));
			 * this.resourcesOfJobs.put(jobID, allocatedResource);
			 */
		}
	};

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
	 * This test covers the parsing of instance types from the configuration and the default instance type.
	 */
	@Test
	public void testInstanceConfiguration() {

		GlobalConfiguration.loadConfiguration(System.getProperty(USER_DIR_KEY) + CORRECT_CONF_DIR);

		final MyInstanceListener dummyInstanceListener = new MyInstanceListener();
		final ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(dummyInstanceListener);
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
			final List<InstanceTypeDescription> instanceTypeDescriptions = cm.getListOfAvailableInstanceTypes();
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

		final MyInstanceListener dummyInstanceListener = new MyInstanceListener();
		final ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(dummyInstanceListener);

		List<InstanceTypeDescription> instanceTypeDescriptions = null;

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

			instanceTypeDescriptions = cm.getListOfAvailableInstanceTypes();
			assertEquals(3, instanceTypeDescriptions.size());
			assertTrue(LARGE_INSTANCE_TYPE_NAME.equals(instanceTypeDescriptions.get(0).getInstanceType().getIdentifier()));
			assertTrue(MEDIUM_INSTANCE_TYPE_NAME.equals(instanceTypeDescriptions.get(1).getInstanceType().getIdentifier()));
			assertTrue(SMALL_INSTANCE_TYPE_NAME.equals(instanceTypeDescriptions.get(2).getInstanceType().getIdentifier()));

			assertEquals(1, instanceTypeDescriptions.get(0).getMaximumNumberOfAvailableInstances());
			assertEquals(2, instanceTypeDescriptions.get(1).getMaximumNumberOfAvailableInstances());
			assertEquals(4, instanceTypeDescriptions.get(2).getMaximumNumberOfAvailableInstances());

			// Now we add a second cluster instance which is expected to identified as of type "small" as a result of
			// its hardware description
			hardwareDescription = HardwareDescriptionFactory.construct(3, 2L * 1024L * 1024L * 1024L,
				1024L * 1024L * 1024L);
			ici = new InstanceConnectionInfo(InetAddress.getByName("192.168.198.3"), ipcPort, dataPort);
			cm.reportHeartBeat(ici, hardwareDescription);

			instanceTypeDescriptions = cm.getListOfAvailableInstanceTypes();

			assertEquals(3, instanceTypeDescriptions.size());
			assertTrue(LARGE_INSTANCE_TYPE_NAME.equals(instanceTypeDescriptions.get(0).getInstanceType().getIdentifier()));
			assertTrue(MEDIUM_INSTANCE_TYPE_NAME.equals(instanceTypeDescriptions.get(1).getInstanceType().getIdentifier()));
			assertTrue(SMALL_INSTANCE_TYPE_NAME.equals(instanceTypeDescriptions.get(2).getInstanceType().getIdentifier()));

			assertEquals(1, instanceTypeDescriptions.get(0).getMaximumNumberOfAvailableInstances());
			assertEquals(2, instanceTypeDescriptions.get(1).getMaximumNumberOfAvailableInstances());
			assertEquals(5, instanceTypeDescriptions.get(2).getMaximumNumberOfAvailableInstances());

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
		MyInstanceListener dummyInstanceListener = new MyInstanceListener();
		ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(dummyInstanceListener);

		try {

			final InstanceConnectionInfo instanceConnectionInfo = new InstanceConnectionInfo(
				InetAddress.getByName("192.168.198.1"), 1234, 1235);
			final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(8,
				32L * 1024L * 1024L * 1024L, 32L * 1024L * 1024L * 1024L);
			cm.reportHeartBeat(instanceConnectionInfo, hardwareDescription);

			// now we should be able to request two instances of type small and one of type medium
			final JobID jobID = new JobID();
			final Configuration conf = new Configuration();

			try {
				cm.requestInstance(jobID, conf, cm.getInstanceTypeByName(SMALL_INSTANCE_TYPE_NAME));
				cm.requestInstance(jobID, conf, cm.getInstanceTypeByName(SMALL_INSTANCE_TYPE_NAME));
				//TODO: Go on here
				
			} catch (InstanceException ie) {
				fail(ie.getMessage());
			}

			/*
			 * for (int i = 0; i < 4; ++i) {
			 * try {
			 * cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
			 * } catch (InstanceException e) {
			 * fail(e.getMessage());
			 * }
			 * }
			 * waitForInstanceArrival(dummyInstanceListener, 4, 1000);
			 * // all 4 are registered
			 * assertEquals(4, dummyInstanceListener.resourcesOfJobs.get(jobID).size());
			 * try {
			 * cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
			 * assertTrue(false); // we cannot get a 5th one
			 * } catch (InstanceException e) {
			 * assertTrue(true); // we expect the exception
			 * }
			 * final List<AllocatedResource> resources = new ArrayList<AllocatedResource>(
			 * dummyInstanceListener.resourcesOfJobs.get(jobID));
			 * for (AllocatedResource i : resources) {
			 * try {
			 * cm.releaseAllocatedResource(jobID, new Configuration(), i);
			 * } catch (InstanceException e) {
			 * fail(e.getMessage());
			 * }
			 * }
			 * // none are registered but they are not marked as dead
			 * assertEquals(4, dummyInstanceListener.resourcesOfJobs.get(jobID).size());
			 * // however, we can create new ones
			 * for (int i = 0; i < 4; ++i) {
			 * try {
			 * cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
			 * } catch (InstanceException e) {
			 * fail(e.getMessage());
			 * }
			 * }
			 * // wait for all threads to report instance availability
			 * try {
			 * Thread.sleep(500);
			 * } catch (InterruptedException e) {
			 * e.printStackTrace();
			 * }
			 */
			// all 4+4 are registered
			// assertEquals(8, dummyInstanceListener.resourcesOfJobs.get(jobID).size());
		} catch (UnknownHostException e) {
			fail(e.getMessage());
		} finally {
			if (cm != null) {
				cm.shutdown();
			}
		}
	}

	/*
	 * private void waitForInstanceArrival(MyInstanceListener instanceListener, int numberOfInstances, long maxWaitTime)
	 * {
	 * final long startTime = System.currentTimeMillis();
	 * while (instanceListener.getNumberOfAllocatedInstances() < numberOfInstances) {
	 * try {
	 * Thread.sleep(100);
	 * } catch (InterruptedException e) {
	 * break;
	 * }
	 * if ((System.currentTimeMillis() - startTime) >= maxWaitTime) {
	 * break;
	 * }
	 * }
	 * }
	 */

	/*
	 * @Test(timeout = 30 * 1000)
	 * public void testTimeout() throws InstanceException {
	 * if (!EXECUTE_LONG_TESTS) {
	 * System.err.println("Skipping test testTimeout");
	 * return;
	 * }
	 * GlobalConfiguration.loadConfiguration(System.getProperty("user.dir") + "/correct-conf");
	 * Configuration overwrite = new Configuration();
	 * overwrite.setInteger("cloud.ec2.cleanupinterval", 10000);
	 * GlobalConfiguration.includeConfiguration(overwrite);
	 * MyInstanceListener dummyInstanceListener = new MyInstanceListener();
	 * ClusterManager cm = new ClusterManager();
	 * cm.setInstanceListener(dummyInstanceListener);
	 * try {
	 * InetAddress inetAddress = null;
	 * try {
	 * inetAddress = InetAddress.getByName("localhost");
	 * } catch (UnknownHostException e) {
	 * e.printStackTrace();
	 * }
	 * InstanceConnectionInfo instanceConnectionInfo = new InstanceConnectionInfo(inetAddress, 1234, 1235);
	 * final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(8,
	 * 32L * 1024L * 1024L * 1024L, 32L * 1024L * 1024L * 1024L);
	 * cm.reportHeartBeat(instanceConnectionInfo, hardwareDescription);
	 * // now we should be able to request 4 m1.large instances
	 * JobID jobID = new JobID();
	 * for (int i = 0; i < 4; ++i) {
	 * cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
	 * }
	 * // all 4 are registered
	 * assertEquals(4, dummyInstanceListener.resourcesOfJobs.get(jobID).size());
	 * try {
	 * cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
	 * assertTrue(false); // we cannot get a 5th one
	 * } catch (InstanceException e) {
	 * assertTrue(true); // we expect the exception
	 * }
	 * List<AllocatedResource> resources = new ArrayList<AllocatedResource>(
	 * dummyInstanceListener.resourcesOfJobs.get(jobID));
	 * // now we wait for 25 seconds -> the instances should be gone
	 * try {
	 * Thread.sleep(25 * 1000);
	 * } catch (InterruptedException e1) {
	 * }
	 * // none are registered, all dead
	 * assertEquals(0, dummyInstanceListener.resourcesOfJobs.get(jobID).size());
	 * try {
	 * cm.releaseAllocatedResource(jobID, new Configuration(), resources.get(0));
	 * // we cannot unregister
	 * assertTrue(false);
	 * } catch (Exception e) {
	 * assertTrue(true);
	 * }
	 * try {
	 * cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
	 * assertTrue(false); // we cannot register a new instance as all hosts are dead
	 * } catch (InstanceException e) {
	 * assertTrue(true); // we expect the exception
	 * }
	 * } finally {
	 * if (cm != null) {
	 * cm.shutdown();
	 * }
	 * }
	 * }
	 */

}
