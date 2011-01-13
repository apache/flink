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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import static org.junit.Assert.*;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.cluster.ClusterManager;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Tests for {@link ClusterManager}.
 * 
 * @author Dominic Battre
 */
public class ClusterManagerTest {

	public static final boolean EXECUTE_LONG_TESTS = false;

	private static final class MyInstanceListener implements InstanceListener {

		private volatile int numberOfAllocatedInstances = 0;

		final Multimap<JobID, AllocatedResource> resourcesOfJobs = HashMultimap.create();

		@Override
		public void allocatedResourceDied(JobID jobID, AllocatedResource allocatedResource) {
			--this.numberOfAllocatedInstances;
			assertTrue(this.numberOfAllocatedInstances >= 0);

			assertTrue(this.resourcesOfJobs.containsEntry(jobID, allocatedResource));
			this.resourcesOfJobs.remove(jobID, allocatedResource);
		}

		@Override
		public void resourceAllocated(JobID jobID, AllocatedResource allocatedResource) {
			assertTrue(this.numberOfAllocatedInstances >= 0);
			++this.numberOfAllocatedInstances;

			assertFalse(this.resourcesOfJobs.containsEntry(jobID, allocatedResource));

			this.resourcesOfJobs.put(jobID, allocatedResource);
		}

		public int getNumberOfAllocatedInstances() {

			return this.numberOfAllocatedInstances;
		}
	};

	@Test
	public void testCleanup() {
	}

	/**
	 * This test covers the parsing of instance types from the configuration and the default instance type.
	 */
	@Test
	public void testInstanceConfiguration() {

		GlobalConfiguration.loadConfiguration(System.getProperty("user.dir") + "/correct-conf");

		final MyInstanceListener dummyInstanceListener = new MyInstanceListener();
		final ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(dummyInstanceListener);
		try {

			final InstanceType defaultIT = cm.getDefaultInstanceType();

			assertNotNull(defaultIT);
			assertEquals("small", defaultIT.getIdentifier());
			assertEquals(2, defaultIT.getNumberOfComputeUnits());
			assertEquals(2, defaultIT.getNumberOfCores());
			assertEquals(2048, defaultIT.getMemorySize());
			assertEquals(10, defaultIT.getDiskCapacity());
			assertEquals(10, defaultIT.getPricePerHour());

		} finally {
			if (cm != null) {
				cm.shutdown();
			}
		}
	}

	@Test
	public void testInstancePattern() {
		Pattern pattern = Pattern.compile("^([^,]+),(\\d+),(\\d+),(\\d+),(\\d+),(\\d+)$");
		Matcher m = pattern.matcher("m1.small,2,1,2048,10,10");
		assertEquals(6, m.groupCount());
		assertTrue(m.matches());
		assertEquals("m1.small", m.group(1));
	}

	/**
	 * This test checks the correctness of extracting instance types from the configuration, mapping IPs to instance
	 * types from the slave file
	 * 
	 * @throws InstanceException
	 */
	@Test
	public void testAllocationDeallocation() {
		GlobalConfiguration.loadConfiguration(System.getProperty("user.dir") + "/correct-conf");

		MyInstanceListener dummyInstanceListener = new MyInstanceListener();
		ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(dummyInstanceListener);
		InetAddress inetAddress = null;

		try {
			try {
				inetAddress = InetAddress.getByName("192.168.198.1");
			} catch (UnknownHostException e) {
				fail(e.getMessage());
			}
			final InstanceConnectionInfo instanceConnectionInfo = new InstanceConnectionInfo(inetAddress, 1234, 1235);
			final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(8,
				32L * 1024L * 1024L * 1024L, 32L * 1024L * 1024L * 1024L);
			cm.reportHeartBeat(instanceConnectionInfo, hardwareDescription);
			// now we should be able to request 4 m1.large instances
			final JobID jobID = new JobID();
			for (int i = 0; i < 4; ++i) {
				try {
					cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
				} catch (InstanceException e) {
					fail(e.getMessage());
				}
			}

			waitForInstanceArrival(dummyInstanceListener, 4, 1000);

			// all 4 are registered
			assertEquals(4, dummyInstanceListener.resourcesOfJobs.get(jobID).size());

			try {
				cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
				assertTrue(false); // we cannot get a 5th one
			} catch (InstanceException e) {
				assertTrue(true); // we expect the exception
			}

			final List<AllocatedResource> resources = new ArrayList<AllocatedResource>(
				dummyInstanceListener.resourcesOfJobs.get(jobID));

			for (AllocatedResource i : resources) {
				try {
					cm.releaseAllocatedResource(jobID, new Configuration(), i);
				} catch (InstanceException e) {
					fail(e.getMessage());
				}
			}

			// none are registered but they are not marked as dead
			assertEquals(4, dummyInstanceListener.resourcesOfJobs.get(jobID).size());

			// however, we can create new ones
			for (int i = 0; i < 4; ++i) {
				try {
					cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
				} catch (InstanceException e) {
					fail(e.getMessage());
				}
			}

			// wait for all threads to report instance availability
			try {
				Thread.sleep(500);
			} catch(InterruptedException e) {
				e.printStackTrace();
			}

			// all 4+4 are registered
			assertEquals(8, dummyInstanceListener.resourcesOfJobs.get(jobID).size());

		} finally {
			if (cm != null) {
				cm.shutdown();
			}
		}
	}

	private void waitForInstanceArrival(MyInstanceListener instanceListener, int numberOfInstances, long maxWaitTime) {

		final long startTime = System.currentTimeMillis();
		while (instanceListener.getNumberOfAllocatedInstances() < numberOfInstances) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				break;
			}

			if ((System.currentTimeMillis() - startTime) >= maxWaitTime) {
				break;
			}
		}
	}

	@Test(timeout = 30 * 1000)
	public void testTimeout() throws InstanceException {
		if (!EXECUTE_LONG_TESTS) {
			System.err.println("Skipping test testTimeout");
			return;
		}

		GlobalConfiguration.loadConfiguration(System.getProperty("user.dir") + "/correct-conf");

		Configuration overwrite = new Configuration();
		overwrite.setInteger("cloud.ec2.cleanupinterval", 10000);
		GlobalConfiguration.includeConfiguration(overwrite);

		MyInstanceListener dummyInstanceListener = new MyInstanceListener();
		ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(dummyInstanceListener);
		try {
			InetAddress inetAddress = null;
			try {
				inetAddress = InetAddress.getByName("localhost");
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			InstanceConnectionInfo instanceConnectionInfo = new InstanceConnectionInfo(inetAddress, 1234, 1235);
			final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(8,
				32L * 1024L * 1024L * 1024L, 32L * 1024L * 1024L * 1024L);
			cm.reportHeartBeat(instanceConnectionInfo, hardwareDescription);
			// now we should be able to request 4 m1.large instances
			JobID jobID = new JobID();
			for (int i = 0; i < 4; ++i) {
				cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
			}

			// all 4 are registered
			assertEquals(4, dummyInstanceListener.resourcesOfJobs.get(jobID).size());

			try {
				cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
				assertTrue(false); // we cannot get a 5th one
			} catch (InstanceException e) {
				assertTrue(true); // we expect the exception
			}

			List<AllocatedResource> resources = new ArrayList<AllocatedResource>(
				dummyInstanceListener.resourcesOfJobs.get(jobID));

			// now we wait for 25 seconds -> the instances should be gone

			try {
				Thread.sleep(25 * 1000);
			} catch (InterruptedException e1) {
			}

			// none are registered, all dead
			assertEquals(0, dummyInstanceListener.resourcesOfJobs.get(jobID).size());

			try {
				cm.releaseAllocatedResource(jobID, new Configuration(), resources.get(0));
				// we cannot unregister
				assertTrue(false);
			} catch (Exception e) {
				assertTrue(true);
			}

			try {
				cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
				assertTrue(false); // we cannot register a new instance as all hosts are dead
			} catch (InstanceException e) {
				assertTrue(true); // we expect the exception
			}

		} finally {
			if (cm != null) {
				cm.shutdown();
			}
		}
	}

}
