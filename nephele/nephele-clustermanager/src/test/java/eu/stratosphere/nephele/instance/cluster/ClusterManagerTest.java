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

	public static boolean EXECUTE_LONG_TESTS = false;

	private static final class MyInstanceListener implements InstanceListener {
		int nrAvailable = 0;

		final Multimap<JobID, AllocatedResource> resourcesOfJobs = HashMultimap.create();

		@Override
		public void allocatedResourceDied(JobID jobID, AllocatedResource allocatedResource) {
			--nrAvailable;
			assertTrue(nrAvailable >= 0);

			assertTrue(resourcesOfJobs.containsEntry(jobID, allocatedResource));
			resourcesOfJobs.remove(jobID, allocatedResource);
		}

		@Override
		public void resourceAllocated(JobID jobID, AllocatedResource allocatedResource) {
			assertTrue(nrAvailable >= 0);
			++nrAvailable;

			assertFalse(resourcesOfJobs.containsEntry(jobID, allocatedResource));

			resourcesOfJobs.put(jobID, allocatedResource);
		}
	};

	@Test
	public void testCleanup() {
	}

	@Test
	public void testInstanceConfiguration() {
		// this verifies:
		// - parsing of configuration
		// - getDefaultInstanceType
		// - getInstanceTypeByName
		GlobalConfiguration.loadConfiguration(System.getProperty("user.dir") + "/correct-conf");

		MyInstanceListener dummyInstanceListener = new MyInstanceListener();
		ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(dummyInstanceListener);
		try {

			InstanceType defaultIT = cm.getDefaultInstanceType();

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

	@Test
	public void testAllocationDeallocation() throws InstanceException {
		GlobalConfiguration.loadConfiguration(System.getProperty("user.dir") + "/correct-conf");

		MyInstanceListener dummyInstanceListener = new MyInstanceListener();
		ClusterManager cm = new ClusterManager();
		cm.setInstanceListener(dummyInstanceListener);
		try {
			InetAddress inetAddress = null;
			try {
				inetAddress = InetAddress.getByName("192.168.198.1");
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			InstanceConnectionInfo instanceConnectionInfo = new InstanceConnectionInfo(inetAddress, 1234, 1235);
			cm.reportHeartBeat(instanceConnectionInfo);
			// now we should be able to request 4 m1.large instances
			JobID jobID = new JobID();
			for (int i = 0; i < 4; ++i) {
				cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
			}

			// wait for all threads to report instance availability
			Thread.sleep(500);

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

			for (AllocatedResource i : resources) {
				cm.releaseAllocatedResource(jobID, new Configuration(), i);
			}

			// none are registered but they are not maked as dead
			assertEquals(4, dummyInstanceListener.resourcesOfJobs.get(jobID).size());

			// however, we can create new ones
			for (int i = 0; i < 4; ++i) {
				cm.requestInstance(jobID, new Configuration(), cm.getDefaultInstanceType());
			}

			// wait for all threads to report instance availability
			Thread.sleep(500);

			// all 4+4 are registered
			assertEquals(8, dummyInstanceListener.resourcesOfJobs.get(jobID).size());

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			if (cm != null) {
				cm.shutdown();
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
			cm.reportHeartBeat(instanceConnectionInfo);
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
