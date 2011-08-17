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

package eu.stratosphere.nephele.instance.ec2;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceRequestMap;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.nephele.instance.ec2.EC2CloudInstance;
import eu.stratosphere.nephele.instance.ec2.EC2CloudManager;
import eu.stratosphere.nephele.instance.ec2.FloatingInstance;
import eu.stratosphere.nephele.instance.ec2.JobToInstancesMapping;
import eu.stratosphere.nephele.jobgraph.JobID;

public class EC2CloudManagerTest {

	private static final class MyInstanceListener implements InstanceListener {

		int nrAvailable = 0;

		final Map<JobID, List<AllocatedResource>> resourcesOfJobs = new HashMap<JobID, List<AllocatedResource>>();

		@Override
		public void allocatedResourcesDied(final JobID jobID, final List<AllocatedResource> allocatedResources) {

			final List<AllocatedResource> resourcesOfJob = this.resourcesOfJobs.get(jobID);
			assertTrue(resourcesOfJob != null);
			assertTrue(resourcesOfJob.contains(allocatedResource));
			resourcesOfJob.remove(allocatedResource);
			if (resourcesOfJob.isEmpty()) {
				this.resourcesOfJobs.remove(jobID);
			}
		}

		@Override
		public void resourcesAllocated(final JobID jobID, final List<AllocatedResource> allocatedResources) {

			assertTrue(nrAvailable >= 0);
			++nrAvailable;
			List<AllocatedResource> resourcesOfJob = this.resourcesOfJobs.get(jobID);
			if (resourcesOfJob == null) {
				resourcesOfJob = new ArrayList<AllocatedResource>();
				this.resourcesOfJobs.put(jobID, resourcesOfJob);
			}

			for (final AllocatedResource allocatedResource : allocatedResources) {
				assertFalse(resourcesOfJob.contains(allocatedResource));
				resourcesOfJob.add(allocatedResource);
			}
		}
	};

	@Test
	public void testLoadConf() {

		GlobalConfiguration.loadConfiguration(System.getProperty("user.dir") + "/correct-conf");

		assertEquals("m1.small,1,1,2048,40,10", GlobalConfiguration.getString("instancemanager.ec2.type.1", null));
		assertEquals("c1.medium,2,2,4096,80,20", GlobalConfiguration.getString("instancemanager.ec2.type.2", null));
		assertEquals("m1.large,4,4,6144,160,40", GlobalConfiguration.getString("instancemanager.ec2.type.3", null));
		assertEquals("m1.xlarge,4,4,12288,160,60", GlobalConfiguration.getString("instancemanager.ec2.type.4", null));
		assertEquals("c1.xlarge,8,8,28672,280,80", GlobalConfiguration.getString("instancemanager.ec2.type.5", null));
		assertEquals(1, GlobalConfiguration.getInteger("instancemanager.ec2.defaulttype", -1));
	}

	@Test
	public void testInstancePattern() {

		Pattern pattern = Pattern.compile("^([^,]+),(\\d+),(\\d+),(\\d+),(\\d+),(\\d+)$");
		Matcher m = pattern.matcher("m1.small,1,1,2048,40,10");

		assertTrue(m.matches());
		assertEquals(6, m.groupCount());
		assertEquals("m1.small", m.group(1));
		assertEquals("1", m.group(2));
		assertEquals("1", m.group(3));
		assertEquals("2048", m.group(4));
		assertEquals("40", m.group(5));
		assertEquals("10", m.group(6));
	}

	@Test
	public void testGetDefaultInstance() {

		GlobalConfiguration.loadConfiguration(System.getProperty("user.dir") + "/correct-conf");

		MyInstanceListener myInstanceListener = new MyInstanceListener();
		EC2CloudManager cm = new EC2CloudManager();
		cm.setInstanceListener(myInstanceListener);

		InstanceType defaultIT = cm.getDefaultInstanceType();

		assertNotNull(defaultIT);
		assertEquals("m1.small", defaultIT.getIdentifier());
		assertEquals(1, defaultIT.getNumberOfComputeUnits());
		assertEquals(1, defaultIT.getNumberOfCores());
		assertEquals(2048, defaultIT.getMemorySize());
		assertEquals(40, defaultIT.getDiskCapacity());
		assertEquals(10, defaultIT.getPricePerHour());
	}

	@Test
	public void testGetSuitableInstanceType() {

		GlobalConfiguration.loadConfiguration(System.getProperty("user.dir") + "/correct-conf");

		MyInstanceListener myInstanceListener = new MyInstanceListener();
		EC2CloudManager cm = new EC2CloudManager();
		cm.setInstanceListener(myInstanceListener);

		InstanceType type1 = cm.getSuitableInstanceType(16, 16, 2048, 40, 80);
		InstanceType type2 = cm.getSuitableInstanceType(2, 2, 2048, 40, 10);
		InstanceType type3 = cm.getSuitableInstanceType(2, 2, 6144, 100, 50);
		InstanceType type4 = cm.getSuitableInstanceType(2, 2, 12288, 280, 100);

		assertNull(type1);
		assertNull(type2);

		assertEquals("m1.large", type3.getIdentifier());
		assertEquals(4, type3.getNumberOfComputeUnits());
		assertEquals(4, type3.getNumberOfCores());
		assertEquals(6144, type3.getMemorySize());
		assertEquals(160, type3.getDiskCapacity());
		assertEquals(40, type3.getPricePerHour());

		assertEquals("c1.xlarge", type4.getIdentifier());
		assertEquals(8, type4.getNumberOfComputeUnits());
		assertEquals(8, type4.getNumberOfCores());
		assertEquals(28672, type4.getMemorySize());
		assertEquals(280, type4.getDiskCapacity());
		assertEquals(80, type4.getPricePerHour());
	}

	@Test
	public void testGetInstanceTypeByName() {

		GlobalConfiguration.loadConfiguration(System.getProperty("user.dir") + "/correct-conf");

		MyInstanceListener myInstanceListener = new MyInstanceListener();
		EC2CloudManager cm = new EC2CloudManager();
		cm.setInstanceListener(myInstanceListener);

		InstanceType type = cm.getInstanceTypeByName("m1.small");

		assertEquals("m1.small", type.getIdentifier());
		assertEquals(1, type.getNumberOfComputeUnits());
		assertEquals(1, type.getNumberOfCores());
		assertEquals(2048, type.getMemorySize());
		assertEquals(40, type.getDiskCapacity());
		assertEquals(10, type.getPricePerHour());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRequestReleaseDestroyInstanceAndHeartBeat() {

		GlobalConfiguration.loadConfiguration(System.getProperty("user.dir") + "/correct-conf");

		MyInstanceListener myInstanceListener = new MyInstanceListener();
		EC2CloudManager cm = new EC2CloudManager();
		cm.setInstanceListener(myInstanceListener);

		JobID jobID = new JobID();
		Configuration conf = new Configuration();

		// check whether EC2 account XML file exists
		File f = new File(System.getProperty("user.dir") + "/correct-conf/ec2-account.xml");
		if (!f.exists()) {
			System.err.println("Please create an XML file \"ec2-account.xml\" for EC2 account in the folder "
				+ System.getProperty("user.dir") + "/correct-conf\n"
				+ "Three keys must be included: , job.ec2.awsaccessid, job.ec2.awssecretkey\n"
				+ "The format is:\n" + "<property>\n" + "	<key>...</key>\n" + "	<value>...</value>\n" + "</property>");
			return;
		}

		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(System.getProperty("user.dir") + "/correct-conf/ec2-account.xml");
			NodeList nl = doc.getElementsByTagName("property");

			for (int i = 0; i < nl.getLength(); i++) {

				Element property = (Element) nl.item(i);
				Node nodeKey = property.getElementsByTagName("key").item(0);
				Node nodeValue = property.getElementsByTagName("value").item(0);
				String key = nodeKey.getFirstChild().getNodeValue();
				String value = nodeValue.getFirstChild().getNodeValue();
				conf.setString(key, value);
			}

		} catch (ParserConfigurationException e1) {
			e1.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (conf.getString(EC2CloudManager.AWS_ACCESS_ID_KEY, null) == null) {
			System.err.println("Please set the key " + EC2CloudManager.AWS_ACCESS_ID_KEY + " in "
				+ System.getProperty("user.dir")
				+ "/correct-conf/ec2-account.xml");
			return;
		}

		if (conf.getString(EC2CloudManager.AWS_SECRET_KEY_KEY, null) == null) {
			System.err.println("Please set the key " + EC2CloudManager.AWS_SECRET_KEY_KEY + " in "
				+ System.getProperty("user.dir")
				+ "/correct-conf/ec2-account.xml");
			return;
		}

		Object reservedInstances = new Object();
		Object cloudInstances = new Object();
		Object floatingInstances = new Object();
		Object floatingInstanceIDs = new Object();
		Object jobToInstancesMap = new Object();

		try {
			Field f1 = EC2CloudManager.class.getDeclaredField("reservedInstances");
			f1.setAccessible(true);
			reservedInstances = f1.get(cm);

			Field f2 = EC2CloudManager.class.getDeclaredField("cloudInstances");
			f2.setAccessible(true);
			cloudInstances = f2.get(cm);

			Field f3 = EC2CloudManager.class.getDeclaredField("floatingInstances");
			f3.setAccessible(true);
			floatingInstances = f3.get(cm);

			Field f4 = EC2CloudManager.class.getDeclaredField("floatingInstanceIDs");
			f4.setAccessible(true);
			floatingInstanceIDs = f4.get(cm);

			Field f5 = EC2CloudManager.class.getDeclaredField("jobToInstancesMap");
			f5.setAccessible(true);
			jobToInstancesMap = f5.get(cm);

		} catch (SecurityException e1) {
			e1.printStackTrace();
		} catch (NoSuchFieldException e1) {
			e1.printStackTrace();
		} catch (IllegalArgumentException e1) {
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			e1.printStackTrace();
		}

		assertEquals(0, ((Map<String, JobID>) reservedInstances).size());
		assertEquals(0, ((List<EC2CloudInstance>) cloudInstances).size());
		assertEquals(0, ((Map<InstanceConnectionInfo, FloatingInstance>) floatingInstances).size());
		assertEquals(0, ((Map<String, Configuration>) floatingInstanceIDs).size());
		assertEquals(0, ((Map<JobID, JobToInstancesMapping>) jobToInstancesMap).size());

		// request instance
		try {
			final InstanceRequestMap instanceRequestMap = new InstanceRequestMap();
			final InstanceType instanceType = InstanceTypeFactory.constructFromDescription("m1.small,1,1,2048,40,10");
			instanceRequestMap.setMinimumNumberOfInstances(instanceType, 1);
			instanceRequestMap.setMaximumNumberOfInstances(instanceType, 1);
			cm.requestInstance(jobID, conf, instanceRequestMap, null);
		} catch (InstanceException e) {
			e.printStackTrace();
		}

		assertEquals(1, ((Map<String, JobID>) reservedInstances).size());
		assertEquals(0, ((List<EC2CloudInstance>) cloudInstances).size());
		assertEquals(0, ((Map<InstanceConnectionInfo, FloatingInstance>) floatingInstances).size());
		assertEquals(0, ((Map<String, Configuration>) floatingInstanceIDs).size());
		assertEquals(1, ((Map<JobID, JobToInstancesMapping>) jobToInstancesMap).size());

		// describe instance
		String instanceID = null;

		try {
			// using legacy EC2 client...
			/*
			 * Method m1 = CloudManager.class.getDeclaredMethod("describeInstances", new Class[] { String.class,
			 * String.class, String.class });
			 * m1.setAccessible(true);
			 * Object instanceList = m1.invoke(cm, new Object[] { conf.getString("job.cloud.username", null),
			 * conf.getString("job.ec2.awsaccessid", null), conf.getString("job.ec2.awssecretkey", null) });
			 * assertEquals(1, ((List<com.xerox.amazonws.ec2.ReservationDescription.Instance>) instanceList).size());
			 * com.xerox.amazonws.ec2.ReservationDescription.Instance instance =
			 * ((List<com.xerox.amazonws.ec2.ReservationDescription.Instance>) instanceList)
			 * .get(0);
			 * instanceID = instance.getInstanceId();
			 * // report heart beat
			 * final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(8,
			 * 32L * 1024L * 1024L * 1024L, 32L * 1024L * 1024L * 1024L);
			 * cm.reportHeartBeat(new InstanceConnectionInfo(InetAddress.getByName(instance.getDnsName()), 10000,
			 * 20000),
			 * hardwareDescription);
			 */
		} catch (Exception e) {
			e.printStackTrace();
		}

		assertEquals(0, ((Map<String, JobID>) reservedInstances).size());
		assertEquals(1, ((List<EC2CloudInstance>) cloudInstances).size());
		assertEquals(0, ((Map<InstanceConnectionInfo, FloatingInstance>) floatingInstances).size());
		assertEquals(0, ((Map<String, Configuration>) floatingInstanceIDs).size());
		assertEquals(1, ((Map<JobID, JobToInstancesMapping>) jobToInstancesMap).size());

		// release instance
		EC2CloudInstance ci = ((List<EC2CloudInstance>) cloudInstances).get(0);
		cm.releaseAllocatedResource(jobID, conf, ci.asAllocatedResource());

		assertEquals(0, ((Map<String, JobID>) reservedInstances).size());
		assertEquals(0, ((List<EC2CloudInstance>) cloudInstances).size());
		assertEquals(1, ((Map<InstanceConnectionInfo, FloatingInstance>) floatingInstances).size());
		assertEquals(1, ((Map<String, Configuration>) floatingInstanceIDs).size());
		assertEquals(1, ((Map<JobID, JobToInstancesMapping>) jobToInstancesMap).size());

		// destroy instance
		assertNotNull(instanceID);

		try {
			Method m2 = EC2CloudManager.class.getDeclaredMethod("destroyCloudInstance", new Class[] {
				Configuration.class,
				String.class });
			m2.setAccessible(true);
			Object terminatedID = m2.invoke(cm, new Object[] { conf, instanceID });

			assertEquals(instanceID, terminatedID);

		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
}
