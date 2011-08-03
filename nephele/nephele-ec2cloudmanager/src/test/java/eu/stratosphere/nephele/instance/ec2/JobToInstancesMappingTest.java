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

import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.junit.Test;

import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.nephele.instance.ec2.EC2CloudInstance;
import eu.stratosphere.nephele.instance.ec2.JobToInstancesMapping;
import eu.stratosphere.nephele.topology.NetworkTopology;

public class JobToInstancesMappingTest {

	@Test
	public void testAssignedInstances() {

		final NetworkTopology networkTopology = NetworkTopology.createEmptyTopology();
		final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(1, 2048L * 1024L * 1024L,
			2048L * 1024L * 1024L);

		JobToInstancesMapping map = new JobToInstancesMapping("1234567", "abcdefg");
		EC2CloudInstance ci = new EC2CloudInstance("i-1234ABCD",
			InstanceTypeFactory.constructFromDescription("m1.small,1,1,2048,40,10"),
			new InstanceConnectionInfo(new InetSocketAddress("localhost", 6122).getAddress(), 6122, 6121), 1234567890,
			networkTopology.getRootNode(), networkTopology, hardwareDescription, null, null);

		assertEquals(0, map.getNumberOfAssignedInstances());
		assertEquals(new ArrayList<EC2CloudInstance>(), map.getAssignedInstances());

		map.assignInstanceToJob(ci);

		assertEquals(1, map.getNumberOfAssignedInstances());
		assertEquals(ci, map.getAssignedInstances().get(0));

		assertEquals(ci, map.getInstanceByConnectionInfo(new InstanceConnectionInfo(new InetSocketAddress("localhost",
			6122).getAddress(), 6122, 6121)));

		map.unassignInstanceFromJob(ci);

		assertEquals(0, map.getNumberOfAssignedInstances());
		assertEquals(new ArrayList<EC2CloudInstance>(), map.getAssignedInstances());
	}
}