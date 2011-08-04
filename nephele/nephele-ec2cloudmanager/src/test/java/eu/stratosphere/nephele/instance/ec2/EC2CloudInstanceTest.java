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

import org.junit.Test;

import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.nephele.instance.ec2.EC2CloudInstance;
import eu.stratosphere.nephele.topology.NetworkTopology;

public class EC2CloudInstanceTest {

	private EC2CloudInstance constructSmallCloudInstance() {

		final NetworkTopology networkTopology = NetworkTopology.createEmptyTopology();
		final HardwareDescription hardwareDescription = HardwareDescriptionFactory.construct(1, 2048L * 1024L * 1024L,
			2048L * 1024L * 1024L);

		final EC2CloudInstance cloudInstance = new EC2CloudInstance("i-1234ABCD",
			InstanceTypeFactory.constructFromDescription("m1.small,1,1,2048,40,10"),
			new InstanceConnectionInfo(new InetSocketAddress("localhost", 6122).getAddress(), 6122, 6121),
			1234567890, EC2CloudManager.DEFAULT_LEASE_PERIOD, networkTopology.getRootNode(), networkTopology,
			hardwareDescription, null, null);

		return cloudInstance;
	}

	@Test
	public void testHeartBeat() {

		final EC2CloudInstance ci = constructSmallCloudInstance();

		long lastHeartBeat = ci.getLastReceivedHeartBeat();
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		ci.updateLastReceivedHeartBeat();

		assertTrue(ci.getLastReceivedHeartBeat() - lastHeartBeat > 0);
	}
}