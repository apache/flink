/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.ec2.FloatingInstance;

public class FloatingInstanceTest {

	@Test
	public void testHeartBeat() {

		FloatingInstance fi = new FloatingInstance("i-1234ABCD", new InstanceConnectionInfo(new InetSocketAddress(
			"localhost", 6122).getAddress(), 6122, 6121), System.currentTimeMillis(),
			EC2CloudManager.DEFAULT_LEASE_PERIOD, null, null, null, null);

		long lastHeartBeat = fi.getLastReceivedHeartBeat();
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		fi.updateLastReceivedHeartBeat();

		assertTrue(fi.getLastReceivedHeartBeat() - lastHeartBeat > 0);
	}
}