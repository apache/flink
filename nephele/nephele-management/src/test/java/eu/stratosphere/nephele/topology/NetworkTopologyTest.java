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

package eu.stratosphere.nephele.topology;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.util.StringUtils;

public class NetworkTopologyTest {

	@Test
	public void testNetworkTopology() {

		NetworkTopology topology;
		try {
			topology = NetworkTopology.fromFile(System.getProperty("user.dir") + File.separator + "topology.txt");
		} catch (IOException e) {
			assertEquals(StringUtils.stringifyException(e), false, true);
			return;
		}

		assertEquals(topology.getDepth(), 4);

		final NetworkNode networkNode = topology.getNodeByName("rackswitch3");
		if (networkNode == null) {
			assertEquals("Cannot find network node rackswitch3", true, false);
			return;
		}

		assertEquals(networkNode.getNumberOfChildNodes(), 4);
	}

}
