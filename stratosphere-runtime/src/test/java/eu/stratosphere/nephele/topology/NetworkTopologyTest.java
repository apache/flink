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

package eu.stratosphere.nephele.topology;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.util.ManagementTestUtils;

/**
 * This class contains unit tests which address the correctness of the network topology implementation.
 * 
 * @author warneke
 */
public class NetworkTopologyTest {

	/**
	 * This test checks some basic functionality like loading a network topology from a file, looking up network nodes
	 * in the topology by their name, or determining the depth of network node with respect to the topology.
	 */
	@Test
	public void testNetworkTopology() {

		NetworkTopology topology;
		try {
			topology = NetworkTopology.fromFile(getPathToTopologyFile());
		} catch (IOException e) {
			fail(e.getMessage());
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

	/**
	 * This test checks the distance computation between two network nodes within a network topology.
	 */
	@Test
	public void testTopologyDistance() {

		NetworkTopology topology;
		try {
			topology = NetworkTopology.fromFile(getPathToTopologyFile());
		} catch (IOException e) {
			fail(e.getMessage());
			return;
		}

		final NetworkNode node01 = topology.getNodeByName("node01");
		final NetworkNode node02 = topology.getNodeByName("node02");
		final NetworkNode node05 = topology.getNodeByName("node05");
		final NetworkNode node11 = topology.getNodeByName("node11");
		final NetworkNode rackswitch1 = topology.getNodeByName("rackswitch1");
		final NetworkNode mainswitch1 = topology.getNodeByName("mainswitch1");

		// Check if the network node lookup works correctly
		assertNotNull(node01);
		assertNotNull(node02);
		assertNotNull(node05);
		assertNotNull(node11);
		assertNotNull(rackswitch1);
		assertNotNull(mainswitch1);
		assertNull(topology.getNodeByName("nonexistant node"));

		// Check the various distances between the nodes
		assertEquals(0, node01.getDistance(node01));
		assertEquals(2, node01.getDistance(node02));
		assertEquals(4, node01.getDistance(node05));
		assertEquals(6, node01.getDistance(node11));

		assertEquals(1, node01.getDistance(rackswitch1));
		assertEquals(1, rackswitch1.getDistance(node01));
		assertEquals(2, mainswitch1.getDistance(node02));
		assertEquals(3, topology.getRootNode().getDistance(node11));

		// Finally, create a new topology a new node and make sure the distance computation returns the correct
		// result
		final NetworkTopology otherTopology = new NetworkTopology();
		final NetworkNode nodeFromOtherTopology = new NetworkNode("node from other topology",
			otherTopology.getRootNode(), otherTopology);

		assertEquals(Integer.MAX_VALUE, node01.getDistance(nodeFromOtherTopology));

		// topology2.addNode(new NetworkNode(", parentNode, networkTopology))
	}

	/**
	 * Locates the file which contains the network topology. The network topology file is required for these tests.
	 * 
	 * @return a file object pointing to the network topology file
	 * @throws IOException
	 *         thrown if the network topology could not be located within the test resources
	 */
	private File getPathToTopologyFile() throws IOException {

		final File topology = ManagementTestUtils.locateResource("topology.txt");
		if (topology != null) {
			return topology;
		}

		throw new IOException("Cannot locate topology file to perform unit tests");
	}
}
