/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.deployment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.io.network.channels.ChannelType;
import org.apache.flink.runtime.io.network.gates.GateID;
import org.apache.flink.runtime.testutils.ServerTestUtils;
import org.apache.flink.util.StringUtils;
import org.junit.Test;

/**
 * This class contains unit tests for the {@link GateDeploymentDescriptor} class.
 * 
 */
public class GateDeploymentDescriptorTest {

	/**
	 * Tests the constructor of the {@link GateDeploymentDescriptor} class with valid arguments.
	 */
	@Test
	public void testConstructorWithValidArguments() {

		final GateID gateID = new GateID();
		final ChannelType channelType = ChannelType.IN_MEMORY;
		final List<ChannelDeploymentDescriptor> channels = new ArrayList<ChannelDeploymentDescriptor>(0);

		final GateDeploymentDescriptor gdd = new GateDeploymentDescriptor(gateID, channelType, channels);

		assertEquals(gateID, gdd.getGateID());
		assertEquals(channelType, gdd.getChannelType());
		assertEquals(channels.size(), gdd.getNumberOfChannelDescriptors());
	}

	/**
	 * Tests the constructor of the {@link GateDeploymentDescriptor} class with valid arguments.
	 */
	@Test
	public void testConstructorWithInvalidArguments() {

		final GateID gateID = new GateID();
		final ChannelType channelType = ChannelType.IN_MEMORY;
		final List<ChannelDeploymentDescriptor> channels = new ArrayList<ChannelDeploymentDescriptor>(0);

		boolean firstExceptionCaught = false;
		boolean secondExceptionCaught = false;
		boolean thirdExceptionCaught = false;

		try {
			new GateDeploymentDescriptor(null, channelType, channels);
		} catch (IllegalArgumentException e) {
			firstExceptionCaught = true;
		}

		try {
			new GateDeploymentDescriptor(gateID, null, channels);
		} catch (IllegalArgumentException e) {
			secondExceptionCaught = true;
		}

		try {
			new GateDeploymentDescriptor(gateID, channelType, null);
		} catch (IllegalArgumentException e) {
			thirdExceptionCaught = true;
		}

		if (!firstExceptionCaught) {
			fail("First argument was illegal but not detected");
		}

		if (!secondExceptionCaught) {
			fail("Second argument was illegal but not detected");
		}


		if (!thirdExceptionCaught) {
			fail("Third argument was illegal but not detected");
		}
	}

	/**
	 * Tests the serialization/deserialization of the {@link GateDeploymentDescriptor} class.
	 */
	@Test
	public void testSerialization() {

		final GateID gateID = new GateID();
		final ChannelType channelType = ChannelType.IN_MEMORY;
		final List<ChannelDeploymentDescriptor> channels = new ArrayList<ChannelDeploymentDescriptor>(0);
		final ChannelDeploymentDescriptor cdd = new ChannelDeploymentDescriptor(new ChannelID(), new ChannelID());
		channels.add(cdd);

		final GateDeploymentDescriptor orig = new GateDeploymentDescriptor(gateID, channelType,
			channels);

		GateDeploymentDescriptor copy = null;

		try {
			copy = ServerTestUtils.createCopy(orig);
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		assertFalse(orig.getGateID() == copy.getGateID());

		assertEquals(orig.getGateID(), copy.getGateID());
		assertEquals(orig.getChannelType(), copy.getChannelType());
		assertEquals(orig.getNumberOfChannelDescriptors(), copy.getNumberOfChannelDescriptors());
		assertEquals(orig.getChannelDescriptor(0).getOutputChannelID(), copy.getChannelDescriptor(0)
			.getOutputChannelID());
		assertEquals(orig.getChannelDescriptor(0).getInputChannelID(), copy.getChannelDescriptor(0).getInputChannelID());
	}
}
