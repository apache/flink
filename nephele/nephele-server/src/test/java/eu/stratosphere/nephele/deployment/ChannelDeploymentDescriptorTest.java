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

package eu.stratosphere.nephele.deployment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.Test;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.util.ServerTestUtils;

/**
 * This class contains unit tests for the {@link ChannelDeploymentDescriptor} class.
 * 
 * @author warneke
 */
public class ChannelDeploymentDescriptorTest {

	/**
	 * Tests the constructor of the {@link ChannelDeploymentDescriptor} class with valid arguments.
	 */
	@Test
	public void testConstructorWithValidArguments() {

		final ChannelID outputChannelID = ChannelID.generate();
		final ChannelID inputChannelID = ChannelID.generate();

		final ChannelDeploymentDescriptor cdd = new ChannelDeploymentDescriptor(outputChannelID, inputChannelID);

		assertEquals(outputChannelID, cdd.getOutputChannelID());
		assertEquals(inputChannelID, cdd.getInputChannelID());
	}

	/**
	 * Tests the constructor of the {@link ChannelDeploymentDescriptor} class with invalid arguments.
	 */
	@Test
	public void testConstructorWithInvalidArguments() {

		final ChannelID channelID = ChannelID.generate();

		boolean firstExceptionCaught = false;
		boolean secondExceptionCaught = false;

		try {

			new ChannelDeploymentDescriptor(null, channelID);

		} catch (IllegalArgumentException e) {
			firstExceptionCaught = true;
		}

		try {

			new ChannelDeploymentDescriptor(channelID, null);

		} catch (IllegalArgumentException e) {
			secondExceptionCaught = true;
		}

		if (!firstExceptionCaught) {
			fail("First argument was illegal but not detected");
		}

		if (!secondExceptionCaught) {
			fail("Second argument was illegal but not detected");
		}
	}

	/**
	 * Tests the serialization/deserialization of the {@link ChannelDeploymentDescriptor} class.
	 */
	@Test
	public void testSerialization() {

		final ChannelID outputChannelID = ChannelID.generate();
		final ChannelID inputChannelID = ChannelID.generate();

		final ChannelDeploymentDescriptor orig = new ChannelDeploymentDescriptor(outputChannelID, inputChannelID);
		final ChannelDeploymentDescriptor copy = ServerTestUtils.createCopy(orig);

		assertFalse(orig.getOutputChannelID() == copy.getOutputChannelID());
		assertFalse(orig.getInputChannelID() == copy.getInputChannelID());

		assertEquals(orig.getOutputChannelID(), copy.getOutputChannelID());
		assertEquals(orig.getInputChannelID(), copy.getInputChannelID());
	}
}
