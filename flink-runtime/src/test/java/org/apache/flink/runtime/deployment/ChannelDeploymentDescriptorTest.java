/*
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

import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.testutils.ServerTestUtils;
import org.apache.flink.util.StringUtils;
import org.junit.Test;


public class ChannelDeploymentDescriptorTest {

	/**
	 * Tests the constructor of the {@link ChannelDeploymentDescriptor} class with valid arguments.
	 */
	@Test
	public void testConstructorWithValidArguments() {

		final ChannelID outputChannelID = new ChannelID();
		final ChannelID inputChannelID = new ChannelID();

		final ChannelDeploymentDescriptor cdd = new ChannelDeploymentDescriptor(outputChannelID, inputChannelID);

		assertEquals(outputChannelID, cdd.getOutputChannelID());
		assertEquals(inputChannelID, cdd.getInputChannelID());
	}

	/**
	 * Tests the constructor of the {@link ChannelDeploymentDescriptor} class with invalid arguments.
	 */
	@Test
	public void testConstructorWithInvalidArguments() {

		final ChannelID channelID = new ChannelID();

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

		final ChannelID outputChannelID = new ChannelID();
		final ChannelID inputChannelID = new ChannelID();

		final ChannelDeploymentDescriptor orig = new ChannelDeploymentDescriptor(outputChannelID, inputChannelID);

		ChannelDeploymentDescriptor copy = null;

		try {
			copy = ServerTestUtils.createCopy(orig);
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		assertFalse(orig.getOutputChannelID() == copy.getOutputChannelID());
		assertFalse(orig.getInputChannelID() == copy.getInputChannelID());

		assertEquals(orig.getOutputChannelID(), copy.getOutputChannelID());
		assertEquals(orig.getInputChannelID(), copy.getInputChannelID());
	}
}
