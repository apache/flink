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


package org.apache.flink.runtime;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.junit.Test;

/**
 * This class contains tests for the {@link org.apache.flink.runtime.AbstractID} class.
 * 
 */
public class AbstractIDTest {

	/**
	 * Tests the setID method of an abstract ID.
	 */
	@Test
	public void testSetID() {

		final ChannelID id1 = new ChannelID();
		final ChannelID id2 = new ChannelID();
		id1.setID(id2);

		assertEquals(id1.hashCode(), id2.hashCode());
		assertEquals(id1, id2);
	}

	/**
	 * Tests the serialization/deserialization of an abstract ID.
	 */
	@Test
	public void testSerialization() {

		final ChannelID origID = new ChannelID();
		try {
			final ChannelID copyID = (ChannelID) CommonTestUtils.createCopy(origID);

			assertEquals(origID.hashCode(), copyID.hashCode());
			assertEquals(origID, copyID);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
