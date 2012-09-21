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

package eu.stratosphere.nephele.io;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.util.CommonTestUtils;

/**
 * This class contains tests for the {@link AbstractID} class.
 * 
 * @author warneke
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
		final ChannelID copyID = (ChannelID) CommonTestUtils.createCopy(origID);

		assertEquals(origID.hashCode(), copyID.hashCode());
		assertEquals(origID, copyID);
	}
}
