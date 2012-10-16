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

package eu.stratosphere.nephele.event.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import eu.stratosphere.nephele.util.CommonTestUtils;

/**
 * This class contains serialization tests concerning task events derived from
 * {@link eu.stratosphere.nephele.event.task.AbstractEvent}.
 * 
 * @author warneke
 */
public class TaskEventTest {

	/**
	 * This test checks the serialization/deserialization of {@link IntegerTaskEvent} objects.
	 */
	@Test
	public void testIntegerTaskEvent() {

		final IntegerTaskEvent orig = new IntegerTaskEvent(11);
		final IntegerTaskEvent copy = (IntegerTaskEvent) CommonTestUtils.createCopy(orig);

		assertEquals(orig.getInteger(), copy.getInteger());
		assertEquals(orig.hashCode(), copy.hashCode());
		assertTrue(orig.equals(copy));

	}

	/**
	 * This test checks the serialization/deserialization of {@link StringTaskEvent} objects.
	 */
	@Test
	public void testStringTaskEvent() {

		final StringTaskEvent orig = new StringTaskEvent("Test");
		final StringTaskEvent copy = (StringTaskEvent) CommonTestUtils.createCopy(orig);

		assertEquals(orig.getString(), copy.getString());
		assertEquals(orig.hashCode(), copy.hashCode());
		assertTrue(orig.equals(copy));

	}

	/**
	 * This test checks the serialization/deserialization of {@link EventList} objects.
	 */
	@Test
	public void testEventList() {

		final EventList orig = new EventList();
		orig.add(new StringTaskEvent("Test 2"));
		orig.add(new IntegerTaskEvent(70));
		final EventList copy = (EventList) CommonTestUtils.createCopy(orig);

		assertEquals(orig.size(), copy.size());
		final Iterator<AbstractEvent> origIt = orig.iterator();
		final Iterator<AbstractEvent> copyIt = copy.iterator();
		while (origIt.hasNext()) {
			assertEquals(origIt.next(), copyIt.next());
		}
	}
}
