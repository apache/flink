/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.faulttolerance;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.types.StringValue;

public class AtLeastOnceBufferTest {

	AtLeastOnceFaultToleranceBuffer buffer;
	int[] numberOfChannels;

	@Before
	public void setUp() throws Exception {

		numberOfChannels = new int[] { 1, 2 };

		buffer = new AtLeastOnceFaultToleranceBuffer(numberOfChannels, "1");

	}

	@Test
	public void testAddToAckCounter() {

		StreamRecord record1 = new StreamRecord(new StringValue("R1")).setId("1");

		buffer.addToAckCounter(record1.getId());

		assertEquals((Integer) 3, buffer.ackCounter.get(record1.getId()));

	}

	@Test
	public void testRemoveFromAckCounter() {
		StreamRecord record1 = new StreamRecord(new StringValue("R1")).setId("1");
		StreamRecord record2 = new StreamRecord(new StringValue("R2")).setId("1");

		String id = record1.getId();

		buffer.addToAckCounter(record1.getId());
		buffer.addToAckCounter(record2.getId());

		record1.setId("1");

		buffer.removeFromAckCounter(record2.getId());

		assertEquals((Integer) 3, buffer.ackCounter.get(id));
		assertFalse(buffer.ackCounter.containsKey(record2.getId()));

	}

	@Test
	public void testAck() {
		// fail("Not yet implemented");
	}

	@Test
	public void testFailChannel() {
		// fail("Not yet implemented");
	}

	@Test
	public void testAtLeastOnceFaultToleranceBuffer() {
		// fail("Not yet implemented");
	}

	@Test
	public void testFaultToleranceBuffer() {
		// fail("Not yet implemented");
	}

	@Test
	public void testAdd() {

		StreamRecord record1 = new StreamRecord(new StringValue("R1")).setId("1");
		String id1 = record1.getId();
		buffer.add(record1);

		record1.setRecord(new StringValue("R1"));
		record1.setId("1");
		String id2 = record1.getId();

		buffer.add(record1);

		assertEquals((Integer) 3, buffer.ackCounter.get(id1));
		assertEquals((Integer) 3, buffer.ackCounter.get(id2));

		assertEquals(2, buffer.ackCounter.size());

	}

	@Test
	public void testFail() {
		// fail("Not yet implemented");
	}

	@Test
	public void testAddTimestamp() {

		Long ctime = System.currentTimeMillis();

		buffer.addTimestamp("1");

		assertEquals(ctime, buffer.recordTimestamps.get("1"));

		assertTrue(buffer.recordsByTime.containsKey(ctime));
		assertTrue(buffer.recordsByTime.get(ctime).contains("1"));

	}

	@Test
	public void testRemove() {
		// fail("Not yet implemented");
	}

}
