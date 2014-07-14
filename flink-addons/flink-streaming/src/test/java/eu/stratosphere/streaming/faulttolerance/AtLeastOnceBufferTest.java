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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

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

		StreamRecord record1 = new StreamRecord(new Tuple1<String>("R1")).setId("1");

		buffer.addToAckCounter(record1.getId());

		assertEquals((Integer) 3, buffer.ackCounter.get(record1.getId()));

	}

	@Test
	public void testRemoveFromAckCounter() {
		StreamRecord record1 = new StreamRecord(new Tuple1<String>("R1")).setId("1");
		StreamRecord record2 = new StreamRecord(new Tuple1<String>("R2")).setId("1");

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
		StreamRecord record1 = new StreamRecord(new Tuple1<String>("R1")).setId("1");
		String id = record1.getId();

		buffer.add(record1);
		assertEquals((Integer) 3, buffer.ackCounter.get(id));

		buffer.ack(id, 1);
		assertEquals((Integer) 2, buffer.ackCounter.get(id));

		buffer.ack(id, 1);
		assertEquals((Integer) 1, buffer.ackCounter.get(id));

		buffer.ack(id, 1);
		assertFalse(buffer.ackCounter.containsKey(id));
		assertFalse(buffer.recordBuffer.containsKey(id));
		assertFalse(buffer.recordTimestamps.containsKey(id));

	}

	@Test
	public void testAtLeastOnceFaultToleranceBuffer() {
		numberOfChannels = new int[] { 2, 2, 2 };

		buffer = new AtLeastOnceFaultToleranceBuffer(numberOfChannels, "2");

		assertArrayEquals(numberOfChannels, buffer.numberOfEffectiveChannels);
		assertEquals("2", buffer.componentInstanceID);
		assertEquals(6, buffer.totalNumberOfEffectiveChannels);

	}

	@Test
	public void testAdd() {

		StreamRecord record1 = new StreamRecord(new Tuple1<String>("R1")).setId("1");

		String id1 = record1.getId();

		Long nt = System.nanoTime();

		buffer.add(record1);

		System.out.println("ADD - " + " exec. time (ns): " + (System.nanoTime() - nt));

		record1.setRecord(new Tuple1<String>("R2"));
		record1.setId("1");
		String id2 = record1.getId();

		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
		}
		buffer.add(record1);

		assertEquals((Integer) 3, buffer.ackCounter.get(id1));
		assertEquals((Integer) 3, buffer.ackCounter.get(id2));

		assertEquals("R1", buffer.recordBuffer.get(id1).getField(0));
		assertEquals(id1, buffer.recordBuffer.get(id1).getId());

		assertEquals("R2", buffer.recordBuffer.get(id2).getField(0));
		assertEquals(id2, buffer.recordBuffer.get(id2).getId());

		assertEquals(2, buffer.recordTimestamps.size());
		assertEquals(2, buffer.recordsByTime.size());
		assertEquals(2, buffer.recordBuffer.size());
		assertEquals(2, buffer.ackCounter.size());

	}

	@Test
	public void testFail() {
		StreamRecord record1 = new StreamRecord(new Tuple1<String>("R1")).setId("1");
		String id1 = record1.getId();

		buffer.add(record1);
		buffer.ack(id1, 1);
		buffer.ack(id1, 1);

		assertEquals(1, buffer.recordBuffer.size());
		assertEquals(1, buffer.recordTimestamps.size());
		assertEquals(1, buffer.ackCounter.size());

		StreamRecord failed = buffer.fail(id1);
		String id2 = failed.getId();

		assertFalse(buffer.ackCounter.containsKey(id1));
		assertFalse(buffer.recordBuffer.containsKey(id1));
		assertFalse(buffer.recordTimestamps.containsKey(id1));

		assertTrue(buffer.ackCounter.containsKey(id2));
		assertTrue(buffer.recordBuffer.containsKey(id2));
		assertTrue(buffer.recordTimestamps.containsKey(id2));

		assertEquals(1, buffer.recordBuffer.size());
		assertEquals(1, buffer.recordTimestamps.size());
		assertEquals(1, buffer.ackCounter.size());
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
		StreamRecord record1 = new StreamRecord(new Tuple1<String>("R1")).setId("1");

		String id1 = record1.getId();
		buffer.add(record1);

		record1.setRecord(new Tuple1<String>("R2"));
		record1.setId("1");
		String id2 = record1.getId();
		buffer.add(record1);

		assertTrue(buffer.ackCounter.containsKey(id1));
		assertTrue(buffer.recordBuffer.containsKey(id1));
		assertTrue(buffer.recordTimestamps.containsKey(id1));

		assertTrue(buffer.ackCounter.containsKey(id2));
		assertTrue(buffer.recordBuffer.containsKey(id2));
		assertTrue(buffer.recordTimestamps.containsKey(id2));

		assertEquals(2, buffer.recordBuffer.size());
		assertEquals(2, buffer.recordTimestamps.size());
		assertEquals(2, buffer.ackCounter.size());

		StreamRecord removed = buffer.remove(id1);
		assertEquals("R1", removed.getField(0));
		assertEquals(id1, removed.getId());

		assertFalse(buffer.ackCounter.containsKey(id1));
		assertFalse(buffer.recordBuffer.containsKey(id1));
		assertFalse(buffer.recordTimestamps.containsKey(id1));

		assertTrue(buffer.ackCounter.containsKey(id2));
		assertTrue(buffer.recordBuffer.containsKey(id2));
		assertTrue(buffer.recordTimestamps.containsKey(id2));

		assertEquals(1, buffer.recordBuffer.size());
		assertEquals(1, buffer.recordTimestamps.size());
		assertEquals(1, buffer.ackCounter.size());

	}

}
