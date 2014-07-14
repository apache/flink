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

package eu.stratosphere.streaming.api.streamrecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

public class StreamRecordTest {

	@Test
	public void singleRecordSetGetTest() {
		StreamRecord record = new StreamRecord(new Tuple2<String, Integer>("Stratosphere",1));

		assertEquals(2, record.getNumOfFields());
		assertEquals(1, record.getNumOfRecords());
		assertEquals("Stratosphere",record.getField(0));
		assertEquals(1, record.getField(1));

		record.setField(1, "Big Data");
		assertEquals("Big Data", record.getField(1));

		record.setRecord(new Tuple2<String, Integer>("Big Data looks tiny from here.",2));
		assertEquals(2, record.getNumOfFields());
		assertEquals(1, record.getNumOfRecords());
		assertEquals(2, record.getField(1));
	}

	@Test
	public void batchRecordSetGetTest() {
		StreamRecord record = new StreamRecord(new Tuple2<Integer, Integer>(1, 2));
		record.addRecord(new Tuple2<Integer, Integer>(2, 2));
		try {
			record.addRecord(new Tuple1<String>("4"));
			fail();
		} catch (RecordSizeMismatchException e) {
		}

		assertEquals(2, record.getNumOfFields());
		assertEquals(2, record.getNumOfRecords());
		assertEquals(1, record.getField(0, 0));
		assertEquals(2, record.getField(1, 1));
		
		record.setRecord(1, new Tuple2<Integer, Integer>(-1, -3));
		assertEquals(-1, record.getField(1, 0));

		assertEquals(2, record.getNumOfFields());
		assertEquals(2, record.getNumOfRecords());
	}

	@Test
	public void copyTest() {
		// TODO:test ID copy
		StreamRecord a = new StreamRecord(new Tuple1<String>("Big"));
		StreamRecord b = a.copy();
		assertTrue(a.getField(0).equals(b.getField(0)));
		assertTrue(a.getId().equals(b.getId()));
		b.setId("2");
		b.setRecord(new Tuple1<String>("Data"));
		assertFalse(a.getId().equals(b.getId()));
		assertFalse(a.getField(0).equals(b.getField(0)));

	}

	@Test
	public void exceptionTest() {
		StreamRecord a = new StreamRecord(new Tuple1<String>("Big"));
		try {
			a.setRecord(4, new Tuple1<String>("Data"));
			fail();
		} catch (NoSuchRecordException e) {
		}

		try {
			a.setRecord(new Tuple2<String,String>("Data","Stratosphere"));
			fail();
		} catch (RecordSizeMismatchException e) {
		}

		StreamRecord b = new StreamRecord();
		try {
			b.addRecord(new Tuple2<String,String>("Data","Stratosphere"));
			fail();
		} catch (RecordSizeMismatchException e) {
		}

		try {
			a.getField(3);
			fail();
		} catch (NoSuchFieldException e) {
		}
	}

	@Test
	public void writeReadTest() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(buff);

		int num = 42;
		String str = "above clouds";
		StreamRecord rec = new StreamRecord(
				new Tuple2<Integer, String>(num, str));

		try {
			rec.write(out);
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(
					buff.toByteArray()));

			StreamRecord newRec = new StreamRecord();
			newRec.read(in);
			Tuple2<Integer, String> tupleOut =  (Tuple2<Integer, String>) newRec.getRecord(0);

			assertEquals(tupleOut.getField(0), 42);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}

	}

}
