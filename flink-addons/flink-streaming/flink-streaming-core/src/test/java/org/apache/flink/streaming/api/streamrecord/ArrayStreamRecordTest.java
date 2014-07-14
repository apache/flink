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

package org.apache.flink.streaming.api.streamrecord;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.flink.streaming.api.streamrecord.ArrayStreamRecord;
import org.apache.flink.streaming.api.streamrecord.ListStreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.junit.Test;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.types.TypeInformation;

public class ArrayStreamRecordTest {

	@Test
	public void constructorTest() {
		ArrayStreamRecord record = new ArrayStreamRecord(10);
		assertEquals(10, record.getBatchSize());

		Tuple[] tuples = new Tuple[2];
		tuples[0] = new Tuple1<Integer>(2);
		tuples[1] = new Tuple1<Integer>(3);

		ArrayStreamRecord record1 = new ArrayStreamRecord(tuples);

		assertEquals(2, record1.getBatchSize());

		ArrayStreamRecord record2 = new ArrayStreamRecord(record1);
		assertEquals(2, record2.getBatchSize());
	}

	@Test
	public void typeExtractTest() throws IOException, ClassNotFoundException {

		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(buff);

		MyGeneric<?> g = new MyGeneric2();
		out.writeObject(g);

		ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(buff.toByteArray()));

		MyGeneric<?> f = (MyGeneric<?>) in.readObject();

		TypeInformation<?> ti = TypeExtractor.createTypeInfo(MyGeneric.class, f.getClass(), 0,
				null, null);

		System.out.println("Type info: " + ti);

	}

	@Test
	public void StreamRecordSpeedTest() {
		int len = 100000;
		ArrayStreamRecord arecord = new ArrayStreamRecord(len);
		StreamRecord record = new ListStreamRecord(len);
		Tuple2<Integer, String> tuple = new Tuple2<Integer, String>(2, "a");
		long standardTime=System.nanoTime();
		
		for (int i = 0; i < len; i++) {
			record.setTuple(i, tuple);
		}
		standardTime=System.nanoTime()-standardTime;
		
		long arrayTime=System.nanoTime();
		for (int i = 0; i < len; i++) {
			arecord.setTuple(i, tuple);
		}
		arrayTime=System.nanoTime()-arrayTime;
		
		System.out.println("Standard time: "+standardTime);
		System.out.println("Array time: "+arrayTime);
		
		float multi = (float)standardTime/(float)arrayTime;
		System.out.println("Mulitplier: "+multi);

	}

	@Test
	public void truncatedSizeTest() {
		StreamRecord record = new ArrayStreamRecord(4);
		record.setTuple(0, new Tuple1<Integer>(0));
		record.setTuple(1, new Tuple1<Integer>(1));
		record.setTuple(2, new Tuple1<Integer>(2));
		record.setTuple(3, new Tuple1<Integer>(3));
		
		StreamRecord truncatedRecord = new ArrayStreamRecord(record, 2);
		assertEquals(2, truncatedRecord.batchSize);
		assertEquals(0, truncatedRecord.getTuple(0).getField(0));
		assertEquals(1, truncatedRecord.getTuple(1).getField(0));
	}
	
	@Test
	public void copyTupleTest() {
		Tuple1<String> t1 = new Tuple1<String>("T1");
		Tuple1<String> t2 = (Tuple1<String>) StreamRecord.copyTuple(t1);
		assertEquals("T1", t2.f0);
		
		t2.f0 = "T2";
		assertEquals("T1", t1.f0);
		assertEquals("T2", t2.f0);
	}
}
