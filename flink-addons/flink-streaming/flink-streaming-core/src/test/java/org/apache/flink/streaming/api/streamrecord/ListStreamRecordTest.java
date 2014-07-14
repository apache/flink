/***********************************************************************************************************************
 *
 * Copyright (C) 20102014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE2.0
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
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.streamrecord.ListStreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.junit.Test;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.types.TypeInformation;

public class ListStreamRecordTest {

	@Test
	public void constructorTest() {
		StreamRecord record = new ListStreamRecord(10);
		assertEquals(10, record.getBatchSize());

		List<Tuple> tuples = new ArrayList<Tuple>();
		tuples.add(new Tuple1<Integer>(2));
		tuples.add(new Tuple1<Integer>(3));

		StreamRecord record1 = new ListStreamRecord(tuples);

		assertEquals(2, record1.getBatchSize());

		StreamRecord record2 = new ListStreamRecord(record1);
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
		TupleTypeInfo<Tuple> tt= (TupleTypeInfo<Tuple>)ti;

		System.out.println("Type info: " + tt);

	}

}