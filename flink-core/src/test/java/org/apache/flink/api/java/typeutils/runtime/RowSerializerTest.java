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
package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.io.Serializable;

public class RowSerializerTest {

	@Test
	public void testRowSerializer() {
		TypeInformation<Row> typeInfo = new RowTypeInfo(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);
		Row row1 = new Row(2);
		row1.setField(0, 1);
		row1.setField(1, "a");

		Row row2 = new Row(2);
		row2.setField(0, 2);
		row2.setField(1, null);

		TypeSerializer<Row> serializer = typeInfo.createSerializer(new ExecutionConfig());
		RowSerializerTestInstance instance = new RowSerializerTestInstance(serializer, row1, row2);
		instance.testAll();
	}

	@Test
	public void testLargeRowSerializer() {
		TypeInformation<Row> typeInfo = new RowTypeInfo(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);

		Row row = new Row(13);
		row.setField(0, 2);
		row.setField(1, null);
		row.setField(3, null);
		row.setField(4, null);
		row.setField(5, null);
		row.setField(6, null);
		row.setField(7, null);
		row.setField(8, null);
		row.setField(9, null);
		row.setField(10, null);
		row.setField(11, null);
		row.setField(12, "Test");

		TypeSerializer<Row> serializer = typeInfo.createSerializer(new ExecutionConfig());
		RowSerializerTestInstance testInstance = new RowSerializerTestInstance(serializer, row);
		testInstance.testAll();
	}

	@Test
	public void testRowSerializerWithComplexTypes() {
		TypeInformation<Row> typeInfo = new RowTypeInfo(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			new TupleTypeInfo<Tuple3<Integer, Boolean, Short>>(
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.BOOLEAN_TYPE_INFO,
				BasicTypeInfo.SHORT_TYPE_INFO),
			TypeExtractor.createTypeInfo(MyPojo.class));

		MyPojo testPojo1 = new MyPojo();
		testPojo1.name = null;
		MyPojo testPojo2 = new MyPojo();
		testPojo2.name = "Test1";
		MyPojo testPojo3 = new MyPojo();
		testPojo3.name = "Test2";

		Row[] data = new Row[]{
			createRow(null, null, null, null, null),
			createRow(0, null, null, null, null),
			createRow(0, 0.0, null, null, null),
			createRow(0, 0.0, "a", null, null),
			createRow(1, 0.0, "a", null, null),
			createRow(1, 1.0, "a", null, null),
			createRow(1, 1.0, "b", null, null),
			createRow(1, 1.0, "b", new Tuple3<>(1, false, (short) 2), null),
			createRow(1, 1.0, "b", new Tuple3<>(2, false, (short) 2), null),
			createRow(1, 1.0, "b", new Tuple3<>(2, true, (short) 2), null),
			createRow(1, 1.0, "b", new Tuple3<>(2, true, (short) 3), null),
			createRow(1, 1.0, "b", new Tuple3<>(2, true, (short) 3), testPojo1),
			createRow(1, 1.0, "b", new Tuple3<>(2, true, (short) 3), testPojo2),
			createRow(1, 1.0, "b", new Tuple3<>(2, true, (short) 3), testPojo3)
		};

		TypeSerializer<Row> serializer = typeInfo.createSerializer(new ExecutionConfig());
		RowSerializerTestInstance testInstance = new RowSerializerTestInstance(serializer, data);
		testInstance.testAll();
	}

	// ----------------------------------------------------------------------------------------------

	private static Row createRow(Object f0, Object f1, Object f2, Object f3, Object f4) {
		Row row = new Row(5);
		row.setField(0, f0);
		row.setField(1, f1);
		row.setField(2, f2);
		row.setField(3, f3);
		row.setField(4, f4);
		return row;
	}


	private class RowSerializerTestInstance extends SerializerTestInstance<Row> {

		RowSerializerTestInstance(
			TypeSerializer<Row> serializer,
			Row... testData) {
			super(serializer, Row.class, -1, testData);
		}
	}

	public static class MyPojo implements Serializable, Comparable<MyPojo> {
		public String name = null;

		@Override
		public int compareTo(MyPojo o) {
			if (name == null && o.name == null) {
				return 0;
			} else if (name == null) {
				return -1;
			} else if (o.name == null) {
				return 1;
			} else {
				return name.compareTo(o.name);
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			MyPojo myPojo = (MyPojo) o;

			return name != null ? name.equals(myPojo.name) : myPojo.name == null;
		}

	}
}
