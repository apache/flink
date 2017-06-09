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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Tests for the type information parsing of {@link Writable}.
 */
public class WritableInfoParserTest {

	@Test
	public void testWritableType() {
		TypeInformation<?> ti = TypeInfoParser.parse(
				"Writable<org.apache.flink.api.java.typeutils.WritableInfoParserTest$MyWritable>");

		Assert.assertTrue(ti instanceof WritableTypeInfo<?>);
		Assert.assertEquals(MyWritable.class, ((WritableTypeInfo<?>) ti).getTypeClass());
	}

	@Test
	public void testPojoWithWritableType() {
		TypeInformation<?> ti = TypeInfoParser.parse(
				"org.apache.flink.api.java.typeutils.WritableInfoParserTest$MyPojo<"
				+ "basic=Integer,"
				+ "tuple=Tuple2<String, Integer>,"
				+ "hadoopCitizen=Writable<org.apache.flink.api.java.typeutils.WritableInfoParserTest$MyWritable>,"
				+ "array=String[]"
				+ ">");
		Assert.assertTrue(ti instanceof PojoTypeInfo);
		PojoTypeInfo<?> pti = (PojoTypeInfo<?>) ti;
		Assert.assertEquals("array", pti.getPojoFieldAt(0).getField().getName());
		Assert.assertTrue(pti.getPojoFieldAt(0).getTypeInformation() instanceof BasicArrayTypeInfo);
		Assert.assertEquals("basic", pti.getPojoFieldAt(1).getField().getName());
		Assert.assertTrue(pti.getPojoFieldAt(1).getTypeInformation() instanceof BasicTypeInfo);
		Assert.assertEquals("hadoopCitizen", pti.getPojoFieldAt(2).getField().getName());
		Assert.assertTrue(pti.getPojoFieldAt(2).getTypeInformation() instanceof WritableTypeInfo);
		Assert.assertEquals("tuple", pti.getPojoFieldAt(3).getField().getName());
		Assert.assertTrue(pti.getPojoFieldAt(3).getTypeInformation() instanceof TupleTypeInfo);
	}
	// ------------------------------------------------------------------------
	//  Test types
	// ------------------------------------------------------------------------

	private static class MyWritable implements Writable {

		@Override
		public void write(DataOutput out) throws IOException {}

		@Override
		public void readFields(DataInput in) throws IOException {}
	}

	/**
	 * Test Pojo containing a {@link Writable}.
	 */
	public static class MyPojo {
		public Integer basic;
		public Tuple2<String, Integer> tuple;
		public MyWritable hadoopCitizen;
		public String[] array;
	}
}
