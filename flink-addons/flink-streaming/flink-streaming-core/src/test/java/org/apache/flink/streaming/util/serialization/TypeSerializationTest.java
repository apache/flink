/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.util.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.java.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.junit.Test;

public class TypeSerializationTest {

	private static class MyMap extends MapFunction<Tuple1<Integer>, Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple1<String> map(Tuple1<Integer> value) throws Exception {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void functionTypeSerializationTest() {
		TypeSerializerWrapper<Tuple1<Integer>, Tuple, Tuple1<Integer>> ser = new FunctionTypeWrapper<Tuple1<Integer>, Tuple, Tuple1<Integer>>(
				new MyMap(), MapFunction.class, 0, -1, 1);

		byte[] serializedType = SerializationUtils.serialize(ser);

		TypeSerializerWrapper<Tuple1<Integer>, Tuple, Tuple1<Integer>> ser2 = (TypeSerializerWrapper<Tuple1<Integer>, Tuple, Tuple1<Integer>>) SerializationUtils
				.deserialize(serializedType);

		assertNotNull(ser.getInputTupleTypeInfo1());
		assertNotNull(ser2.getInputTupleTypeInfo1());
		
		assertNotNull(ser.getOutputTupleTypeInfo());
		assertNotNull(ser2.getOutputTupleTypeInfo());
		
		assertEquals(ser.getInputTupleTypeInfo1(), ser2.getInputTupleTypeInfo1());
		assertEquals(ser.getInputTupleTypeInfo2(), ser2.getInputTupleTypeInfo2());
		assertEquals(ser.getOutputTupleTypeInfo(), ser2.getOutputTupleTypeInfo());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void objectTypeSerializationTest() {
		Integer instance1 = new Integer(22);
		Integer instance2 = null;
		Integer instance3 = new Integer(34);

		TypeSerializerWrapper<Tuple1<Integer>, Tuple, Tuple1<Integer>> ser = new ObjectTypeWrapper<Tuple1<Integer>, Tuple, Tuple1<Integer>>(
				instance1, instance2, instance3);

		System.out.println(ser.getInputTupleTypeInfo1());

		byte[] serializedType = SerializationUtils.serialize(ser);

		TypeSerializerWrapper<Tuple1<Integer>, Tuple, Tuple1<Integer>> ser2 = (TypeSerializerWrapper<Tuple1<Integer>, Tuple, Tuple1<Integer>>) SerializationUtils
				.deserialize(serializedType);

		assertNotNull(ser.getInputTupleTypeInfo1());
		assertNotNull(ser2.getInputTupleTypeInfo1());

		assertNotNull(ser.getOutputTupleTypeInfo());
		assertNotNull(ser2.getOutputTupleTypeInfo());
		
		assertEquals(ser.getInputTupleTypeInfo1(), ser2.getInputTupleTypeInfo1());
		assertEquals(ser.getInputTupleTypeInfo2(), ser2.getInputTupleTypeInfo2());
		assertEquals(ser.getOutputTupleTypeInfo(), ser2.getOutputTupleTypeInfo());
	}
}
