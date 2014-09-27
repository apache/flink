/*
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
 */

package org.apache.flink.streaming.util.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.junit.Test;

public class TypeSerializationTest {

	private static class MyMap extends RichMapFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map(Integer value) throws Exception {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void functionTypeSerializationTest() {
		TypeWrapper<Integer> ser = new FunctionTypeWrapper<Integer>(new MyMap(),
				RichMapFunction.class, 0);

		byte[] serializedType = SerializationUtils.serialize(ser);

		TypeWrapper<Integer> ser2 = (TypeWrapper<Integer>) SerializationUtils
				.deserialize(serializedType);

		assertNotNull(ser.getTypeInfo());
		assertNotNull(ser2.getTypeInfo());

		assertEquals(ser.getTypeInfo(), ser2.getTypeInfo());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void objectTypeSerializationTest() {
		Integer instance = new Integer(22);
		
		TypeWrapper<Integer> ser = new ObjectTypeWrapper<Integer>(instance);
		
		byte[] serializedType = SerializationUtils.serialize(ser);

		TypeWrapper<Integer> ser2 = (TypeWrapper<Integer>) SerializationUtils
				.deserialize(serializedType);

		assertNotNull(ser.getTypeInfo());
		assertNotNull(ser2.getTypeInfo());

		assertEquals(ser.getTypeInfo(), ser2.getTypeInfo());
	}
}
