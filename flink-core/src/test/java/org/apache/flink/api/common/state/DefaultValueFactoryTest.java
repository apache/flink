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

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

/**
 * Tests for the default value factory.
 */
@SuppressWarnings("deprecation")
public class DefaultValueFactoryTest {

	@Test
	public void testCreateFromNull() {
		assertNull(DefaultValueFactory.create(String.class, null));
		assertNull(DefaultValueFactory.create(BasicTypeInfo.STRING_TYPE_INFO, null));
		assertNull(DefaultValueFactory.create(StringSerializer.INSTANCE, null));
	}

	@Test
	public void testCreateFromSerializer() throws Exception {
		MyType value = new MyType(42);

		DefaultValueFactory<MyType> factory = DefaultValueFactory.create(
				new KryoSerializer<>(MyType.class, new ExecutionConfig()), value);

		runTests(factory, value);
	}

	@Test
	public void testCreateFromTypeInfo() throws Exception {
		MyType value = new MyType(17);

		DefaultValueFactory<MyType> factory = DefaultValueFactory.create(
				new GenericTypeInfo<>(MyType.class), value);

		runTests(factory, value);
	}

	@Test
	public void testCreateFromClass() throws Exception {
		MyType value = new MyType(31);

		DefaultValueFactory<MyType> factory = DefaultValueFactory.create(MyType.class, value);

		runTests(factory, value);
	}

	private <T> void runTests(DefaultValueFactory<T> factory, T expectedValue) throws Exception {
		assertEquals(expectedValue, factory.get());
		assertNotSame(expectedValue, factory.get());

		DefaultValueFactory<T> serializedCopy = CommonTestUtils.createCopySerializable(factory);

		assertEquals(expectedValue, serializedCopy.get());
		assertNotSame(expectedValue, serializedCopy.get());
	}

	// ------------------------------------------------------------------------

	/** Simple test data type. */
	public static class MyType {

		public int a;

		@SuppressWarnings("unused")
		public MyType() {}

		public MyType(int a) {
			this.a = a;
		}

		@Override
		public boolean equals(Object o) {
			return this == o || o != null && o.getClass() == MyType.class && ((MyType) o).a == this.a;
		}

		@Override
		public int hashCode() {
			return a;
		}
	}
}
