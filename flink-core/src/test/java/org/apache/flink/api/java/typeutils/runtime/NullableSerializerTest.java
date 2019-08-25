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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link NullableSerializer}. */
@RunWith(Parameterized.class)
public class NullableSerializerTest extends SerializerTestBase<Integer> {
	private static final TypeSerializer<Integer> originalSerializer = IntSerializer.INSTANCE;

	@Parameterized.Parameters(name = "{0}")
	public static List<Boolean> whetherToPadNullValue() {
		return Arrays.asList(true, false);
	}

	@Parameterized.Parameter
	public boolean padNullValue;

	private TypeSerializer<Integer> nullableSerializer;

	@Before
	public void init() {
		nullableSerializer = NullableSerializer.wrapIfNullIsNotSupported(originalSerializer, padNullValue);
	}

	@Override
	protected TypeSerializer<Integer> createSerializer() {
		return NullableSerializer.wrapIfNullIsNotSupported(originalSerializer, padNullValue);
	}

	@Override
	protected int getLength() {
		return padNullValue ? 5 : -1;
	}

	@Override
	protected Class<Integer> getTypeClass() {
		return Integer.class;
	}

	@Override
	protected Integer[] getTestData() {
		return new Integer[] { 5, -1, 0, null };
	}

	@Test
	public void testWrappingNotNeeded() {
		assertEquals(NullableSerializer.wrapIfNullIsNotSupported(StringSerializer.INSTANCE, padNullValue), StringSerializer.INSTANCE);
	}

	@Test
	public void testWrappingNeeded() {
		assertTrue(nullableSerializer instanceof NullableSerializer);
		assertEquals(NullableSerializer.wrapIfNullIsNotSupported(nullableSerializer, padNullValue), nullableSerializer);
	}
}
