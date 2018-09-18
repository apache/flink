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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link NullableSerializer}. */
public class NullableSerializerTest {
	private static final TypeSerializer<Integer> originalSerializer = IntSerializer.INSTANCE;
	private static final TypeSerializer<Integer> nullableSerializer =
		NullableSerializer.wrapIfNullIsNotSupported(originalSerializer);
	private static final DataOutputSerializer dov = new DataOutputSerializer(
		nullableSerializer.getLength() > 0 ? nullableSerializer.getLength() : 0);
	private static final DataInputDeserializer div = new DataInputDeserializer();

	@Test
	public void testWrappingNotNeeded() {
		assertEquals(NullableSerializer.wrapIfNullIsNotSupported(StringSerializer.INSTANCE), StringSerializer.INSTANCE);
	}

	@Test
	public void testWrappingNeeded() {
		assertTrue(nullableSerializer instanceof NullableSerializer);
		assertEquals(NullableSerializer.wrapIfNullIsNotSupported(nullableSerializer), nullableSerializer);
	}

	@Test
	public void testNonNullSerialization() throws IOException {
		dov.clear();
		nullableSerializer.serialize(5, dov);

		div.setBuffer(dov.getSharedBuffer());
		assertEquals((int) nullableSerializer.deserialize(div), 5);
		div.setBuffer(dov.getSharedBuffer());
		assertEquals((int) nullableSerializer.deserialize(10, div), 5);
		div.setBuffer(dov.getSharedBuffer());
		assertEquals((int) nullableSerializer.deserialize(null, div), 5);
	}

	@Test
	public void testNullSerialization() throws IOException {
		dov.clear();
		nullableSerializer.serialize(null, dov);

		div.setBuffer(dov.getSharedBuffer());
		assertNull(nullableSerializer.deserialize(div));
		div.setBuffer(dov.getSharedBuffer());
		assertNull(nullableSerializer.deserialize(10, div));
		div.setBuffer(dov.getSharedBuffer());
		assertNull(nullableSerializer.deserialize(null, div));
	}
}
