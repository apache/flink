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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class StreamRecordSerializerTest {
	
	@Test
	public void testDeepDuplication() {
		try {
			@SuppressWarnings("unchecked")
			TypeSerializer<Long> serializer1 = (TypeSerializer<Long>) mock(TypeSerializer.class);
			@SuppressWarnings("unchecked")
			TypeSerializer<Long> serializer2 = (TypeSerializer<Long>) mock(TypeSerializer.class);
			
			when(serializer1.duplicate()).thenReturn(serializer2);
			
			StreamRecordSerializer<Long> streamRecSer = new StreamRecordSerializer<Long>(serializer1);
			assertEquals(serializer1, streamRecSer.getContainedTypeSerializer());
			
			StreamRecordSerializer<Long> copy = streamRecSer.duplicate();
			assertNotEquals(copy, streamRecSer);
			assertNotEquals(copy.getContainedTypeSerializer(), streamRecSer.getContainedTypeSerializer());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testBasicProperties() {
		try {
			StreamRecordSerializer<Long> streamRecSer = new StreamRecordSerializer<Long>(LongSerializer.INSTANCE);
			
			assertFalse(streamRecSer.isImmutableType());
			assertEquals(Long.class, streamRecSer.createInstance().getValue().getClass());
			assertEquals(LongSerializer.INSTANCE.getLength(), streamRecSer.getLength());
			
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testDeserializedValuesHaveNoTimestamps() throws Exception {
		final StreamRecord<Long> original = new StreamRecord<>(42L);
		
		StreamRecordSerializer<Long> streamRecSer = new StreamRecordSerializer<Long>(LongSerializer.INSTANCE);

		DataOutputSerializer buffer = new DataOutputSerializer(16);
		streamRecSer.serialize(original, buffer);
		
		DataInputDeserializer input = new DataInputDeserializer(buffer.getByteArray(), 0, buffer.length());
		StreamRecord<Long> result = streamRecSer.deserialize(input);
		
		assertFalse(result.hasTimestamp());
		assertEquals(original, result);
	}
}
