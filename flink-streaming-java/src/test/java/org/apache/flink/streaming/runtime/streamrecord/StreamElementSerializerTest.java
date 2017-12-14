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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link StreamElementSerializer}.
 */
public class StreamElementSerializerTest {

	@Test
	public void testDeepDuplication() {
		@SuppressWarnings("unchecked")
		TypeSerializer<Long> serializer1 = (TypeSerializer<Long>) mock(TypeSerializer.class);

		@SuppressWarnings("unchecked")
		TypeSerializer<Long> serializer2 = (TypeSerializer<Long>) mock(TypeSerializer.class);

		when(serializer1.duplicate()).thenReturn(serializer2);

		StreamElementSerializer<Long> streamRecSer =
				new StreamElementSerializer<Long>(serializer1);

		assertEquals(serializer1, streamRecSer.getContainedTypeSerializer());

		StreamElementSerializer<Long> copy = streamRecSer.duplicate();
		assertNotEquals(copy, streamRecSer);
		assertNotEquals(copy.getContainedTypeSerializer(), streamRecSer.getContainedTypeSerializer());
	}

	@Test
	public void testBasicProperties() {
		StreamElementSerializer<Long> streamRecSer =
				new StreamElementSerializer<Long>(LongSerializer.INSTANCE);

		assertFalse(streamRecSer.isImmutableType());
		assertEquals(Long.class, streamRecSer.createInstance().getValue().getClass());
		assertEquals(-1L, streamRecSer.getLength());
	}

	@Test
	public void testSerialization() throws Exception {
		final StreamElementSerializer<String> serializer =
				new StreamElementSerializer<String>(StringSerializer.INSTANCE);

		StreamRecord<String> withoutTimestamp = new StreamRecord<>("test 1 2 分享基督耶穌的愛給們，開拓雙贏!");
		assertEquals(withoutTimestamp, serializeAndDeserialize(withoutTimestamp, serializer));

		StreamRecord<String> withTimestamp = new StreamRecord<>("one more test 拓 們 分", 77L);
		assertEquals(withTimestamp, serializeAndDeserialize(withTimestamp, serializer));

		StreamRecord<String> negativeTimestamp = new StreamRecord<>("他", Long.MIN_VALUE);
		assertEquals(negativeTimestamp, serializeAndDeserialize(negativeTimestamp, serializer));

		Watermark positiveWatermark = new Watermark(13);
		assertEquals(positiveWatermark, serializeAndDeserialize(positiveWatermark, serializer));

		Watermark negativeWatermark = new Watermark(-4647654567676555876L);
		assertEquals(negativeWatermark, serializeAndDeserialize(negativeWatermark, serializer));
	}

	@SuppressWarnings("unchecked")
	private static <T, X extends StreamElement> X serializeAndDeserialize(
			X record,
			StreamElementSerializer<T> serializer) throws IOException {

		DataOutputSerializer output = new DataOutputSerializer(32);
		serializer.serialize(record, output);

		// additional binary copy step
		DataInputDeserializer copyInput = new DataInputDeserializer(output.getByteArray(), 0, output.length());
		DataOutputSerializer copyOutput = new DataOutputSerializer(32);
		serializer.copy(copyInput, copyOutput);

		DataInputDeserializer input = new DataInputDeserializer(copyOutput.getByteArray(), 0, copyOutput.length());
		return (X) serializer.deserialize(input);
	}
}
