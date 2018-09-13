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
