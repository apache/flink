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
package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;

/**
 * The byte-wise comparator.
 *
 * @param <T>
 */
public class BytewiseComparator<T> implements Comparator<T>, Serializable {

	private static final long serialVersionUID = -854039422114672899L;

	public final static BytewiseComparator<byte[]> BYTEARRAY_INSTANCE = new BytewiseComparator<>(ComparableByteArraySerializer.INSTANCE);

	/**
	 * NOTE: The comparison of the serialized forms are identical to that of
	 * the values only when the numbers to compare are both not negative.
	 */
	public final static BytewiseComparator<Byte> BYTE_INSTANCE = new BytewiseComparator<>(ByteSerializer.INSTANCE);
	public final static BytewiseComparator<Integer> INT_INSTANCE = new BytewiseComparator<>(IntSerializer.INSTANCE);
	public final static BytewiseComparator<Long> LONG_INSTANCE = new BytewiseComparator<>(LongSerializer.INSTANCE);
	public final static BytewiseComparator<Float> FLOAT_INSTANCE = new BytewiseComparator<>(FloatSerializer.INSTANCE);
	public final static BytewiseComparator<Double> DOUBLE_INSTANCE = new BytewiseComparator<>(DoubleSerializer.INSTANCE);

	private final TypeSerializer<T> serializer;

	public BytewiseComparator(TypeSerializer<T> serializer) {
		this.serializer = Preconditions.checkNotNull(serializer, "The serializer cannot be null.");
	}

	public static int compareBytes(byte[] leftBytes, byte[] rightBytes) {
		return compareBytes(leftBytes, 0, leftBytes.length, rightBytes, 0, rightBytes.length);
	}

	public static int compareBytes(byte[] leftBytes, int leftOffset, int leftLength, byte[] rightBytes, int rightOffset, int rightLength) {
		int length = Math.min(leftLength, rightLength);

		for (int i = 0; i < length; ++i) {
			if (leftBytes[leftOffset + i] != rightBytes[rightOffset + i]) {
				return (leftBytes[leftOffset + i] & 0xFF) - (rightBytes[rightOffset + i] & 0xFF);
			}
		}

		return leftLength - rightLength;
	}

	public TypeSerializer<T> getSerializer() {
		return serializer;
	}

	@Override
	public int compare(T left, T right) {
		ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
		DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);

		try {
			serializer.serialize(left, outputView);
			byte[] leftBytes = outputStream.toByteArray();

			outputStream.reset();

			serializer.serialize(right, outputView);
			byte[] rightBytes = outputStream.toByteArray();

			return compareBytes(leftBytes, rightBytes);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		} else if (o == null || o.getClass() != getClass()) {
			return false;
		}

		return (((BytewiseComparator)o).serializer.equals(serializer));
	}

	@Override
	public String toString() {
		return "BytewiseComparator(" +
			serializer.toString() +
			")";
	}

	private static class ComparableByteArraySerializer extends TypeSerializerSingleton<byte[]> {

		static final ComparableByteArraySerializer INSTANCE = new ComparableByteArraySerializer();

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public byte[] createInstance() {
			return new byte[0];
		}

		@Override
		public byte[] copy(byte[] from) {
			byte[] to = new byte[from.length];
			System.arraycopy(from, 0, to, 0, from.length);
			return to;
		}

		@Override
		public byte[] copy(byte[] from, byte[] reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(byte[] record, DataOutputView target) throws IOException {
			if (record == null) {
				throw new IllegalArgumentException("The record must not be null.");
			}

			target.skipBytesToWrite(1);
			target.write(record);
		}

		@Override
		public byte[] deserialize(DataInputView source) throws IOException {
			ByteArrayOutputStreamWithPos output = new ByteArrayOutputStreamWithPos();
			byte[] buffer = new byte[1024];
			int numBytes;

			source.skipBytesToRead(1);
			while ((numBytes = source.read(buffer)) != -1) {
				output.write(buffer, 0, numBytes);
			}

			return output.toByteArray();
		}

		@Override
		public byte[] deserialize(byte[] reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			byte[] buffer = new byte[1024];
			int numBytes;

			while ((numBytes = source.read(buffer)) != -1) {
				target.write(buffer, 0, numBytes);
			}
		}

		@Override
		public boolean equals(Object obj) {
			return (this == obj) ||
				(obj instanceof ComparableByteArraySerializer);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof ComparableByteArraySerializer;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}

		@Override
		public String toString() {
			return "ComparableByteArraySerializer";
		}
	}
}
