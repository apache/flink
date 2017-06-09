/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.python.api.streaming.util;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.python.api.types.CustomTypeWrapper;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Utility class containing serializers for all supported types.
 */
public class SerializationUtils {
	public static final byte TYPE_BOOLEAN = 34;
	public static final byte TYPE_BYTE = 33;
	public static final byte TYPE_INTEGER = 32;
	public static final byte TYPE_LONG = 31;
	public static final byte TYPE_DOUBLE = 30;
	public static final byte TYPE_FLOAT = 29;
	public static final byte TYPE_STRING = 28;
	public static final byte TYPE_BYTES = 27;
	public static final byte TYPE_NULL = 26;

	private enum SupportedTypes {
		TUPLE, BOOLEAN, BYTE, BYTES, INTEGER, LONG, FLOAT, DOUBLE, STRING, NULL, CUSTOMTYPEWRAPPER
	}

	@SuppressWarnings("unchecked")
	public static <IN> Serializer<IN> getSerializer(IN value) {
		String className = value.getClass().getSimpleName().toUpperCase();
		if (className.startsWith("TUPLE")) {
			className = "TUPLE";
		}
		if (className.startsWith("BYTE[]")) {
			className = "BYTES";
		}
		SupportedTypes type = SupportedTypes.valueOf(className);
		Serializer<?> serializer;
		switch (type) {
			case TUPLE:
				serializer = new TupleSerializer((Tuple) value);
				break;
			case BOOLEAN:
				serializer = new BooleanSerializer();
				break;
			case BYTE:
				serializer = new ByteSerializer();
				break;
			case BYTES:
				serializer = new BytesSerializer();
				break;
			case INTEGER:
				serializer = new IntSerializer();
				break;
			case LONG:
				serializer = new LongSerializer();
				break;
			case STRING:
				serializer = new StringSerializer();
				break;
			case FLOAT:
				serializer = new FloatSerializer();
				break;
			case DOUBLE:
				serializer = new DoubleSerializer();
				break;
			case NULL:
				serializer = new NullSerializer();
				break;
			case CUSTOMTYPEWRAPPER:
				serializer = new CustomTypeWrapperSerializer((CustomTypeWrapper) value);
				break;
			default:
				throw new IllegalArgumentException("Unsupported Type encountered: " + type);
		}
		return (Serializer<IN>) serializer;
	}

	/**
	 * Super class for all serializers used to serialize data. These serializers are used to serialize values emitted
	 * from java input formats.
	 *
	 * <p>These serializer smut be kept in sync with the python counterparts.
	 *
	 * @param <IN> input type
	 */
	public abstract static class Serializer<IN> {
		private byte[] typeInfo = null;

		public byte[] serialize(IN value) {
			if (typeInfo == null) {
				typeInfo = new byte[getTypeInfoSize()];
				ByteBuffer typeBuffer = ByteBuffer.wrap(typeInfo);
				putTypeInfo(typeBuffer);
			}
			byte[] bytes = serializeWithoutTypeInfo(value);
			byte[] total = new byte[typeInfo.length + bytes.length];
			ByteBuffer.wrap(total).put(typeInfo).put(bytes);
			return total;
		}

		public abstract byte[] serializeWithoutTypeInfo(IN value);

		protected abstract void putTypeInfo(ByteBuffer buffer);

		protected int getTypeInfoSize() {
			return 1;
		}
	}

	/**
	 * A serializer for {@link CustomTypeWrapper CustomTypeWrappers}.
	 */
	public static class CustomTypeWrapperSerializer extends Serializer<CustomTypeWrapper> {
		private final byte type;

		public CustomTypeWrapperSerializer(CustomTypeWrapper value) {
			this.type = value.getType();
		}

		@Override
		public byte[] serializeWithoutTypeInfo(CustomTypeWrapper value) {
			byte[] result = new byte[4 + value.getData().length];
			ByteBuffer.wrap(result).putInt(value.getData().length).put(value.getData());
			return result;
		}

		@Override
		public void putTypeInfo(ByteBuffer buffer) {
			buffer.put(type);
		}
	}

	/**
	 * A serializer for bytes.
	 */
	public static class ByteSerializer extends Serializer<Byte> {
		@Override
		public byte[] serializeWithoutTypeInfo(Byte value) {
			return new byte[]{value};
		}

		@Override
		public void putTypeInfo(ByteBuffer buffer) {
			buffer.put(TYPE_BYTE);
		}
	}

	/**
	 * A serializer for booleans.
	 */
	public static class BooleanSerializer extends Serializer<Boolean> {
		@Override
		public byte[] serializeWithoutTypeInfo(Boolean value) {
			return new byte[]{(byte) (value ? 1 : 0)};
		}

		@Override
		public void putTypeInfo(ByteBuffer buffer) {
			buffer.put(TYPE_BOOLEAN);
		}
	}

	/**
	 * A serializer for ints.
	 */
	public static class IntSerializer extends Serializer<Integer> {
		@Override
		public byte[] serializeWithoutTypeInfo(Integer value) {
			byte[] data = new byte[4];
			ByteBuffer.wrap(data).putInt(value);
			return data;
		}

		@Override
		public void putTypeInfo(ByteBuffer buffer) {
			buffer.put(TYPE_INTEGER);
		}
	}

	/**
	 * A serializer for longs.
	 */
	public static class LongSerializer extends Serializer<Long> {
		@Override
		public byte[] serializeWithoutTypeInfo(Long value) {
			byte[] data = new byte[8];
			ByteBuffer.wrap(data).putLong(value);
			return data;
		}

		@Override
		public void putTypeInfo(ByteBuffer buffer) {
			buffer.put(TYPE_LONG);
		}
	}

	/**
	 * A serializer for strings.
	 */
	public static class StringSerializer extends Serializer<String> {
		@Override
		public byte[] serializeWithoutTypeInfo(String value) {
			byte[] string = value.getBytes(ConfigConstants.DEFAULT_CHARSET);
			byte[] data = new byte[4 + string.length];
			ByteBuffer.wrap(data).putInt(string.length).put(string);
			return data;
		}

		@Override
		public void putTypeInfo(ByteBuffer buffer) {
			buffer.put(TYPE_STRING);
		}
	}

	/**
	 * A serializer for floats.
	 */
	public static class FloatSerializer extends Serializer<Float> {
		@Override
		public byte[] serializeWithoutTypeInfo(Float value) {
			byte[] data = new byte[4];
			ByteBuffer.wrap(data).putFloat(value);
			return data;
		}

		@Override
		public void putTypeInfo(ByteBuffer buffer) {
			buffer.put(TYPE_FLOAT);
		}
	}

	/**
	 * A serializer for doubles.
	 */
	public static class DoubleSerializer extends Serializer<Double> {
		@Override
		public byte[] serializeWithoutTypeInfo(Double value) {
			byte[] data = new byte[8];
			ByteBuffer.wrap(data).putDouble(value);
			return data;
		}

		@Override
		public void putTypeInfo(ByteBuffer buffer) {
			buffer.put(TYPE_DOUBLE);
		}
	}

	/**
	 * A serializer for null.
	 */
	public static class NullSerializer extends Serializer<Object> {
		@Override
		public byte[] serializeWithoutTypeInfo(Object value) {
			return new byte[0];
		}

		@Override
		public void putTypeInfo(ByteBuffer buffer) {
			buffer.put(TYPE_NULL);
		}
	}

	/**
	 * A serializer for byte arrays.
	 */
	public static class BytesSerializer extends Serializer<byte[]> {
		@Override
		public byte[] serializeWithoutTypeInfo(byte[] value) {
			byte[] data = new byte[4 + value.length];
			ByteBuffer.wrap(data).putInt(value.length).put(value);
			return data;
		}

		@Override
		public void putTypeInfo(ByteBuffer buffer) {
			buffer.put(TYPE_BYTES);
		}
	}

	/**
	 * A serializer for tuples.
	 */
	public static class TupleSerializer extends Serializer<Tuple> {
		private final Serializer<Object>[] serializer;

		public TupleSerializer(Tuple value) {
			serializer = new Serializer[value.getArity()];
			for (int x = 0; x < serializer.length; x++) {
				serializer[x] = getSerializer(value.getField(x));
			}
		}

		@Override
		public byte[] serializeWithoutTypeInfo(Tuple value) {
			ArrayList<byte[]> bits = new ArrayList<>();

			int totalSize = 0;
			for (int x = 0; x < serializer.length; x++) {
				byte[] bit = serializer[x].serializeWithoutTypeInfo(value.getField(x));
				bits.add(bit);
				totalSize += bit.length;
			}
			int pointer = 0;
			byte[] data = new byte[totalSize];
			for (byte[] bit : bits) {
				System.arraycopy(bit, 0, data, pointer, bit.length);
				pointer += bit.length;
			}
			return data;
		}

		@Override
		public void putTypeInfo(ByteBuffer buffer) {
			buffer.put((byte) serializer.length);
			for (Serializer<Object> s : serializer) {
				s.putTypeInfo(buffer);
			}
		}

		@Override
		public int getTypeInfoSize() {
			int size = 1;
			for (Serializer<Object> s : serializer) {
				size += s.getTypeInfoSize();
			}
			return size;
		}
	}
}
