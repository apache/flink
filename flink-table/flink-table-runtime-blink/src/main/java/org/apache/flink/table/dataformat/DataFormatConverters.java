/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.type.InternalType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Converters between internal data format and java format.
 */
public class DataFormatConverters {

	private static final Map<TypeInformation, DataFormatConverter> TYPE_INFO_TO_CONVERTER;
	static {
		Map<TypeInformation, DataFormatConverter> t2C = new HashMap<>();
		t2C.put(BasicTypeInfo.STRING_TYPE_INFO, StringConverter.INSTANCE);
		t2C.put(BasicTypeInfo.BOOLEAN_TYPE_INFO, BooleanConverter.INSTANCE);
		t2C.put(BasicTypeInfo.INT_TYPE_INFO, StringConverter.INSTANCE);
		t2C.put(BasicTypeInfo.LONG_TYPE_INFO, LongConverter.INSTANCE);
		t2C.put(BasicTypeInfo.FLOAT_TYPE_INFO, FloatConverter.INSTANCE);
		t2C.put(BasicTypeInfo.DOUBLE_TYPE_INFO, DoubleConverter.INSTANCE);
		t2C.put(BasicTypeInfo.SHORT_TYPE_INFO, ShortConverter.INSTANCE);
		t2C.put(BasicTypeInfo.BYTE_TYPE_INFO, ByteConverter.INSTANCE);
		t2C.put(BasicTypeInfo.CHAR_TYPE_INFO, CharConverter.INSTANCE);

		TYPE_INFO_TO_CONVERTER = Collections.unmodifiableMap(t2C);
	}

	public static DataFormatConverter getConverterForTypeInfo(TypeInformation typeInfo) {
		InternalType type = TYPE_INFO_TO_INTERNAL_TYPE.get(typeInfo);
		if (type != null) {
			return type;
		}

		if (BasicTypeInfo.STRING_TYPE_INFO.equals(typeInfo)) {
			return StringConverter.INSTANCE;
		} else if (BasicTypeInfo.BOOLEAN_TYPE_INFO.equals(typeInfo)) {
			return BooleanConverter.INSTANCE;
		} else if (BasicTypeInfo.INT_TYPE_INFO.equals(typeInfo)) {
			return IntConverter.INSTANCE;
		} else if (BasicTypeInfo.LONG_TYPE_INFO.equals(typeInfo)) {
			return LongConverter.INSTANCE;
		} else if (BasicTypeInfo.FLOAT_TYPE_INFO.equals(typeInfo)) {
			return FloatConverter.INSTANCE;
		} else if (BasicTypeInfo.DOUBLE_TYPE_INFO.equals(typeInfo)) {
			return DoubleConverter.INSTANCE;
		} else if (BasicTypeInfo.SHORT_TYPE_INFO.equals(typeInfo)) {
			return ShortConverter.INSTANCE;
		} else if (BasicTypeInfo.BYTE_TYPE_INFO.equals(typeInfo)) {
			return ByteConverter.INSTANCE;
		} else if (BasicTypeInfo.CHAR_TYPE_INFO.equals(typeInfo)) {
			return CharConverter.INSTANCE;
		}
	}

	/**
	 * Converter between internal data format and java format.
	 * @param <Internal> Internal data format.
	 * @param <External> External data format.
	 */
	public static abstract class DataFormatConverter<Internal, External> {

		/**
		 * Converts a external(Java) data format to its internal equivalent while automatically handling nulls.
		 */
		public final Internal toInternal(External value) {
			return value == null ? null : toInternalImpl(value);
		}

		/**
		 * Converts a non-null external(Java) data format to its internal equivalent.
		 */
		abstract Internal toInternalImpl(External value);

		/**
		 * Convert a internal data format to its external(Java) equivalent while automatically handling nulls.
		 */
		public final External toExternal(Internal value) {
			return value == null ? null : toExternalImpl(value);
		}

		/**
		 * Convert a non-null internal data format to its external(Java) equivalent.
		 */
		abstract External toExternalImpl(Internal value);

		/**
		 * Given a internalType row, convert the value at column `column` to its external(Java) equivalent.
		 * This method will only be called on non-null columns.
		 */
		abstract External toExternalImpl(BaseRow row, int column);

		/**
		 * Given a internalType row, convert the value at column `column` to its external(Java) equivalent.
		 */
		public final External toExternal(BaseRow row, int column) {
			return row.isNullAt(column) ? null : toExternalImpl(row, column);
		}
	}

	public static abstract class IdentityConverter<T> extends DataFormatConverter<T, T> {

		@Override
		T toInternalImpl(T value) {
			return value;
		}

		@Override
		T toExternalImpl(T value) {
			return value;
		}
	}

	/**
	 * Converter for String.
	 */
	public static class StringConverter extends DataFormatConverter<BinaryString, String> {

		public static final StringConverter INSTANCE = new StringConverter();

		private StringConverter() {}

		@Override
		BinaryString toInternalImpl(String value) {
			return BinaryString.fromString(value);
		}

		@Override
		String toExternalImpl(BinaryString value) {
			return value.toString();
		}

		@Override
		String toExternalImpl(BaseRow row, int column) {
			return row.getString(column).toString();
		}
	}

	public static class BooleanConverter extends IdentityConverter<Boolean> {

		public static final BooleanConverter INSTANCE = new BooleanConverter();

		private BooleanConverter() {}

		@Override
		Boolean toExternalImpl(BaseRow row, int column) {
			return row.getBoolean(column);
		}
	}

	public static class ByteConverter extends IdentityConverter<Byte> {

		public static final ByteConverter INSTANCE = new ByteConverter();

		private ByteConverter() {}

		@Override
		Byte toExternalImpl(BaseRow row, int column) {
			return row.getByte(column);
		}
	}

	public static class ShortConverter extends IdentityConverter<Short> {

		public static final ShortConverter INSTANCE = new ShortConverter();

		private ShortConverter() {}

		@Override
		Short toExternalImpl(BaseRow row, int column) {
			return row.getShort(column);
		}
	}

	public static class IntConverter extends IdentityConverter<Integer> {

		public static final IntConverter INSTANCE = new IntConverter();

		private IntConverter() {}

		@Override
		Integer toExternalImpl(BaseRow row, int column) {
			return row.getInt(column);
		}
	}

	public static class LongConverter extends IdentityConverter<Long> {

		public static final LongConverter INSTANCE = new LongConverter();

		private LongConverter() {}

		@Override
		Long toExternalImpl(BaseRow row, int column) {
			return row.getLong(column);
		}
	}

	public static class FloatConverter extends IdentityConverter<Float> {

		public static final FloatConverter INSTANCE = new FloatConverter();

		private FloatConverter() {}

		@Override
		Float toExternalImpl(BaseRow row, int column) {
			return row.getFloat(column);
		}
	}

	public static class DoubleConverter extends IdentityConverter<Double> {

		public static final DoubleConverter INSTANCE = new DoubleConverter();

		private DoubleConverter() {}

		@Override
		Double toExternalImpl(BaseRow row, int column) {
			return row.getDouble(column);
		}
	}

	public static class CharConverter extends IdentityConverter<Character> {

		public static final CharConverter INSTANCE = new CharConverter();

		private CharConverter() {}

		@Override
		Character toExternalImpl(BaseRow row, int column) {
			return row.getChar(column);
		}
	}

}
