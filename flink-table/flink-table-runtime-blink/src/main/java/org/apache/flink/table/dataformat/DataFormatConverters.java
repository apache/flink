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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.table.runtime.functions.DateTimeUtils;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.TypeConverters;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.typeutils.BinaryArrayTypeInfo;
import org.apache.flink.table.typeutils.BinaryMapTypeInfo;
import org.apache.flink.table.typeutils.BinaryStringTypeInfo;
import org.apache.flink.types.Row;

import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import scala.Product;

/**
 * Converters between internal data format and java format.
 *
 * <p>The following scenarios will use converter for java format to internal data format:
 * In source, data from user define source to internal sql engine.
 * In udx return value, User outputs java format data to the SQL engine.
 *
 * <p>The following scenarios will use converter for internal data format to java format:
 * In udx method parameters, data from internal sql engine need to be provided to user udx.
 * In sink, data from internal sql engine need to be provided to user define sink.
 */
public class DataFormatConverters {

	private static final Map<TypeInformation, DataFormatConverter> TYPE_INFO_TO_CONVERTER;
	static {
		Map<TypeInformation, DataFormatConverter> t2C = new HashMap<>();
		t2C.put(BasicTypeInfo.STRING_TYPE_INFO, StringConverter.INSTANCE);
		t2C.put(BasicTypeInfo.BOOLEAN_TYPE_INFO, BooleanConverter.INSTANCE);
		t2C.put(BasicTypeInfo.INT_TYPE_INFO, IntConverter.INSTANCE);
		t2C.put(BasicTypeInfo.LONG_TYPE_INFO, LongConverter.INSTANCE);
		t2C.put(BasicTypeInfo.FLOAT_TYPE_INFO, FloatConverter.INSTANCE);
		t2C.put(BasicTypeInfo.DOUBLE_TYPE_INFO, DoubleConverter.INSTANCE);
		t2C.put(BasicTypeInfo.SHORT_TYPE_INFO, ShortConverter.INSTANCE);
		t2C.put(BasicTypeInfo.BYTE_TYPE_INFO, ByteConverter.INSTANCE);
		t2C.put(BasicTypeInfo.CHAR_TYPE_INFO, CharConverter.INSTANCE);

		t2C.put(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveBooleanArrayConverter.INSTANCE);
		t2C.put(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveIntArrayConverter.INSTANCE);
		t2C.put(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveLongArrayConverter.INSTANCE);
		t2C.put(PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveFloatArrayConverter.INSTANCE);
		t2C.put(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveDoubleArrayConverter.INSTANCE);
		t2C.put(PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveShortArrayConverter.INSTANCE);
		t2C.put(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveByteArrayConverter.INSTANCE);
		t2C.put(PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveCharArrayConverter.INSTANCE);

		t2C.put(SqlTimeTypeInfo.DATE, DateConverter.INSTANCE);
		t2C.put(SqlTimeTypeInfo.TIME, TimeConverter.INSTANCE);
		t2C.put(SqlTimeTypeInfo.TIMESTAMP, TimestampConverter.INSTANCE);

		t2C.put(BinaryStringTypeInfo.INSTANCE, BinaryStringConverter.INSTANCE);

		TYPE_INFO_TO_CONVERTER = Collections.unmodifiableMap(t2C);
	}

	/**
	 * Get {@link DataFormatConverter} for {@link TypeInformation}.
	 *
	 * @param typeInfo DataFormatConverter is oriented to Java format, while InternalType has
	 *                   lost its specific Java format. Only TypeInformation retains all its
	 *                   Java format information.
	 */
	public static DataFormatConverter getConverterForTypeInfo(TypeInformation typeInfo) {
		DataFormatConverter converter = TYPE_INFO_TO_CONVERTER.get(typeInfo);
		if (converter != null) {
			return converter;
		}

		if (typeInfo instanceof BasicArrayTypeInfo) {
			return new ObjectArrayConverter(((BasicArrayTypeInfo) typeInfo).getComponentInfo());
		} else if (typeInfo instanceof ObjectArrayTypeInfo) {
			return new ObjectArrayConverter(((ObjectArrayTypeInfo) typeInfo).getComponentInfo());
		} else if (typeInfo instanceof MapTypeInfo) {
			MapTypeInfo mapType = (MapTypeInfo) typeInfo;
			return new MapConverter(mapType.getKeyTypeInfo(), mapType.getValueTypeInfo());
		} else if (typeInfo instanceof RowTypeInfo) {
			return new RowConverter((RowTypeInfo) typeInfo);
		} else if (typeInfo instanceof PojoTypeInfo) {
			return new PojoConverter((PojoTypeInfo) typeInfo);
		} else if (typeInfo instanceof TupleTypeInfo) {
			return new TupleConverter((TupleTypeInfo) typeInfo);
		} else if (typeInfo instanceof TupleTypeInfoBase && Product.class.isAssignableFrom(typeInfo.getTypeClass())) {
			return new CaseClassConverter((TupleTypeInfoBase) typeInfo);
		} else if (typeInfo instanceof BinaryArrayTypeInfo) {
			return BinaryArrayConverter.INSTANCE;
		} else if (typeInfo instanceof BinaryMapTypeInfo) {
			return BinaryMapConverter.INSTANCE;
		} else if (typeInfo instanceof BaseRowTypeInfo) {
			return BaseRowConverter.INSTANCE;
		} else {
			throw new RuntimeException("Not support generic yet: " + typeInfo);
		}
	}

	/**
	 * Converter between internal data format and java format.
	 *
	 * @param <Internal> Internal data format.
	 * @param <External> External data format.
	 */
	public abstract static class DataFormatConverter<Internal, External> {

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

	/**
	 * Identity converter.
	 */
	public abstract static class IdentityConverter<T> extends DataFormatConverter<T, T> {

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
	 * Converter for boolean.
	 */
	public static class BooleanConverter extends IdentityConverter<Boolean> {

		public static final BooleanConverter INSTANCE = new BooleanConverter();

		private BooleanConverter() {}

		@Override
		Boolean toExternalImpl(BaseRow row, int column) {
			return row.getBoolean(column);
		}
	}

	/**
	 * Converter for byte.
	 */
	public static class ByteConverter extends IdentityConverter<Byte> {

		public static final ByteConverter INSTANCE = new ByteConverter();

		private ByteConverter() {}

		@Override
		Byte toExternalImpl(BaseRow row, int column) {
			return row.getByte(column);
		}
	}

	/**
	 * Converter for short.
	 */
	public static class ShortConverter extends IdentityConverter<Short> {

		public static final ShortConverter INSTANCE = new ShortConverter();

		private ShortConverter() {}

		@Override
		Short toExternalImpl(BaseRow row, int column) {
			return row.getShort(column);
		}
	}

	/**
	 * Converter for int.
	 */
	public static class IntConverter extends IdentityConverter<Integer> {

		public static final IntConverter INSTANCE = new IntConverter();

		private IntConverter() {}

		@Override
		Integer toExternalImpl(BaseRow row, int column) {
			return row.getInt(column);
		}
	}

	/**
	 * Converter for long.
	 */
	public static class LongConverter extends IdentityConverter<Long> {

		public static final LongConverter INSTANCE = new LongConverter();

		private LongConverter() {}

		@Override
		Long toExternalImpl(BaseRow row, int column) {
			return row.getLong(column);
		}
	}

	/**
	 * Converter for float.
	 */
	public static class FloatConverter extends IdentityConverter<Float> {

		public static final FloatConverter INSTANCE = new FloatConverter();

		private FloatConverter() {}

		@Override
		Float toExternalImpl(BaseRow row, int column) {
			return row.getFloat(column);
		}
	}

	/**
	 * Converter for double.
	 */
	public static class DoubleConverter extends IdentityConverter<Double> {

		public static final DoubleConverter INSTANCE = new DoubleConverter();

		private DoubleConverter() {}

		@Override
		Double toExternalImpl(BaseRow row, int column) {
			return row.getDouble(column);
		}
	}

	/**
	 * Converter for char.
	 */
	public static class CharConverter extends IdentityConverter<Character> {

		public static final CharConverter INSTANCE = new CharConverter();

		private CharConverter() {}

		@Override
		Character toExternalImpl(BaseRow row, int column) {
			return row.getChar(column);
		}
	}

	/**
	 * Converter for BinaryString.
	 */
	public static class BinaryStringConverter extends IdentityConverter<BinaryString> {

		public static final BinaryStringConverter INSTANCE = new BinaryStringConverter();

		private BinaryStringConverter() {}

		@Override
		BinaryString toExternalImpl(BaseRow row, int column) {
			return row.getString(column);
		}
	}

	/**
	 * Converter for BinaryArray.
	 */
	public static class BinaryArrayConverter extends IdentityConverter<BinaryArray> {

		public static final BinaryArrayConverter INSTANCE = new BinaryArrayConverter();

		private BinaryArrayConverter() {}

		@Override
		BinaryArray toExternalImpl(BaseRow row, int column) {
			return row.getArray(column);
		}
	}

	/**
	 * Converter for BinaryMap.
	 */
	public static class BinaryMapConverter extends IdentityConverter<BinaryMap> {

		public static final BinaryMapConverter INSTANCE = new BinaryMapConverter();

		private BinaryMapConverter() {}

		@Override
		BinaryMap toExternalImpl(BaseRow row, int column) {
			return row.getMap(column);
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

	/**
	 * Converter for date.
	 */
	public static class DateConverter extends DataFormatConverter<Integer, Date> {

		public static final DateConverter INSTANCE = new DateConverter();

		private DateConverter() {}

		@Override
		Integer toInternalImpl(Date value) {
			return DateTimeUtils.toInt(value);
		}

		@Override
		Date toExternalImpl(Integer value) {
			return DateTimeUtils.internalToDate(value);
		}

		@Override
		Date toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getInt(column));
		}
	}

	/**
	 * Converter for time.
	 */
	public static class TimeConverter extends DataFormatConverter<Integer, Time> {

		public static final TimeConverter INSTANCE = new TimeConverter();

		private TimeConverter() {}

		@Override
		Integer toInternalImpl(Time value) {
			return DateTimeUtils.toInt(value);
		}

		@Override
		Time toExternalImpl(Integer value) {
			return DateTimeUtils.internalToTime(value);
		}

		@Override
		Time toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getInt(column));
		}
	}

	/**
	 * Converter for timestamp.
	 */
	public static class TimestampConverter extends DataFormatConverter<Long, Timestamp> {

		public static final TimestampConverter INSTANCE = new TimestampConverter();

		private TimestampConverter() {}

		@Override
		Long toInternalImpl(Timestamp value) {
			return DateTimeUtils.toLong(value);
		}

		@Override
		Timestamp toExternalImpl(Long value) {
			return DateTimeUtils.internalToTimestamp(value);
		}

		@Override
		Timestamp toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getLong(column));
		}
	}

	/**
	 * Converter for primitive int array.
	 */
	public static class PrimitiveIntArrayConverter extends DataFormatConverter<BinaryArray, int[]> {

		public static final PrimitiveIntArrayConverter INSTANCE = new PrimitiveIntArrayConverter();

		private PrimitiveIntArrayConverter() {}

		@Override
		BinaryArray toInternalImpl(int[] value) {
			return BinaryArray.fromPrimitiveArray(value);
		}

		@Override
		int[] toExternalImpl(BinaryArray value) {
			return value.toIntArray();
		}

		@Override
		int[] toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for primitive boolean array.
	 */
	public static class PrimitiveBooleanArrayConverter extends DataFormatConverter<BinaryArray, boolean[]> {

		public static final PrimitiveBooleanArrayConverter INSTANCE = new PrimitiveBooleanArrayConverter();

		private PrimitiveBooleanArrayConverter() {}

		@Override
		BinaryArray toInternalImpl(boolean[] value) {
			return BinaryArray.fromPrimitiveArray(value);
		}

		@Override
		boolean[] toExternalImpl(BinaryArray value) {
			return value.toBooleanArray();
		}

		@Override
		boolean[] toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for primitive byte array.
	 */
	public static class PrimitiveByteArrayConverter extends DataFormatConverter<BinaryArray, byte[]> {

		public static final PrimitiveByteArrayConverter INSTANCE = new PrimitiveByteArrayConverter();

		private PrimitiveByteArrayConverter() {}

		@Override
		BinaryArray toInternalImpl(byte[] value) {
			return BinaryArray.fromPrimitiveArray(value);
		}

		@Override
		byte[] toExternalImpl(BinaryArray value) {
			return value.toByteArray();
		}

		@Override
		byte[] toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for primitive short array.
	 */
	public static class PrimitiveShortArrayConverter extends DataFormatConverter<BinaryArray, short[]> {

		public static final PrimitiveShortArrayConverter INSTANCE = new PrimitiveShortArrayConverter();

		private PrimitiveShortArrayConverter() {}

		@Override
		BinaryArray toInternalImpl(short[] value) {
			return BinaryArray.fromPrimitiveArray(value);
		}

		@Override
		short[] toExternalImpl(BinaryArray value) {
			return value.toShortArray();
		}

		@Override
		short[] toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for primitive long array.
	 */
	public static class PrimitiveLongArrayConverter extends DataFormatConverter<BinaryArray, long[]> {

		public static final PrimitiveLongArrayConverter INSTANCE = new PrimitiveLongArrayConverter();

		private PrimitiveLongArrayConverter() {}

		@Override
		BinaryArray toInternalImpl(long[] value) {
			return BinaryArray.fromPrimitiveArray(value);
		}

		@Override
		long[] toExternalImpl(BinaryArray value) {
			return value.toLongArray();
		}

		@Override
		long[] toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for primitive float array.
	 */
	public static class PrimitiveFloatArrayConverter extends DataFormatConverter<BinaryArray, float[]> {

		public static final PrimitiveFloatArrayConverter INSTANCE = new PrimitiveFloatArrayConverter();

		private PrimitiveFloatArrayConverter() {}

		@Override
		BinaryArray toInternalImpl(float[] value) {
			return BinaryArray.fromPrimitiveArray(value);
		}

		@Override
		float[] toExternalImpl(BinaryArray value) {
			return value.toFloatArray();
		}

		@Override
		float[] toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for primitive double array.
	 */
	public static class PrimitiveDoubleArrayConverter extends DataFormatConverter<BinaryArray, double[]> {

		public static final PrimitiveDoubleArrayConverter INSTANCE = new PrimitiveDoubleArrayConverter();

		private PrimitiveDoubleArrayConverter() {}

		@Override
		BinaryArray toInternalImpl(double[] value) {
			return BinaryArray.fromPrimitiveArray(value);
		}

		@Override
		double[] toExternalImpl(BinaryArray value) {
			return value.toDoubleArray();
		}

		@Override
		double[] toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for primitive char array.
	 */
	public static class PrimitiveCharArrayConverter extends DataFormatConverter<BinaryArray, char[]> {

		public static final PrimitiveCharArrayConverter INSTANCE = new PrimitiveCharArrayConverter();

		private PrimitiveCharArrayConverter() {}

		@Override
		BinaryArray toInternalImpl(char[] value) {
			return BinaryArray.fromPrimitiveArray(value);
		}

		@Override
		char[] toExternalImpl(BinaryArray value) {
			return value.toCharArray();
		}

		@Override
		char[] toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for object array.
	 */
	public static class ObjectArrayConverter<T> extends DataFormatConverter<BinaryArray, T[]> {

		private final InternalType elementType;
		private final DataFormatConverter<Object, T> elementConverter;
		private final Class<T> componentClass;
		private final int elementSize;

		public ObjectArrayConverter(TypeInformation<T> elementTypeInfo) {
			this.elementType = TypeConverters.createInternalTypeFromTypeInfo(elementTypeInfo);
			this.elementConverter = DataFormatConverters.getConverterForTypeInfo(elementTypeInfo);
			this.componentClass = elementTypeInfo.getTypeClass();
			this.elementSize = BinaryArray.calculateFixLengthPartSize(elementType);
		}

		@Override
		BinaryArray toInternalImpl(T[] value) {
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, value.length, elementSize);
			for (int i = 0; i < value.length; i++) {
				Object field = value[i];
				if (field == null) {
					// if we reuse BinaryArrayWriter, we need invoke setNullInt.
					writer.setNullAt(i);
				} else {
					BinaryWriter.write(writer, i, elementConverter.toInternalImpl(value[i]), elementType);
				}
			}
			writer.complete();
			return array;
		}

		@Override
		T[] toExternalImpl(BinaryArray value) {
			return binaryArrayToJavaArray(value, elementType, componentClass, elementConverter);
		}

		@Override
		T[] toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	private static  <T> T[] binaryArrayToJavaArray(BinaryArray value, InternalType elementType,
			Class<T> componentClass, DataFormatConverter<Object, T> elementConverter) {
		int size = value.numElements();
		T[] values = (T[]) Array.newInstance(componentClass, size);
		for (int i = 0; i < size; i++) {
			if (value.isNullAt(i)) {
				values[i] = null;
			} else {
				values[i] = elementConverter.toExternalImpl(
						TypeGetterSetters.get(value, i, elementType));
			}
		}
		return values;
	}

	/**
	 * Converter for map.
	 */
	public static class MapConverter extends DataFormatConverter<BinaryMap, Map> {

		private final InternalType keyType;
		private final InternalType valueType;

		private final DataFormatConverter keyConverter;
		private final DataFormatConverter valueConverter;

		private final int keyElementSize;
		private final int valueElementSize;

		private final Class keyComponentClass;
		private final Class valueComponentClass;

		public MapConverter(TypeInformation keyTypeInfo, TypeInformation valueTypeInfo) {
			this.keyType = TypeConverters.createInternalTypeFromTypeInfo(keyTypeInfo);
			this.valueType = TypeConverters.createInternalTypeFromTypeInfo(valueTypeInfo);
			this.keyConverter = DataFormatConverters.getConverterForTypeInfo(keyTypeInfo);
			this.valueConverter = DataFormatConverters.getConverterForTypeInfo(valueTypeInfo);
			this.keyElementSize = BinaryArray.calculateFixLengthPartSize(keyType);
			this.valueElementSize = BinaryArray.calculateFixLengthPartSize(valueType);
			this.keyComponentClass = keyTypeInfo.getTypeClass();
			this.valueComponentClass = valueTypeInfo.getTypeClass();
		}

		@Override
		BinaryMap toInternalImpl(Map value) {
			BinaryArray keyArray = new BinaryArray();
			BinaryArrayWriter keyWriter = new BinaryArrayWriter(keyArray, value.size(), keyElementSize);

			BinaryArray valueArray = new BinaryArray();
			BinaryArrayWriter valueWriter = new BinaryArrayWriter(valueArray, value.size(), valueElementSize);

			int i = 0;
			for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
				// if we reuse BinaryArrayWriter, we need invoke setNullInt.
				if (entry.getKey() == null) {
					keyWriter.setNullAt(i);
				} else {
					BinaryWriter.write(keyWriter, i, keyConverter.toInternalImpl(entry.getKey()), keyType);
				}
				if (entry.getValue() == null) {
					valueWriter.setNullAt(i);
				} else {
					BinaryWriter.write(valueWriter, i, valueConverter.toInternalImpl(entry.getValue()), valueType);
				}
				i++;
			}

			keyWriter.complete();
			valueWriter.complete();
			return BinaryMap.valueOf(keyArray, valueArray);
		}

		@Override
		Map toExternalImpl(BinaryMap value) {
			Map<Object, Object> map = new HashMap<>();
			Object[] keys = binaryArrayToJavaArray(value.keyArray(), keyType, keyComponentClass, keyConverter);
			Object[] values = binaryArrayToJavaArray(value.valueArray(), valueType, valueComponentClass, valueConverter);
			for (int i = 0; i < value.numElements(); i++) {
				map.put(keys[i], values[i]);
			}
			return map;
		}

		@Override
		Map toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getMap(column));
		}
	}

	/**
	 * Abstract converter for internal base row.
	 */
	public abstract static class AbstractBaseRowConverter<I extends BaseRow, E> extends DataFormatConverter<I, E> {

		protected final DataFormatConverter[] converters;

		public AbstractBaseRowConverter(CompositeType t) {
			converters = new DataFormatConverter[t.getArity()];
			for (int i = 0; i < t.getArity(); i++) {
				converters[i] = getConverterForTypeInfo(t.getTypeAt(i));
			}
		}

		@Override
		E toExternalImpl(BaseRow row, int column) {
			throw new RuntimeException("Not support yet!");
		}
	}

	/**
	 * Converter for base row.
	 */
	public static class BaseRowConverter extends IdentityConverter<BaseRow> {

		public static final BaseRowConverter INSTANCE = new BaseRowConverter();

		private BaseRowConverter() {}

		@Override
		BaseRow toExternalImpl(BaseRow row, int column) {
			throw new RuntimeException("Not support yet!");
		}
	}

	/**
	 * Converter for pojo.
	 */
	public static class PojoConverter<T> extends AbstractBaseRowConverter<GenericRow, T> {

		private final PojoTypeInfo<T> t;
		private final PojoField[] fields;

		public PojoConverter(PojoTypeInfo<T> t) {
			super(t);
			this.fields = new PojoField[t.getArity()];
			for (int i = 0; i < t.getArity(); i++) {
				fields[i] = t.getPojoFieldAt(i);
				fields[i].getField().setAccessible(true);
			}
			this.t = t;
		}

		@Override
		GenericRow toInternalImpl(T value) {
			GenericRow genericRow = new GenericRow(t.getArity());
			for (int i = 0; i < t.getArity(); i++) {
				try {
					genericRow.setField(i, converters[i].toInternal(
							fields[i].getField().get(value)));
				} catch (IllegalAccessException e) {
					throw new RuntimeException(e);
				}
			}
			return genericRow;
		}

		@Override
		T toExternalImpl(GenericRow value) {
			try {
				T pojo = t.getTypeClass().newInstance();
				for (int i = 0; i < t.getArity(); i++) {
					fields[i].getField().set(pojo, converters[i].toExternal(value, i));
				}
				return pojo;
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Converter for row.
	 */
	public static class RowConverter extends AbstractBaseRowConverter<GenericRow, Row> {

		private final RowTypeInfo t;

		public RowConverter(RowTypeInfo t) {
			super(t);
			this.t = t;
		}

		@Override
		GenericRow toInternalImpl(Row value) {
			GenericRow genericRow = new GenericRow(t.getArity());
			for (int i = 0; i < t.getArity(); i++) {
				genericRow.setField(i, converters[i].toInternal(value.getField(i)));
			}
			return genericRow;
		}

		@Override
		Row toExternalImpl(GenericRow value) {
			Row row = new Row(t.getArity());
			for (int i = 0; i < t.getArity(); i++) {
				row.setField(i, converters[i].toExternal(value, i));
			}
			return row;
		}
	}

	/**
	 * Converter for flink tuple.
	 */
	public static class TupleConverter extends AbstractBaseRowConverter<GenericRow, Tuple> {

		private final TupleTypeInfo t;

		public TupleConverter(TupleTypeInfo t) {
			super(t);
			this.t = t;
		}

		@Override
		GenericRow toInternalImpl(Tuple value) {
			GenericRow genericRow = new GenericRow(t.getArity());
			for (int i = 0; i < t.getArity(); i++) {
				genericRow.setField(i, converters[i].toInternal(value.getField(i)));
			}
			return genericRow;
		}

		@Override
		Tuple toExternalImpl(GenericRow value) {
			try {
				Tuple tuple = (Tuple) t.getTypeClass().newInstance();
				for (int i = 0; i < t.getArity(); i++) {
					tuple.setField(converters[i].toExternal(value, i), i);
				}
				return tuple;
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}

		}
	}

	/**
	 * Converter for case class.
	 */
	public static class CaseClassConverter extends AbstractBaseRowConverter<GenericRow, Product> {

		private final TupleTypeInfoBase t;
		private final TupleSerializerBase serializer;

		public CaseClassConverter(TupleTypeInfoBase t) {
			super(t);
			this.t = t;
			this.serializer = (TupleSerializerBase) t.createSerializer(new ExecutionConfig());
		}

		@Override
		GenericRow toInternalImpl(Product value) {
			GenericRow genericRow = new GenericRow(t.getArity());
			for (int i = 0; i < t.getArity(); i++) {
				genericRow.setField(i, converters[i].toInternal(value.productElement(i)));
			}
			return genericRow;
		}

		@Override
		Product toExternalImpl(GenericRow value) {
			Object[] fields = new Object[t.getArity()];
			for (int i = 0; i < t.getArity(); i++) {
				fields[i] = converters[i].toExternal(value, i);
			}
			return (Product) serializer.createInstance(fields);
		}
	}
}
