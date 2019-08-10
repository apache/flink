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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.runtime.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TypeInformationAnyType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import scala.Product;

import static org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

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

	private static final Map<DataType, DataFormatConverter> TYPE_TO_CONVERTER;
	static {
		Map<DataType, DataFormatConverter> t2C = new HashMap<>();

		t2C.put(DataTypes.BOOLEAN().bridgedTo(Boolean.class), BooleanConverter.INSTANCE);
		t2C.put(DataTypes.BOOLEAN().bridgedTo(boolean.class), BooleanConverter.INSTANCE);

		t2C.put(DataTypes.INT().bridgedTo(Integer.class), IntConverter.INSTANCE);
		t2C.put(DataTypes.INT().bridgedTo(int.class), IntConverter.INSTANCE);

		t2C.put(DataTypes.BIGINT().bridgedTo(Long.class), LongConverter.INSTANCE);
		t2C.put(DataTypes.BIGINT().bridgedTo(long.class), LongConverter.INSTANCE);

		t2C.put(DataTypes.SMALLINT().bridgedTo(Short.class), ShortConverter.INSTANCE);
		t2C.put(DataTypes.SMALLINT().bridgedTo(short.class), ShortConverter.INSTANCE);

		t2C.put(DataTypes.FLOAT().bridgedTo(Float.class), FloatConverter.INSTANCE);
		t2C.put(DataTypes.FLOAT().bridgedTo(float.class), FloatConverter.INSTANCE);

		t2C.put(DataTypes.DOUBLE().bridgedTo(Double.class), DoubleConverter.INSTANCE);
		t2C.put(DataTypes.DOUBLE().bridgedTo(double.class), DoubleConverter.INSTANCE);

		t2C.put(DataTypes.TINYINT().bridgedTo(Byte.class), ByteConverter.INSTANCE);
		t2C.put(DataTypes.TINYINT().bridgedTo(byte.class), ByteConverter.INSTANCE);

		t2C.put(DataTypes.DATE().bridgedTo(Date.class), DateConverter.INSTANCE);
		t2C.put(DataTypes.DATE().bridgedTo(LocalDate.class), LocalDateConverter.INSTANCE);
		t2C.put(DataTypes.DATE().bridgedTo(Integer.class), IntConverter.INSTANCE);
		t2C.put(DataTypes.DATE().bridgedTo(int.class), IntConverter.INSTANCE);

		t2C.put(DataTypes.TIME().bridgedTo(Time.class), TimeConverter.INSTANCE);
		t2C.put(DataTypes.TIME().bridgedTo(LocalTime.class), LocalTimeConverter.INSTANCE);
		t2C.put(DataTypes.TIME().bridgedTo(Integer.class), IntConverter.INSTANCE);
		t2C.put(DataTypes.TIME().bridgedTo(int.class), IntConverter.INSTANCE);

		t2C.put(DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class), TimestampConverter.INSTANCE);
		t2C.put(DataTypes.TIMESTAMP(3).bridgedTo(LocalDateTime.class), LocalDateTimeConverter.INSTANCE);

		t2C.put(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).bridgedTo(Long.class), LongConverter.INSTANCE);
		t2C.put(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).bridgedTo(Instant.class), InstantConverter.INSTANCE);

		t2C.put(DataTypes.INTERVAL(DataTypes.MONTH()).bridgedTo(Integer.class), IntConverter.INSTANCE);
		t2C.put(DataTypes.INTERVAL(DataTypes.MONTH()).bridgedTo(int.class), IntConverter.INSTANCE);

		t2C.put(DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(Long.class), LongConverter.INSTANCE);
		t2C.put(DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(long.class), LongConverter.INSTANCE);

		TYPE_TO_CONVERTER = Collections.unmodifiableMap(t2C);
	}

	/**
	 * Get {@link DataFormatConverter} for {@link DataType}.
	 *
	 * @param originDataType DataFormatConverter is oriented to Java format, while LogicalType has
	 *                   lost its specific Java format. Only DataType retains all its
	 *                   Java format information.
	 */
	public static DataFormatConverter getConverterForDataType(DataType originDataType) {
		DataType dataType = originDataType.nullable();
		DataFormatConverter converter = TYPE_TO_CONVERTER.get(dataType);
		if (converter != null) {
			return converter;
		}

		Class<?> clazz = dataType.getConversionClass();
		LogicalType logicalType = dataType.getLogicalType();
		switch (logicalType.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				if (clazz == String.class) {
					return StringConverter.INSTANCE;
				} else if (clazz == BinaryString.class) {
					return BinaryStringConverter.INSTANCE;
				} else {
					throw new RuntimeException("Not support class for VARCHAR: " + clazz);
				}
			case BINARY:
			case VARBINARY:
				return PrimitiveByteArrayConverter.INSTANCE;
			case DECIMAL:
				Tuple2<Integer, Integer> ps = getPrecision(logicalType);
				if (clazz == BigDecimal.class) {
					return new BigDecimalConverter(ps.f0, ps.f1);
				} else {
					return new DecimalConverter(ps.f0, ps.f1);
				}
			case ARRAY:
				if (clazz == BinaryArray.class) {
					return BinaryArrayConverter.INSTANCE;
				} else if (clazz == boolean[].class) {
					return PrimitiveBooleanArrayConverter.INSTANCE;
				} else if (clazz == short[].class) {
					return PrimitiveShortArrayConverter.INSTANCE;
				} else if (clazz == int[].class) {
					return PrimitiveIntArrayConverter.INSTANCE;
				} else if (clazz == long[].class) {
					return PrimitiveLongArrayConverter.INSTANCE;
				} else if (clazz == float[].class) {
					return PrimitiveFloatArrayConverter.INSTANCE;
				} else if (clazz == double[].class) {
					return PrimitiveDoubleArrayConverter.INSTANCE;
				}
				if (dataType instanceof CollectionDataType) {
					return new ObjectArrayConverter(
							((CollectionDataType) dataType).getElementDataType().bridgedTo(clazz.getComponentType()));
				} else {
					BasicArrayTypeInfo typeInfo =
							(BasicArrayTypeInfo) ((LegacyTypeInformationType) dataType.getLogicalType()).getTypeInformation();
					return new ObjectArrayConverter(
							fromLegacyInfoToDataType(typeInfo.getComponentInfo())
									.bridgedTo(clazz.getComponentType()));
				}
			case MAP:
				if (clazz == BinaryMap.class) {
					return BinaryMapConverter.INSTANCE;
				}
				KeyValueDataType keyValueDataType = (KeyValueDataType) dataType;
				return new MapConverter(keyValueDataType.getKeyDataType(), keyValueDataType.getValueDataType());
			case MULTISET:
				if (clazz == BinaryMap.class) {
					return BinaryMapConverter.INSTANCE;
				}
				CollectionDataType collectionDataType = (CollectionDataType) dataType;
				return new MapConverter(
						collectionDataType.getElementDataType(),
						DataTypes.INT().bridgedTo(Integer.class));
			case ROW:
			case STRUCTURED_TYPE:
				CompositeType compositeType = (CompositeType) fromDataTypeToTypeInfo(dataType);
				DataType[] fieldTypes = Stream.iterate(0, x -> x + 1).limit(compositeType.getArity())
						.map((Function<Integer, TypeInformation>) compositeType::getTypeAt)
						.map(TypeConversions::fromLegacyInfoToDataType).toArray(DataType[]::new);
				if (clazz == BaseRow.class) {
					return new BaseRowConverter(compositeType.getArity());
				} else if (clazz == Row.class) {
					return new RowConverter(fieldTypes);
				} else if (Tuple.class.isAssignableFrom(clazz)) {
					return new TupleConverter((Class<Tuple>) clazz, fieldTypes);
				} else if (Product.class.isAssignableFrom(clazz)) {
					return new CaseClassConverter((TupleTypeInfoBase) compositeType, fieldTypes);
				} else {
					return new PojoConverter((PojoTypeInfo) compositeType, fieldTypes);
				}
			case ANY:
				TypeInformation typeInfo = logicalType instanceof LegacyTypeInformationType ?
						((LegacyTypeInformationType) logicalType).getTypeInformation() :
						((TypeInformationAnyType) logicalType).getTypeInformation();

				// planner type info
				if (typeInfo instanceof BinaryStringTypeInfo) {
					return BinaryStringConverter.INSTANCE;
				} else if (typeInfo instanceof DecimalTypeInfo) {
					DecimalTypeInfo decimalType = (DecimalTypeInfo) typeInfo;
					return new DecimalConverter(decimalType.precision(), decimalType.scale());
				} else if (typeInfo instanceof BigDecimalTypeInfo) {
					BigDecimalTypeInfo decimalType = (BigDecimalTypeInfo) typeInfo;
					return new BigDecimalConverter(decimalType.precision(), decimalType.scale());
				}

				if (clazz == BinaryGeneric.class) {
					return BinaryGenericConverter.INSTANCE;
				}
				return new GenericConverter(typeInfo.createSerializer(new ExecutionConfig()));
			default:
				throw new RuntimeException("Not support dataType: " + dataType);
		}
	}

	private static Tuple2<Integer, Integer> getPrecision(LogicalType logicalType) {
		Tuple2<Integer, Integer> ps = new Tuple2<>();
		if (logicalType instanceof DecimalType) {
			DecimalType decimalType = (DecimalType) logicalType;
			ps.f0 = decimalType.getPrecision();
			ps.f1 = decimalType.getScale();
		} else {
			TypeInformation typeInfo = ((LegacyTypeInformationType) logicalType).getTypeInformation();
			if (typeInfo instanceof BigDecimalTypeInfo) {
				BigDecimalTypeInfo decimalType = (BigDecimalTypeInfo) typeInfo;
				ps.f0 = decimalType.precision();
				ps.f1 = decimalType.scale();
			} else if (typeInfo instanceof DecimalTypeInfo) {
				DecimalTypeInfo decimalType = (DecimalTypeInfo) typeInfo;
				ps.f0 = decimalType.precision();
				ps.f1 = decimalType.scale();
			} else {
				ps.f0 = Decimal.DECIMAL_SYSTEM_DEFAULT.getPrecision();
				ps.f1 = Decimal.DECIMAL_SYSTEM_DEFAULT.getScale();
			}
		}
		return ps;
	}

	/**
	 * Converter between internal data format and java format.
	 *
	 * @param <Internal> Internal data format.
	 * @param <External> External data format.
	 */
	public abstract static class DataFormatConverter<Internal, External> implements Serializable {

		private static final long serialVersionUID = 1L;

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

		private static final long serialVersionUID = 6146619729108124872L;

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
	public static final class BooleanConverter extends IdentityConverter<Boolean> {

		private static final long serialVersionUID = 3618373319753553272L;

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
	public static final class ByteConverter extends IdentityConverter<Byte> {

		private static final long serialVersionUID = 1880134895918999433L;

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
	public static final class ShortConverter extends IdentityConverter<Short> {

		private static final long serialVersionUID = 8055034507232206636L;

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
	public static final class IntConverter extends IdentityConverter<Integer> {

		private static final long serialVersionUID = -7749307898273403416L;

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
	public static final class LongConverter extends IdentityConverter<Long> {

		private static final long serialVersionUID = 7373868336730797650L;

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
	public static final class FloatConverter extends IdentityConverter<Float> {

		private static final long serialVersionUID = -1119035126939832966L;

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
	public static final class DoubleConverter extends IdentityConverter<Double> {

		private static final long serialVersionUID = 2801171640313215040L;

		public static final DoubleConverter INSTANCE = new DoubleConverter();

		private DoubleConverter() {}

		@Override
		Double toExternalImpl(BaseRow row, int column) {
			return row.getDouble(column);
		}
	}

	/**
	 * Converter for BinaryString.
	 */
	public static final class BinaryStringConverter extends IdentityConverter<BinaryString> {

		private static final long serialVersionUID = 5565684451615599206L;

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
	public static final class BinaryArrayConverter extends IdentityConverter<BaseArray> {

		private static final long serialVersionUID = -7790350668043604641L;

		public static final BinaryArrayConverter INSTANCE = new BinaryArrayConverter();

		private BinaryArrayConverter() {}

		@Override
		BaseArray toExternalImpl(BaseRow row, int column) {
			return row.getArray(column);
		}
	}

	/**
	 * Converter for BinaryMap.
	 */
	public static final class BinaryMapConverter extends IdentityConverter<BaseMap> {

		private static final long serialVersionUID = -9114231688474126815L;

		public static final BinaryMapConverter INSTANCE = new BinaryMapConverter();

		private BinaryMapConverter() {}

		@Override
		BaseMap toExternalImpl(BaseRow row, int column) {
			return row.getMap(column);
		}
	}

	/**
	 * Converter for Decimal.
	 */
	public static final class DecimalConverter extends IdentityConverter<Decimal> {

		private static final long serialVersionUID = 3825744951173809617L;

		private final int precision;
		private final int scale;

		public DecimalConverter(int precision, int scale) {
			this.precision = precision;
			this.scale = scale;
		}

		@Override
		Decimal toExternalImpl(BaseRow row, int column) {
			return row.getDecimal(column, precision, scale);
		}
	}

	/**
	 * Converter for BinaryGeneric.
	 */
	public static final class BinaryGenericConverter extends IdentityConverter<BinaryGeneric> {

		private static final long serialVersionUID = 1436229503920584273L;

		public static final BinaryGenericConverter INSTANCE = new BinaryGenericConverter();

		private BinaryGenericConverter() {}

		@Override
		BinaryGeneric toExternalImpl(BaseRow row, int column) {
			return row.getGeneric(column);
		}
	}

	/**
	 * Converter for String.
	 */
	public static final class StringConverter extends DataFormatConverter<BinaryString, String> {

		private static final long serialVersionUID = 4713165079099282774L;

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
	 * Converter for BigDecimal.
	 */
	public static final class BigDecimalConverter extends DataFormatConverter<Decimal, BigDecimal> {

		private static final long serialVersionUID = -6586239704060565834L;

		private final int precision;
		private final int scale;

		public BigDecimalConverter(int precision, int scale) {
			this.precision = precision;
			this.scale = scale;
		}

		@Override
		Decimal toInternalImpl(BigDecimal value) {
			return Decimal.fromBigDecimal(value, precision, scale);
		}

		@Override
		BigDecimal toExternalImpl(Decimal value) {
			return value.toBigDecimal();
		}

		@Override
		BigDecimal toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getDecimal(column, precision, scale));
		}
	}

	/**
	 * Converter for generic.
	 */
	public static final class GenericConverter<T> extends DataFormatConverter<BinaryGeneric<T>, T> {

		private static final long serialVersionUID = -3611718364918053384L;

		private final TypeSerializer<T> serializer;

		public GenericConverter(TypeSerializer<T> serializer) {
			this.serializer = serializer;
		}

		@Override
		BinaryGeneric<T> toInternalImpl(T value) {
			return new BinaryGeneric<>(value, serializer);
		}

		@Override
		T toExternalImpl(BinaryGeneric<T> value) {
			return BinaryGeneric.getJavaObjectFromBinaryGeneric(value, serializer);
		}

		@Override
		T toExternalImpl(BaseRow row, int column) {
			return (T) toExternalImpl(row.getGeneric(column));
		}
	}

	/**
	 * Converter for LocalDate.
	 */
	public static final class LocalDateConverter extends DataFormatConverter<Integer, LocalDate> {

		private static final long serialVersionUID = 1L;

		public static final LocalDateConverter INSTANCE = new LocalDateConverter();

		private LocalDateConverter() {}

		@Override
		Integer toInternalImpl(LocalDate value) {
			return SqlDateTimeUtils.localDateToUnixDate(value);
		}

		@Override
		LocalDate toExternalImpl(Integer value) {
			return SqlDateTimeUtils.unixDateToLocalDate(value);
		}

		@Override
		LocalDate toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getInt(column));
		}
	}

	/**
	 * Converter for LocalTime.
	 */
	public static final class LocalTimeConverter extends DataFormatConverter<Integer, LocalTime> {

		private static final long serialVersionUID = 1L;

		public static final LocalTimeConverter INSTANCE = new LocalTimeConverter();

		private LocalTimeConverter() {}

		@Override
		Integer toInternalImpl(LocalTime value) {
			return SqlDateTimeUtils.localTimeToUnixDate(value);
		}

		@Override
		LocalTime toExternalImpl(Integer value) {
			return SqlDateTimeUtils.unixTimeToLocalTime(value);
		}

		@Override
		LocalTime toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getInt(column));
		}
	}

	/**
	 * Converter for LocalDateTime.
	 */
	public static final class LocalDateTimeConverter extends DataFormatConverter<Long, LocalDateTime> {

		private static final long serialVersionUID = 1L;

		public static final LocalDateTimeConverter INSTANCE = new LocalDateTimeConverter();

		private LocalDateTimeConverter() {}

		@Override
		Long toInternalImpl(LocalDateTime value) {
			return SqlDateTimeUtils.localDateTimeToUnixTimestamp(value);
		}

		@Override
		LocalDateTime toExternalImpl(Long value) {
			return SqlDateTimeUtils.unixTimestampToLocalDateTime(value);
		}

		@Override
		LocalDateTime toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getLong(column));
		}
	}

	/**
	 * Converter for Instant.
	 */
	public static final class InstantConverter extends DataFormatConverter<Long, Instant> {

		private static final long serialVersionUID = 1L;

		public static final InstantConverter INSTANCE = new InstantConverter();

		private InstantConverter() {}

		@Override
		Long toInternalImpl(Instant value) {
			return value.toEpochMilli();
		}

		@Override
		Instant toExternalImpl(Long value) {
			return Instant.ofEpochMilli(value);
		}

		@Override
		Instant toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getLong(column));
		}
	}

	/**
	 * Converter for date.
	 */
	public static final class DateConverter extends DataFormatConverter<Integer, Date> {

		private static final long serialVersionUID = 1343457113582411650L;

		public static final DateConverter INSTANCE = new DateConverter();

		private DateConverter() {}

		@Override
		Integer toInternalImpl(Date value) {
			return SqlDateTimeUtils.dateToInternal(value);
		}

		@Override
		Date toExternalImpl(Integer value) {
			return SqlDateTimeUtils.internalToDate(value);
		}

		@Override
		Date toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getInt(column));
		}
	}

	/**
	 * Converter for time.
	 */
	public static final class TimeConverter extends DataFormatConverter<Integer, Time> {

		private static final long serialVersionUID = -8061475784916442483L;

		public static final TimeConverter INSTANCE = new TimeConverter();

		private TimeConverter() {}

		@Override
		Integer toInternalImpl(Time value) {
			return SqlDateTimeUtils.timeToInternal(value);
		}

		@Override
		Time toExternalImpl(Integer value) {
			return SqlDateTimeUtils.internalToTime(value);
		}

		@Override
		Time toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getInt(column));
		}
	}

	/**
	 * Converter for timestamp.
	 */
	public static final class TimestampConverter extends DataFormatConverter<Long, Timestamp> {

		private static final long serialVersionUID = -779956524906131757L;

		public static final TimestampConverter INSTANCE = new TimestampConverter();

		private TimestampConverter() {}

		@Override
		Long toInternalImpl(Timestamp value) {
			return SqlDateTimeUtils.timestampToInternal(value);
		}

		@Override
		Timestamp toExternalImpl(Long value) {
			return SqlDateTimeUtils.internalToTimestamp(value);
		}

		@Override
		Timestamp toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getLong(column));
		}
	}

	/**
	 * Converter for primitive int array.
	 */
	public static final class PrimitiveIntArrayConverter extends DataFormatConverter<BaseArray, int[]> {

		private static final long serialVersionUID = 1780941126232395638L;

		public static final PrimitiveIntArrayConverter INSTANCE = new PrimitiveIntArrayConverter();

		private PrimitiveIntArrayConverter() {}

		@Override
		BaseArray toInternalImpl(int[] value) {
			return new GenericArray(value, value.length, true);
		}

		@Override
		int[] toExternalImpl(BaseArray value) {
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
	public static final class PrimitiveBooleanArrayConverter extends DataFormatConverter<BaseArray, boolean[]> {

		private static final long serialVersionUID = -4037693692440282141L;

		public static final PrimitiveBooleanArrayConverter INSTANCE = new PrimitiveBooleanArrayConverter();

		private PrimitiveBooleanArrayConverter() {}

		@Override
		BaseArray toInternalImpl(boolean[] value) {
			return new GenericArray(value, value.length, true);
		}

		@Override
		boolean[] toExternalImpl(BaseArray value) {
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
	public static final class PrimitiveByteArrayConverter extends IdentityConverter<byte[]> {

		private static final long serialVersionUID = -2007960927801689921L;

		public static final PrimitiveByteArrayConverter INSTANCE = new PrimitiveByteArrayConverter();

		private PrimitiveByteArrayConverter() {}

		@Override
		byte[] toExternalImpl(BaseRow row, int column) {
			return row.getBinary(column);
		}
	}

	/**
	 * Converter for primitive short array.
	 */
	public static final class PrimitiveShortArrayConverter extends DataFormatConverter<BaseArray, short[]> {

		private static final long serialVersionUID = -1343184089311186834L;

		public static final PrimitiveShortArrayConverter INSTANCE = new PrimitiveShortArrayConverter();

		private PrimitiveShortArrayConverter() {}

		@Override
		BaseArray toInternalImpl(short[] value) {
			return new GenericArray(value, value.length, true);
		}

		@Override
		short[] toExternalImpl(BaseArray value) {
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
	public static final class PrimitiveLongArrayConverter extends DataFormatConverter<BaseArray, long[]> {

		private static final long serialVersionUID = 4061982985342526078L;

		public static final PrimitiveLongArrayConverter INSTANCE = new PrimitiveLongArrayConverter();

		private PrimitiveLongArrayConverter() {}

		@Override
		BaseArray toInternalImpl(long[] value) {
			return new GenericArray(value, value.length, true);
		}

		@Override
		long[] toExternalImpl(BaseArray value) {
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
	public static final class PrimitiveFloatArrayConverter extends DataFormatConverter<BaseArray, float[]> {

		private static final long serialVersionUID = -3237695040861141459L;

		public static final PrimitiveFloatArrayConverter INSTANCE = new PrimitiveFloatArrayConverter();

		private PrimitiveFloatArrayConverter() {}

		@Override
		BaseArray toInternalImpl(float[] value) {
			return new GenericArray(value, value.length, true);
		}

		@Override
		float[] toExternalImpl(BaseArray value) {
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
	public static final class PrimitiveDoubleArrayConverter extends DataFormatConverter<BaseArray, double[]> {

		private static final long serialVersionUID = 6333670535356315691L;

		public static final PrimitiveDoubleArrayConverter INSTANCE = new PrimitiveDoubleArrayConverter();

		private PrimitiveDoubleArrayConverter() {}

		@Override
		BaseArray toInternalImpl(double[] value) {
			return new GenericArray(value, value.length, true);
		}

		@Override
		double[] toExternalImpl(BaseArray value) {
			return value.toDoubleArray();
		}

		@Override
		double[] toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for object array.
	 */
	public static final class ObjectArrayConverter<T> extends DataFormatConverter<BaseArray, T[]> {

		private static final long serialVersionUID = -7434682160639380078L;

		private final Class<T> componentClass;
		private final LogicalType elementType;
		private final DataFormatConverter<Object, T> elementConverter;
		private final int elementSize;
		private final TypeSerializer<T> eleSer;
		private final boolean isEleIndentity;

		private transient BinaryArray reuseArray;
		private transient BinaryArrayWriter reuseWriter;

		public ObjectArrayConverter(DataType elementType) {
			this.componentClass = (Class) elementType.getConversionClass();
			this.elementType = LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(elementType);
			this.elementConverter = DataFormatConverters.getConverterForDataType(elementType);
			this.elementSize = BinaryArray.calculateFixLengthPartSize(this.elementType);
			this.eleSer = InternalSerializers.create(this.elementType, new ExecutionConfig());
			this.isEleIndentity = elementConverter instanceof IdentityConverter;
		}

		@Override
		BaseArray toInternalImpl(T[] value) {
			return isEleIndentity ? new GenericArray(value, value.length) : toBinaryArray(value);
		}

		private BaseArray toBinaryArray(T[] value) {
			if (reuseArray == null) {
				reuseArray = new BinaryArray();
			}
			if (reuseWriter == null || reuseWriter.getNumElements() != value.length) {
				reuseWriter = new BinaryArrayWriter(reuseArray, value.length, elementSize);
			} else {
				reuseWriter.reset();
			}
			for (int i = 0; i < value.length; i++) {
				Object field = value[i];
				if (field == null) {
					reuseWriter.setNullAt(i, elementType);
				} else {
					BinaryWriter.write(reuseWriter, i, elementConverter.toInternalImpl(value[i]), elementType, eleSer);
				}
			}
			reuseWriter.complete();
			return reuseArray;
		}

		@Override
		T[] toExternalImpl(BaseArray value) {
			return (isEleIndentity && value instanceof GenericArray) ?
					genericArrayToJavaArray((GenericArray) value, elementType) :
					binaryArrayToJavaArray((BinaryArray) value, elementType, componentClass, elementConverter);
		}

		@Override
		T[] toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	private static <T> T[] genericArrayToJavaArray(GenericArray value, LogicalType eleType) {
		Object array = value.getArray();
		if (value.isPrimitiveArray()) {
			switch (eleType.getTypeRoot()) {
				case BOOLEAN:
					return (T[]) ArrayUtils.toObject((boolean[]) array);
				case TINYINT:
					return (T[]) ArrayUtils.toObject((byte[]) array);
				case SMALLINT:
					return (T[]) ArrayUtils.toObject((short[]) array);
				case INTEGER:
					return (T[]) ArrayUtils.toObject((int[]) array);
				case BIGINT:
					return (T[]) ArrayUtils.toObject((long[]) array);
				case FLOAT:
					return (T[]) ArrayUtils.toObject((float[]) array);
				case DOUBLE:
					return (T[]) ArrayUtils.toObject((double[]) array);
				default:
					throw new RuntimeException("Not a primitive type: " + eleType);
			}
		} else {
			return (T[]) array;
		}
	}

	private static <T> T[] binaryArrayToJavaArray(BinaryArray value, LogicalType elementType,
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
	public static final class MapConverter extends DataFormatConverter<BaseMap, Map> {

		private static final long serialVersionUID = -916429669828309919L;

		private final LogicalType keyType;
		private final LogicalType valueType;

		private final DataFormatConverter keyConverter;
		private final DataFormatConverter valueConverter;

		private final int keyElementSize;
		private final int valueElementSize;

		private final Class keyComponentClass;
		private final Class valueComponentClass;

		private final TypeSerializer keySer;
		private final TypeSerializer valueSer;

		private final boolean isKeyValueIndentity;

		private transient BinaryArray reuseKArray;
		private transient BinaryArrayWriter reuseKWriter;
		private transient BinaryArray reuseVArray;
		private transient BinaryArrayWriter reuseVWriter;

		public MapConverter(DataType keyTypeInfo, DataType valueTypeInfo) {
			this.keyType = LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(keyTypeInfo);
			this.valueType = LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(valueTypeInfo);
			this.keyConverter = DataFormatConverters.getConverterForDataType(keyTypeInfo);
			this.valueConverter = DataFormatConverters.getConverterForDataType(valueTypeInfo);
			this.keyElementSize = BinaryArray.calculateFixLengthPartSize(keyType);
			this.valueElementSize = BinaryArray.calculateFixLengthPartSize(valueType);
			this.keyComponentClass = keyTypeInfo.getConversionClass();
			this.valueComponentClass = valueTypeInfo.getConversionClass();
			this.isKeyValueIndentity = keyConverter instanceof IdentityConverter &&
					valueConverter instanceof IdentityConverter;
			this.keySer = InternalSerializers.create(this.keyType, new ExecutionConfig());
			this.valueSer = InternalSerializers.create(this.valueType, new ExecutionConfig());
		}

		@Override
		BaseMap toInternalImpl(Map value) {
			return isKeyValueIndentity ? new GenericMap(value) : toBinaryMap(value);
		}

		private BinaryMap toBinaryMap(Map value) {
			if (reuseKArray == null) {
				reuseKArray = new BinaryArray();
				reuseVArray = new BinaryArray();
			}
			if (reuseKWriter == null || reuseKWriter.getNumElements() != value.size()) {
				reuseKWriter = new BinaryArrayWriter(reuseKArray, value.size(), keyElementSize);
				reuseVWriter = new BinaryArrayWriter(reuseVArray, value.size(), valueElementSize);
			} else {
				reuseKWriter.reset();
				reuseVWriter.reset();
			}

			int i = 0;
			for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
				if (entry.getKey() == null) {
					reuseKWriter.setNullAt(i, keyType);
				} else {
					BinaryWriter.write(reuseKWriter, i, keyConverter.toInternalImpl(entry.getKey()), keyType, keySer);
				}
				if (entry.getValue() == null) {
					reuseVWriter.setNullAt(i, valueType);
				} else {
					BinaryWriter.write(reuseVWriter, i, valueConverter.toInternalImpl(entry.getValue()), valueType, valueSer);
				}
				i++;
			}

			reuseKWriter.complete();
			reuseVWriter.complete();
			return BinaryMap.valueOf(reuseKArray, reuseVArray);
		}

		@Override
		Map toExternalImpl(BaseMap value) {
			return (isKeyValueIndentity && value instanceof GenericMap) ?
					((GenericMap) value).getMap() :
					binaryMapToMap((BinaryMap) value);
		}

		private Map binaryMapToMap(BinaryMap value) {
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
	public abstract static class AbstractBaseRowConverter<E> extends DataFormatConverter<BaseRow, E> {

		private static final long serialVersionUID = 4365740929854771618L;

		protected final DataFormatConverter[] converters;

		public AbstractBaseRowConverter(DataType[] fieldTypes) {
			converters = new DataFormatConverter[fieldTypes.length];
			for (int i = 0; i < converters.length; i++) {
				converters[i] = getConverterForDataType(fieldTypes[i]);
			}
		}

		@Override
		E toExternalImpl(BaseRow row, int column) {
			return toExternalImpl(row.getRow(column, converters.length));
		}
	}

	/**
	 * Converter for base row.
	 */
	public static final class BaseRowConverter extends IdentityConverter<BaseRow> {

		private static final long serialVersionUID = -4470307402371540680L;
		private int arity;

		private BaseRowConverter(int arity) {}

		@Override
		BaseRow toExternalImpl(BaseRow row, int column) {
			return row.getRow(column, arity);
		}
	}

	/**
	 * Converter for pojo.
	 */
	public static final class PojoConverter<T> extends AbstractBaseRowConverter<T> {

		private static final long serialVersionUID = 6821541780176167135L;

		private final PojoTypeInfo<T> t;
		private final PojoField[] fields;

		public PojoConverter(PojoTypeInfo<T> t, DataType[] fieldTypes) {
			super(fieldTypes);
			this.fields = new PojoField[t.getArity()];
			for (int i = 0; i < t.getArity(); i++) {
				fields[i] = t.getPojoFieldAt(i);
				fields[i].getField().setAccessible(true);
			}
			this.t = t;
		}

		@Override
		BaseRow toInternalImpl(T value) {
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
		T toExternalImpl(BaseRow value) {
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
	public static final class RowConverter extends AbstractBaseRowConverter<Row> {

		private static final long serialVersionUID = -56553502075225785L;

		public RowConverter(DataType[] fieldTypes) {
			super(fieldTypes);
		}

		@Override
		BaseRow toInternalImpl(Row value) {
			GenericRow genericRow = new GenericRow(converters.length);
			for (int i = 0; i < converters.length; i++) {
				genericRow.setField(i, converters[i].toInternal(value.getField(i)));
			}
			return genericRow;
		}

		@Override
		Row toExternalImpl(BaseRow value) {
			Row row = new Row(converters.length);
			for (int i = 0; i < converters.length; i++) {
				row.setField(i, converters[i].toExternal(value, i));
			}
			return row;
		}
	}

	/**
	 * Converter for flink tuple.
	 */
	public static final class TupleConverter extends AbstractBaseRowConverter<Tuple> {

		private static final long serialVersionUID = 2794892691010934194L;

		private final Class<Tuple> clazz;

		public TupleConverter(Class<Tuple> clazz, DataType[] fieldTypes) {
			super(fieldTypes);
			this.clazz = clazz;
		}

		@Override
		BaseRow toInternalImpl(Tuple value) {
			GenericRow genericRow = new GenericRow(converters.length);
			for (int i = 0; i < converters.length; i++) {
				genericRow.setField(i, converters[i].toInternal(value.getField(i)));
			}
			return genericRow;
		}

		@Override
		Tuple toExternalImpl(BaseRow value) {
			try {
				Tuple tuple = clazz.newInstance();
				for (int i = 0; i < converters.length; i++) {
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
	public static final class CaseClassConverter extends AbstractBaseRowConverter<Product> {

		private static final long serialVersionUID = -966598627968372952L;

		private final TupleTypeInfoBase t;
		private final TupleSerializerBase serializer;

		public CaseClassConverter(TupleTypeInfoBase t, DataType[] fieldTypes) {
			super(fieldTypes);
			this.t = t;
			this.serializer = (TupleSerializerBase) t.createSerializer(new ExecutionConfig());
		}

		@Override
		BaseRow toInternalImpl(Product value) {
			GenericRow genericRow = new GenericRow(t.getArity());
			for (int i = 0; i < t.getArity(); i++) {
				genericRow.setField(i, converters[i].toInternal(value.productElement(i)));
			}
			return genericRow;
		}

		@Override
		Product toExternalImpl(BaseRow value) {
			Object[] fields = new Object[t.getArity()];
			for (int i = 0; i < t.getArity(); i++) {
				fields[i] = converters[i].toExternal(value, i);
			}
			return (Product) serializer.createInstance(fields);
		}
	}
}
