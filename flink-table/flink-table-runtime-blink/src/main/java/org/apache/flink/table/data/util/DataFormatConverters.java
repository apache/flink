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

package org.apache.flink.table.data.util;

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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyInstantTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyLocalDateTimeTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyTimestampTypeInfo;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.TimestampDataTypeInfo;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
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
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldCount;
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
				} else if (clazz == StringData.class) {
					return StringDataConverter.INSTANCE;
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
				} else if (clazz == DecimalData.class) {
					return new DecimalDataConverter(ps.f0, ps.f1);
				} else {
					throw new RuntimeException("Not support conversion class for DECIMAL: " + clazz);
				}
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				int precisionOfTS = getDateTimePrecision(logicalType);
				if (clazz == Timestamp.class) {
					return new TimestampConverter(precisionOfTS);
				} else if (clazz == LocalDateTime.class) {
					return new LocalDateTimeConverter(precisionOfTS);
				} else if (clazz == TimestampData.class) {
					return new TimestampDataConverter(precisionOfTS);
				} else {
					throw new RuntimeException("Not support conversion class for TIMESTAMP WITHOUT TIME ZONE: " + clazz);
				}
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				int precisionOfLZTS = getDateTimePrecision(logicalType);
				if (clazz == Instant.class) {
					return new InstantConverter(precisionOfLZTS);
				} else if (clazz == Long.class || clazz == long.class) {
					return new LongTimestampDataConverter(precisionOfLZTS);
				} else if (clazz == TimestampData.class) {
					return new TimestampDataConverter(precisionOfLZTS);
				} else {
					throw new RuntimeException("Not support conversion class for TIMESTAMP WITH LOCAL TIME ZONE: " + clazz);
				}
			case ARRAY:
				if (clazz == ArrayData.class) {
					return ArrayDataConverter.INSTANCE;
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
				if (clazz == MapData.class) {
					return MapDataConverter.INSTANCE;
				}
				KeyValueDataType keyValueDataType = (KeyValueDataType) dataType;
				return new MapConverter(keyValueDataType.getKeyDataType(), keyValueDataType.getValueDataType());
			case MULTISET:
				if (clazz == MapData.class) {
					return MapDataConverter.INSTANCE;
				}
				CollectionDataType collectionDataType = (CollectionDataType) dataType;
				return new MapConverter(
						collectionDataType.getElementDataType(),
						DataTypes.INT().bridgedTo(Integer.class));
			case ROW:
			case STRUCTURED_TYPE:
				TypeInformation<?> asTypeInfo = fromDataTypeToTypeInfo(dataType);
				if (asTypeInfo instanceof InternalTypeInfo && clazz == RowData.class) {
					LogicalType realLogicalType = ((InternalTypeInfo<?>) asTypeInfo).toLogicalType();
					return new RowDataConverter(getFieldCount(realLogicalType));
				}

				// legacy

				CompositeType compositeType = (CompositeType) asTypeInfo;
				DataType[] fieldTypes = Stream.iterate(0, x -> x + 1).limit(compositeType.getArity())
						.map((Function<Integer, TypeInformation>) compositeType::getTypeAt)
						.map(TypeConversions::fromLegacyInfoToDataType).toArray(DataType[]::new);
				if (clazz == RowData.class) {
					return new RowDataConverter(compositeType.getArity());
				} else if (clazz == Row.class) {
					return new RowConverter(fieldTypes);
				} else if (Tuple.class.isAssignableFrom(clazz)) {
					return new TupleConverter((Class<Tuple>) clazz, fieldTypes);
				} else if (Product.class.isAssignableFrom(clazz)) {
					return new CaseClassConverter((TupleTypeInfoBase) compositeType, fieldTypes);
				} else {
					return new PojoConverter((PojoTypeInfo) compositeType, fieldTypes);
				}
			case RAW:
				if (logicalType instanceof RawType) {
					final RawType<?> rawType = (RawType<?>) logicalType;
					if (clazz == RawValueData.class) {
						return RawValueDataConverter.INSTANCE;
					} else {
						return new GenericConverter<>(rawType.getTypeSerializer());
					}
				}

				// legacy

				TypeInformation typeInfo = logicalType instanceof LegacyTypeInformationType ?
						((LegacyTypeInformationType) logicalType).getTypeInformation() :
						((TypeInformationRawType) logicalType).getTypeInformation();

				// planner type info
				if (typeInfo instanceof StringDataTypeInfo) {
					return StringDataConverter.INSTANCE;
				} else if (typeInfo instanceof DecimalDataTypeInfo) {
					DecimalDataTypeInfo decimalType = (DecimalDataTypeInfo) typeInfo;
					return new DecimalDataConverter(decimalType.precision(), decimalType.scale());
				} else if (typeInfo instanceof BigDecimalTypeInfo) {
					BigDecimalTypeInfo decimalType = (BigDecimalTypeInfo) typeInfo;
					return new BigDecimalConverter(decimalType.precision(), decimalType.scale());
				} else if (typeInfo instanceof TimestampDataTypeInfo) {
					TimestampDataTypeInfo timestampDataTypeInfo = (TimestampDataTypeInfo) typeInfo;
					return new TimestampDataConverter(timestampDataTypeInfo.getPrecision());
				} else if (typeInfo instanceof LegacyLocalDateTimeTypeInfo) {
					LegacyLocalDateTimeTypeInfo dateTimeType = (LegacyLocalDateTimeTypeInfo) typeInfo;
					return new LocalDateTimeConverter(dateTimeType.getPrecision());
				} else if (typeInfo instanceof LegacyTimestampTypeInfo) {
					LegacyTimestampTypeInfo timestampType = (LegacyTimestampTypeInfo) typeInfo;
					return new TimestampConverter(timestampType.getPrecision());
				} else if (typeInfo instanceof LegacyInstantTypeInfo) {
					LegacyInstantTypeInfo instantTypeInfo = (LegacyInstantTypeInfo) typeInfo;
					return new InstantConverter(instantTypeInfo.getPrecision());
				}

				if (clazz == RawValueData.class) {
					return RawValueDataConverter.INSTANCE;
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
			} else if (typeInfo instanceof DecimalDataTypeInfo) {
				DecimalDataTypeInfo decimalType = (DecimalDataTypeInfo) typeInfo;
				ps.f0 = decimalType.precision();
				ps.f1 = decimalType.scale();
			} else {
				ps.f0 = DecimalDataUtils.DECIMAL_SYSTEM_DEFAULT.getPrecision();
				ps.f1 = DecimalDataUtils.DECIMAL_SYSTEM_DEFAULT.getScale();
			}
		}
		return ps;
	}

	private static int getDateTimePrecision(LogicalType logicalType) {
		if (logicalType instanceof LocalZonedTimestampType) {
			return ((LocalZonedTimestampType) logicalType).getPrecision();
		} else if (logicalType instanceof TimestampType) {
			return ((TimestampType) logicalType).getPrecision();
		} else {
			TypeInformation typeInfo = ((LegacyTypeInformationType) logicalType).getTypeInformation();
			if (typeInfo instanceof LegacyInstantTypeInfo) {
				return ((LegacyInstantTypeInfo) typeInfo).getPrecision();
			} else if (typeInfo instanceof LegacyLocalDateTimeTypeInfo) {
				return ((LegacyLocalDateTimeTypeInfo) typeInfo).getPrecision();
			} else {
				// TimestampType.DEFAULT_PRECISION == LocalZonedTimestampType.DEFAULT_PRECISION == 6
				return TimestampType.DEFAULT_PRECISION;
			}
		}
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
		abstract External toExternalImpl(RowData row, int column);

		/**
		 * Given a internalType row, convert the value at column `column` to its external(Java) equivalent.
		 */
		public final External toExternal(RowData row, int column) {
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
		Boolean toExternalImpl(RowData row, int column) {
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
		Byte toExternalImpl(RowData row, int column) {
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
		Short toExternalImpl(RowData row, int column) {
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
		Integer toExternalImpl(RowData row, int column) {
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
		Long toExternalImpl(RowData row, int column) {
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
		Float toExternalImpl(RowData row, int column) {
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
		Double toExternalImpl(RowData row, int column) {
			return row.getDouble(column);
		}
	}

	/**
	 * Converter for StringData.
	 */
	public static final class StringDataConverter extends IdentityConverter<StringData> {

		private static final long serialVersionUID = 5565684451615599206L;

		public static final StringDataConverter INSTANCE = new StringDataConverter();

		private StringDataConverter() {}

		@Override
		StringData toExternalImpl(RowData row, int column) {
			return row.getString(column);
		}
	}

	/**
	 * Converter for ArrayData.
	 */
	public static final class ArrayDataConverter extends IdentityConverter<ArrayData> {

		private static final long serialVersionUID = -7790350668043604641L;

		public static final ArrayDataConverter INSTANCE = new ArrayDataConverter();

		private ArrayDataConverter() {}

		@Override
		ArrayData toExternalImpl(RowData row, int column) {
			return row.getArray(column);
		}
	}

	/**
	 * Converter for MapData.
	 */
	public static final class MapDataConverter extends IdentityConverter<MapData> {

		private static final long serialVersionUID = -9114231688474126815L;

		public static final MapDataConverter INSTANCE = new MapDataConverter();

		private MapDataConverter() {}

		@Override
		MapData toExternalImpl(RowData row, int column) {
			return row.getMap(column);
		}
	}

	/**
	 * Converter for DecimalData.
	 */
	public static final class DecimalDataConverter extends IdentityConverter<DecimalData> {

		private static final long serialVersionUID = 3825744951173809617L;

		private final int precision;
		private final int scale;

		public DecimalDataConverter(int precision, int scale) {
			this.precision = precision;
			this.scale = scale;
		}

		@Override
		DecimalData toExternalImpl(RowData row, int column) {
			return row.getDecimal(column, precision, scale);
		}
	}

	/**
	 * Converter for RawValueData.
	 */
	public static final class RawValueDataConverter extends IdentityConverter<RawValueData<?>> {

		private static final long serialVersionUID = 1436229503920584273L;

		public static final RawValueDataConverter INSTANCE = new RawValueDataConverter();

		private RawValueDataConverter() {}

		@Override
		RawValueData<?> toExternalImpl(RowData row, int column) {
			return row.getRawValue(column);
		}
	}

	/**
	 * Converter for String.
	 */
	public static final class StringConverter extends DataFormatConverter<StringData, String> {

		private static final long serialVersionUID = 4713165079099282774L;

		public static final StringConverter INSTANCE = new StringConverter();

		private StringConverter() {}

		@Override
		StringData toInternalImpl(String value) {
			return StringData.fromString(value);
		}

		@Override
		String toExternalImpl(StringData value) {
			return value.toString();
		}

		@Override
		String toExternalImpl(RowData row, int column) {
			return row.getString(column).toString();
		}
	}

	/**
	 * Converter for BigDecimal.
	 */
	public static final class BigDecimalConverter extends DataFormatConverter<DecimalData, BigDecimal> {

		private static final long serialVersionUID = -6586239704060565834L;

		private final int precision;
		private final int scale;

		public BigDecimalConverter(int precision, int scale) {
			this.precision = precision;
			this.scale = scale;
		}

		@Override
		DecimalData toInternalImpl(BigDecimal value) {
			return DecimalData.fromBigDecimal(value, precision, scale);
		}

		@Override
		BigDecimal toExternalImpl(DecimalData value) {
			return value.toBigDecimal();
		}

		@Override
		BigDecimal toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getDecimal(column, precision, scale));
		}
	}

	/**
	 * Converter for {@link RawValueData}.
	 */
	public static final class GenericConverter<T> extends DataFormatConverter<RawValueData<T>, T> {

		private static final long serialVersionUID = -3611718364918053384L;

		private final TypeSerializer<T> serializer;

		public GenericConverter(TypeSerializer<T> serializer) {
			this.serializer = serializer;
		}

		@Override
		RawValueData<T> toInternalImpl(T value) {
			return RawValueData.fromObject(value);
		}

		@Override
		T toExternalImpl(RawValueData<T> value) {
			return value.toObject(serializer);
		}

		@Override
		T toExternalImpl(RowData row, int column) {
			return (T) toExternalImpl(row.getRawValue(column));
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
		LocalDate toExternalImpl(RowData row, int column) {
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
		LocalTime toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getInt(column));
		}
	}

	/**
	 * Converter for LocalDateTime.
	 */
	public static final class LocalDateTimeConverter extends DataFormatConverter<TimestampData, LocalDateTime> {

		private static final long serialVersionUID = 1L;

		private final int precision;

		public LocalDateTimeConverter(int precision) {
			this.precision = precision;
		}

		@Override
		TimestampData toInternalImpl(LocalDateTime value) {
			return TimestampData.fromLocalDateTime(value);
		}

		@Override
		LocalDateTime toExternalImpl(TimestampData value) {
			return value.toLocalDateTime();
		}

		@Override
		LocalDateTime toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getTimestamp(column, precision));
		}
	}

	/**
	 * Converter for Instant.
	 */
	public static final class InstantConverter extends DataFormatConverter<TimestampData, Instant> {

		private static final long serialVersionUID = 1L;

		private final int precision;

		public InstantConverter(int precision) {
			this.precision = precision;
		}

		@Override
		TimestampData toInternalImpl(Instant value) {
			return TimestampData.fromInstant(value);
		}

		@Override
		Instant toExternalImpl(TimestampData value) {
			return value.toInstant();
		}

		@Override
		Instant toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getTimestamp(column, precision));
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
		Date toExternalImpl(RowData row, int column) {
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
		Time toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getInt(column));
		}
	}

	/**
	 * Converter for timestamp.
	 */
	public static final class TimestampConverter extends DataFormatConverter<TimestampData, Timestamp> {

		private static final long serialVersionUID = -779956524906131757L;

		private final int precision;

		public TimestampConverter(int precision) {
			this.precision = precision;
		}

		@Override
		TimestampData toInternalImpl(Timestamp value) {
			return TimestampData.fromTimestamp(value);
		}

		@Override
		Timestamp toExternalImpl(TimestampData value) {
			return value.toTimestamp();
		}

		@Override
		Timestamp toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getTimestamp(column, precision));
		}
	}

	/**
	 * Converter for primitive int array.
	 */
	public static final class PrimitiveIntArrayConverter extends DataFormatConverter<ArrayData, int[]> {

		private static final long serialVersionUID = 1780941126232395638L;

		public static final PrimitiveIntArrayConverter INSTANCE = new PrimitiveIntArrayConverter();

		private PrimitiveIntArrayConverter() {}

		@Override
		ArrayData toInternalImpl(int[] value) {
			return new GenericArrayData(value);
		}

		@Override
		int[] toExternalImpl(ArrayData value) {
			return value.toIntArray();
		}

		@Override
		int[] toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for primitive boolean array.
	 */
	public static final class PrimitiveBooleanArrayConverter extends DataFormatConverter<ArrayData, boolean[]> {

		private static final long serialVersionUID = -4037693692440282141L;

		public static final PrimitiveBooleanArrayConverter INSTANCE = new PrimitiveBooleanArrayConverter();

		private PrimitiveBooleanArrayConverter() {}

		@Override
		ArrayData toInternalImpl(boolean[] value) {
			return new GenericArrayData(value);
		}

		@Override
		boolean[] toExternalImpl(ArrayData value) {
			return value.toBooleanArray();
		}

		@Override
		boolean[] toExternalImpl(RowData row, int column) {
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
		byte[] toExternalImpl(RowData row, int column) {
			return row.getBinary(column);
		}
	}

	/**
	 * Converter for primitive short array.
	 */
	public static final class PrimitiveShortArrayConverter extends DataFormatConverter<ArrayData, short[]> {

		private static final long serialVersionUID = -1343184089311186834L;

		public static final PrimitiveShortArrayConverter INSTANCE = new PrimitiveShortArrayConverter();

		private PrimitiveShortArrayConverter() {}

		@Override
		ArrayData toInternalImpl(short[] value) {
			return new GenericArrayData(value);
		}

		@Override
		short[] toExternalImpl(ArrayData value) {
			return value.toShortArray();
		}

		@Override
		short[] toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for primitive long array.
	 */
	public static final class PrimitiveLongArrayConverter extends DataFormatConverter<ArrayData, long[]> {

		private static final long serialVersionUID = 4061982985342526078L;

		public static final PrimitiveLongArrayConverter INSTANCE = new PrimitiveLongArrayConverter();

		private PrimitiveLongArrayConverter() {}

		@Override
		ArrayData toInternalImpl(long[] value) {
			return new GenericArrayData(value);
		}

		@Override
		long[] toExternalImpl(ArrayData value) {
			return value.toLongArray();
		}

		@Override
		long[] toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for primitive float array.
	 */
	public static final class PrimitiveFloatArrayConverter extends DataFormatConverter<ArrayData, float[]> {

		private static final long serialVersionUID = -3237695040861141459L;

		public static final PrimitiveFloatArrayConverter INSTANCE = new PrimitiveFloatArrayConverter();

		private PrimitiveFloatArrayConverter() {}

		@Override
		ArrayData toInternalImpl(float[] value) {
			return new GenericArrayData(value);
		}

		@Override
		float[] toExternalImpl(ArrayData value) {
			return value.toFloatArray();
		}

		@Override
		float[] toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for primitive double array.
	 */
	public static final class PrimitiveDoubleArrayConverter extends DataFormatConverter<ArrayData, double[]> {

		private static final long serialVersionUID = 6333670535356315691L;

		public static final PrimitiveDoubleArrayConverter INSTANCE = new PrimitiveDoubleArrayConverter();

		private PrimitiveDoubleArrayConverter() {}

		@Override
		ArrayData toInternalImpl(double[] value) {
			return new GenericArrayData(value);
		}

		@Override
		double[] toExternalImpl(ArrayData value) {
			return value.toDoubleArray();
		}

		@Override
		double[] toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	/**
	 * Converter for object array.
	 */
	public static final class ObjectArrayConverter<T> extends DataFormatConverter<ArrayData, T[]> {

		private static final long serialVersionUID = -7434682160639380078L;

		private final Class<T> componentClass;
		private final LogicalType elementType;
		private final DataFormatConverter<Object, T> elementConverter;
		private final int elementSize;
		private final TypeSerializer<T> eleSer;
		private final boolean isEleIndentity;

		private transient BinaryArrayData reuseArray;
		private transient BinaryArrayWriter reuseWriter;

		public ObjectArrayConverter(DataType elementType) {
			this.componentClass = (Class) elementType.getConversionClass();
			this.elementType = LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(elementType);
			this.elementConverter = DataFormatConverters.getConverterForDataType(elementType);
			this.elementSize = BinaryArrayData.calculateFixLengthPartSize(this.elementType);
			this.eleSer = InternalSerializers.create(this.elementType);
			this.isEleIndentity = elementConverter instanceof IdentityConverter;
		}

		@Override
		ArrayData toInternalImpl(T[] value) {
			return isEleIndentity ? new GenericArrayData(value) : toBinaryArray(value);
		}

		private ArrayData toBinaryArray(T[] value) {
			if (reuseArray == null) {
				reuseArray = new BinaryArrayData();
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
			return reuseArray.copy();
		}

		@Override
		T[] toExternalImpl(ArrayData value) {
			return (isEleIndentity && value instanceof GenericArrayData) ?
					genericArrayToJavaArray((GenericArrayData) value, elementType) :
					arrayDataToJavaArray(value, elementType, componentClass, elementConverter);
		}

		@Override
		T[] toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getArray(column));
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> T[] genericArrayToJavaArray(GenericArrayData value, LogicalType eleType) {
		if (value.isPrimitiveArray()) {
			switch (eleType.getTypeRoot()) {
				case BOOLEAN:
					return (T[]) ArrayUtils.toObject(value.toBooleanArray());
				case TINYINT:
					return (T[]) ArrayUtils.toObject(value.toByteArray());
				case SMALLINT:
					return (T[]) ArrayUtils.toObject(value.toShortArray());
				case INTEGER:
					return (T[]) ArrayUtils.toObject(value.toIntArray());
				case BIGINT:
					return (T[]) ArrayUtils.toObject(value.toLongArray());
				case FLOAT:
					return (T[]) ArrayUtils.toObject(value.toFloatArray());
				case DOUBLE:
					return (T[]) ArrayUtils.toObject(value.toDoubleArray());
				default:
					throw new RuntimeException("Not a primitive type: " + eleType);
			}
		} else {
			return (T[]) value.toObjectArray();
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> T[] arrayDataToJavaArray(
			ArrayData value,
			LogicalType elementType,
			Class<T> componentClass,
			DataFormatConverter<Object, T> elementConverter) {
		int size = value.size();
		T[] values = (T[]) Array.newInstance(componentClass, size);
		for (int i = 0; i < size; i++) {
			if (value.isNullAt(i)) {
				values[i] = null;
			} else {
				values[i] = elementConverter.toExternalImpl(ArrayData.get(value, i, elementType));
			}
		}
		return values;
	}

	/**
	 * Converter for map.
	 */
	public static final class MapConverter extends DataFormatConverter<MapData, Map> {

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

		private transient BinaryArrayData reuseKArray;
		private transient BinaryArrayWriter reuseKWriter;
		private transient BinaryArrayData reuseVArray;
		private transient BinaryArrayWriter reuseVWriter;

		public MapConverter(DataType keyTypeInfo, DataType valueTypeInfo) {
			this.keyType = LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(keyTypeInfo);
			this.valueType = LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(valueTypeInfo);
			this.keyConverter = DataFormatConverters.getConverterForDataType(keyTypeInfo);
			this.valueConverter = DataFormatConverters.getConverterForDataType(valueTypeInfo);
			this.keyElementSize = BinaryArrayData.calculateFixLengthPartSize(keyType);
			this.valueElementSize = BinaryArrayData.calculateFixLengthPartSize(valueType);
			this.keyComponentClass = keyTypeInfo.getConversionClass();
			this.valueComponentClass = valueTypeInfo.getConversionClass();
			this.isKeyValueIndentity = keyConverter instanceof IdentityConverter &&
					valueConverter instanceof IdentityConverter;
			this.keySer = InternalSerializers.create(this.keyType);
			this.valueSer = InternalSerializers.create(this.valueType);
		}

		@Override
		MapData toInternalImpl(Map value) {
			return isKeyValueIndentity ? new GenericMapData(value) : toBinaryMap(value);
		}

		private MapData toBinaryMap(Map value) {
			if (reuseKArray == null) {
				reuseKArray = new BinaryArrayData();
				reuseVArray = new BinaryArrayData();
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
			return BinaryMapData.valueOf(reuseKArray, reuseVArray);
		}

		@SuppressWarnings("unchecked")
		@Override
		Map toExternalImpl(MapData map) {
			Map<Object, Object> javaMap = new HashMap<>();
			ArrayData keyArray = map.keyArray();
			ArrayData valueArray = map.valueArray();
			for (int i = 0; i < map.size(); i++) {
				Object key;
				Object value;
				if (keyArray.isNullAt(i)) {
					key = null;
				} else {
					key = keyConverter.toExternalImpl(ArrayData.get(keyArray, i, keyType));
				}
				if (valueArray.isNullAt(i)) {
					value = null;
				} else {
					value = valueConverter.toExternalImpl(ArrayData.get(valueArray, i, valueType));
				}
				javaMap.put(key, value);
			}
			return javaMap;
		}

		@Override
		Map toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getMap(column));
		}
	}

	/**
	 * Abstract converter for internal base row.
	 */
	public abstract static class AbstractRowDataConverter<E> extends DataFormatConverter<RowData, E> {

		private static final long serialVersionUID = 4365740929854771618L;

		protected final DataFormatConverter[] converters;

		public AbstractRowDataConverter(DataType[] fieldTypes) {
			converters = new DataFormatConverter[fieldTypes.length];
			for (int i = 0; i < converters.length; i++) {
				converters[i] = getConverterForDataType(fieldTypes[i]);
			}
		}

		@Override
		E toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getRow(column, converters.length));
		}
	}

	/**
	 * Converter for base row.
	 */
	public static final class RowDataConverter extends IdentityConverter<RowData> {

		private static final long serialVersionUID = -4470307402371540680L;
		private int arity;

		private RowDataConverter(int arity) {
			this.arity = arity;
		}

		@Override
		RowData toExternalImpl(RowData row, int column) {
			return row.getRow(column, arity);
		}
	}

	/**
	 * Converter for pojo.
	 */
	public static final class PojoConverter<T> extends AbstractRowDataConverter<T> {

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
		RowData toInternalImpl(T value) {
			GenericRowData genericRow = new GenericRowData(t.getArity());
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
		T toExternalImpl(RowData value) {
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
	public static final class RowConverter extends AbstractRowDataConverter<Row> {

		private static final long serialVersionUID = -56553502075225785L;

		public RowConverter(DataType[] fieldTypes) {
			super(fieldTypes);
		}

		@Override
		RowData toInternalImpl(Row value) {
			GenericRowData genericRow = new GenericRowData(value.getKind(), converters.length);
			for (int i = 0; i < converters.length; i++) {
				genericRow.setField(i, converters[i].toInternal(value.getField(i)));
			}
			return genericRow;
		}

		@Override
		Row toExternalImpl(RowData value) {
			Row row = new Row(value.getRowKind(), converters.length);
			for (int i = 0; i < converters.length; i++) {
				row.setField(i, converters[i].toExternal(value, i));
			}
			return row;
		}
	}

	/**
	 * Converter for flink tuple.
	 */
	public static final class TupleConverter extends AbstractRowDataConverter<Tuple> {

		private static final long serialVersionUID = 2794892691010934194L;

		private final Class<Tuple> clazz;

		public TupleConverter(Class<Tuple> clazz, DataType[] fieldTypes) {
			super(fieldTypes);
			this.clazz = clazz;
		}

		@Override
		RowData toInternalImpl(Tuple value) {
			GenericRowData genericRow = new GenericRowData(converters.length);
			for (int i = 0; i < converters.length; i++) {
				genericRow.setField(i, converters[i].toInternal(value.getField(i)));
			}
			return genericRow;
		}

		@Override
		Tuple toExternalImpl(RowData value) {
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
	public static final class CaseClassConverter extends AbstractRowDataConverter<Product> {

		private static final long serialVersionUID = -966598627968372952L;

		private final TupleTypeInfoBase t;
		private final TupleSerializerBase serializer;

		public CaseClassConverter(TupleTypeInfoBase t, DataType[] fieldTypes) {
			super(fieldTypes);
			this.t = t;
			this.serializer = (TupleSerializerBase) t.createSerializer(new ExecutionConfig());
		}

		@Override
		RowData toInternalImpl(Product value) {
			GenericRowData genericRow = new GenericRowData(t.getArity());
			for (int i = 0; i < t.getArity(); i++) {
				genericRow.setField(i, converters[i].toInternal(value.productElement(i)));
			}
			return genericRow;
		}

		@Override
		Product toExternalImpl(RowData value) {
			Object[] fields = new Object[t.getArity()];
			for (int i = 0; i < t.getArity(); i++) {
				fields[i] = converters[i].toExternal(value, i);
			}
			return (Product) serializer.createInstance(fields);
		}
	}

	/**
	 * Converter for Long and TimestampData.
	 */
	public static final class LongTimestampDataConverter extends DataFormatConverter<TimestampData, Long> {

		private static final long serialVersionUID = 1L;

		private final int precision;

		public LongTimestampDataConverter(int precision) {
			this.precision = precision;
		}

		@Override
		TimestampData toInternalImpl(Long value) {
			return TimestampData.fromEpochMillis(value);
		}

		@Override
		Long toExternalImpl(TimestampData value) {
			return value.getMillisecond();
		}

		@Override
		Long toExternalImpl(RowData row, int column) {
			return toExternalImpl(row.getTimestamp(column, precision));
		}
	}

	/**
	 * Converter for {@link TimestampData} class.
	 */
	public static final class TimestampDataConverter extends IdentityConverter<TimestampData> {

		private static final long serialVersionUID = 1L;

		private final int precision;

		public TimestampDataConverter(int precision) {
			this.precision = precision;
		}

		@Override
		TimestampData toExternalImpl(RowData row, int column) {
			return row.getTimestamp(column, precision);
		}
	}
}
