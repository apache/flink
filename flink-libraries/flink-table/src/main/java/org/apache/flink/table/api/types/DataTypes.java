/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.types;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utils for types.
 */
public class DataTypes {

	/**
	 * External: we use jdk {@link String}.
	 * SQL engine internal data structure: we use {@link BinaryString} to compute.
	 */
	public static final StringType STRING = StringType.INSTANCE;

	/**
	 * We use java {@code boolean} to compute both internal and external.
	 */
	public static final BooleanType BOOLEAN = BooleanType.INSTANCE;

	/**
	 * We use java {@code double} to compute both internal and external.
	 */
	public static final DoubleType DOUBLE = DoubleType.INSTANCE;

	/**
	 * We use java {@code float} to compute both internal and external.
	 */
	public static final FloatType FLOAT = FloatType.INSTANCE;

	/**
	 * We use java {@code byte} to compute both internal and external.
	 */
	public static final ByteType BYTE = ByteType.INSTANCE;

	/**
	 * We use java {@code int} to compute both internal and external.
	 */
	public static final IntType INT = IntType.INSTANCE;

	/**
	 * We use java {@code long} to compute both internal and external.
	 */
	public static final LongType LONG = LongType.INSTANCE;

	/**
	 * We use java {@code short} to compute both internal and external.
	 */
	public static final ShortType SHORT = ShortType.INSTANCE;

	/**
	 * We use java {@code char} to compute both internal and external.
	 */
	public static final CharType CHAR = CharType.INSTANCE;

	/**
	 * We use java {@code byte[]} to compute both internal and external.
	 */
	public static final ByteArrayType BYTE_ARRAY = ByteArrayType.INSTANCE;

	/**
	 * External: we use jdk {@link java.sql.Date}.
	 * SQL engine internal data structure: we use {@code int} to compute.
	 */
	public static final DateType DATE = DateType.DATE;

	/**
	 * External: we use jdk {@link java.sql.Timestamp}.
	 * SQL engine internal data structure: we use {@code long} to compute.
	 */
	public static final TimestampType TIMESTAMP = TimestampType.TIMESTAMP;

	/**
	 * External: we use jdk {@link java.sql.Time}.
	 * SQL engine internal data structure: we use {@code int} to compute.
	 */
	public static final TimeType TIME = TimeType.INSTANCE;

	public static final DateType INTERVAL_MONTHS = DateType.INTERVAL_MONTHS;

	public static final TimestampType INTERVAL_MILLIS = TimestampType.INTERVAL_MILLIS;

	public static final TimestampType ROWTIME_INDICATOR = TimestampType.ROWTIME_INDICATOR;

	public static final TimestampType PROCTIME_INDICATOR = TimestampType.PROCTIME_INDICATOR;

	public static final IntervalRowsType INTERVAL_ROWS = IntervalRowsType.INSTANCE;

	public static final IntervalRangeType INTERVAL_RANGE = IntervalRangeType.INSTANCE;

	public static final List<PrimitiveType> INTEGRAL_TYPES = Arrays.asList(BYTE, SHORT, INT, LONG);

	public static final List<PrimitiveType> FRACTIONAL_TYPES = Arrays.asList(FLOAT, DOUBLE);

	/**
	 * The special field index indicates that this is a row time field.
	 */
	public static final int ROWTIME_STREAM_MARKER = -1;

	/**
	 * The special field index indicates that this is a proc time field.
	 */
	public static final int PROCTIME_STREAM_MARKER = -2;

	/**
	 * The special field index indicates that this is a row time field.
	 */
	public static final int ROWTIME_BATCH_MARKER = -3;

	/**
	 * The special field index indicates that this is a proc time field.
	 */
	public static final int PROCTIME_BATCH_MARKER = -4;

	public static ArrayType createArrayType(DataType elementType) {
		return new ArrayType(elementType);
	}

	public static ArrayType createPrimitiveArrayType(DataType elementType) {
		return new ArrayType(elementType, true);
	}

	public static DecimalType createDecimalType(int precision, int scale) {
		return new DecimalType(precision, scale);
	}

	public static MapType createMapType(DataType keyType, DataType valueType) {
		return new MapType(keyType, valueType);
	}

	public static <T> GenericType<T> createGenericType(Class<T> cls) {
		return new GenericType<>(cls);
	}

	public static <T> GenericType<T> createGenericType(TypeInformation<T> typeInfo) {
		return new GenericType<>(typeInfo);
	}

	public static MultisetType createMultisetType(DataType elementType) {
		return new MultisetType(elementType);
	}

	public static RowType createRowType(DataType[] types, String[] fieldNames) {
		return new RowType(types, fieldNames);
	}

	public static RowType createRowType(InternalType[] types, String[] fieldNames) {
		return new RowType(types, fieldNames);
	}

	public static RowType createRowType(DataType... types) {
		return new RowType(types);
	}

	public static RowType createRowType(InternalType... types) {
		return new RowType(types);
	}

	public static <T extends Tuple> DataType createTupleType(Class<T> cls, DataType... types) {
		TupleTypeInfo<T> typeInfo = new TupleTypeInfo<>(cls, TypeConverters.createExternalTypeInfoFromDataTypes(types));
		return new TypeInfoWrappedDataType(typeInfo);
	}

	@SuppressWarnings("unchecked")
	public static DataType createTupleType(DataType... types) {
		TupleTypeInfo typeInfo = new TupleTypeInfo(TypeConverters.createExternalTypeInfoFromDataTypes(types));
		return new TypeInfoWrappedDataType(typeInfo);
	}

	public static <T> PojoBuilder pojoBuilder(Class<T> typeClass) {
		return new PojoBuilder<>(typeClass);
	}

	/**
	 * Builder for external pojo type.
	 */
	public static class PojoBuilder<T> {

		private Class<T> typeClass;
		private List<PojoField> fields;

		private PojoBuilder(Class<T> typeClass) {
			this.typeClass = typeClass;
			this.fields = new ArrayList<>();
		}

		public PojoBuilder field(String name, DataType type) throws NoSuchFieldException {
			fields.add(new PojoField(typeClass.getDeclaredField(name),
					TypeConverters.createExternalTypeInfoFromDataType(type)));
			return this;
		}

		public TypeInfoWrappedDataType build() {
			PojoTypeInfo<T> typeInfo = new PojoTypeInfo<T>(typeClass, fields);
			return new TypeInfoWrappedDataType(typeInfo);
		}
	}

	/**
	 * We can extract the correct Type from most Classes, such as Pojo.
	 * But if it is a Row, we can only extract the GenericType because we don't have the
	 * information of the fields.
	 */
	public static DataType extractDataType(Class<?> cls) {
		return new TypeInfoWrappedDataType(TypeExtractor.createTypeInfo(cls));
	}

	/**
	 * Create a external serializer.
	 *
	 * <p>Eg:
	 * {@code DataTypes.String} => Serializer for {@link String}.
	 * {@link DecimalType} => Serializer for {@link BigDecimal}.
	 * {@link RowType} => Serializer for {@link Row}.
	 */
	public static TypeSerializer createExternalSerializer(DataType type) {
		return TypeConverters.createExternalTypeInfoFromDataType(type).createSerializer(new ExecutionConfig());
	}

	/**
	 * Create a internal serializer. In the SQL execution engine, we use internal data structures.
	 *
	 * <p>Eg:
	 * {@code DataTypes.String} => Serializer for {@link BinaryString}.
	 * {@link DecimalType} => Serializer for {@link Decimal}.
	 * {@link RowType} => Serializer for {@link BaseRow}.
	 */
	public static TypeSerializer createInternalSerializer(DataType type) {
		return TypeConverters.createInternalTypeInfoFromDataType(type).createSerializer(new ExecutionConfig());
	}
}
