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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Registry of available data structure converters.
 *
 * <p>Data structure converters are used at the edges for the API for converting between internal
 * structures (see {@link RowData}) and external structures (see {@link DataType#getConversionClass()}).
 *
 * <p>This is useful for UDFs, sources, sinks, or exposing data in the API (e.g. via a {@code collect()}).
 *
 * <p>Note: It is NOT the responsibility of a converter to normalize the data. Thus, a converter does
 * neither change the precision of a timestamp nor prune/expand strings to their defined length. This
 * might be the responsibility of data classes that are called transitively.
 */
@Internal
public final class DataStructureConverters {

	private static final Map<ConverterIdentifier<?>, DataStructureConverterFactory> converters = new HashMap<>();
	static {
		// ordered by type root and conversion class definition
		putConverter(LogicalTypeRoot.CHAR, String.class, constructor(StringStringConverter::new));
		putConverter(LogicalTypeRoot.CHAR, byte[].class, constructor(StringByteArrayConverter::new));
		putConverter(LogicalTypeRoot.CHAR, StringData.class, identity());
		putConverter(LogicalTypeRoot.VARCHAR, String.class, constructor(StringStringConverter::new));
		putConverter(LogicalTypeRoot.VARCHAR, byte[].class, constructor(StringByteArrayConverter::new));
		putConverter(LogicalTypeRoot.VARCHAR, StringData.class, identity());
		putConverter(LogicalTypeRoot.BOOLEAN, Boolean.class, identity());
		putConverter(LogicalTypeRoot.BOOLEAN, boolean.class, identity());
		putConverter(LogicalTypeRoot.BINARY, byte[].class, identity());
		putConverter(LogicalTypeRoot.VARBINARY, byte[].class, identity());
		putConverter(LogicalTypeRoot.DECIMAL, BigDecimal.class, DecimalBigDecimalConverter::create);
		putConverter(LogicalTypeRoot.DECIMAL, DecimalData.class, identity());
		putConverter(LogicalTypeRoot.TINYINT, Byte.class, identity());
		putConverter(LogicalTypeRoot.TINYINT, byte.class, identity());
		putConverter(LogicalTypeRoot.SMALLINT, Short.class, identity());
		putConverter(LogicalTypeRoot.SMALLINT, short.class, identity());
		putConverter(LogicalTypeRoot.INTEGER, Integer.class, identity());
		putConverter(LogicalTypeRoot.INTEGER, int.class, identity());
		putConverter(LogicalTypeRoot.BIGINT, Long.class, identity());
		putConverter(LogicalTypeRoot.BIGINT, long.class, identity());
		putConverter(LogicalTypeRoot.FLOAT, Float.class, identity());
		putConverter(LogicalTypeRoot.FLOAT, float.class, identity());
		putConverter(LogicalTypeRoot.DOUBLE, Double.class, identity());
		putConverter(LogicalTypeRoot.DOUBLE, double.class, identity());
		putConverter(LogicalTypeRoot.DATE, java.sql.Date.class, constructor(DateDateConverter::new));
		putConverter(LogicalTypeRoot.DATE, java.time.LocalDate.class, constructor(DateLocalDateConverter::new));
		putConverter(LogicalTypeRoot.DATE, Integer.class, identity());
		putConverter(LogicalTypeRoot.DATE, int.class, identity());
		putConverter(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, java.sql.Time.class, constructor(TimeTimeConverter::new));
		putConverter(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, java.time.LocalTime.class, constructor(TimeLocalTimeConverter::new));
		putConverter(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, Integer.class, identity());
		putConverter(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, int.class, identity());
		putConverter(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, Long.class, constructor(TimeLongConverter::new));
		putConverter(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, long.class, constructor(TimeLongConverter::new));
		putConverter(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, java.sql.Timestamp.class, constructor(TimestampTimestampConverter::new));
		putConverter(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, java.time.LocalDateTime.class, constructor(TimestampLocalDateTimeConverter::new));
		putConverter(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, TimestampData.class, identity());
		putConverter(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, java.time.ZonedDateTime.class, unsupported());
		putConverter(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, java.time.OffsetDateTime.class, unsupported());
		putConverter(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, java.time.Instant.class, constructor(LocalZonedTimestampInstantConverter::new));
		putConverter(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, Integer.class, constructor(LocalZonedTimestampIntConverter::new));
		putConverter(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, int.class, constructor(LocalZonedTimestampIntConverter::new));
		putConverter(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, Long.class, constructor(LocalZonedTimestampLongConverter::new));
		putConverter(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, long.class, constructor(LocalZonedTimestampLongConverter::new));
		putConverter(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, TimestampData.class, identity());
		putConverter(LogicalTypeRoot.INTERVAL_YEAR_MONTH, java.time.Period.class, YearMonthIntervalPeriodConverter::create);
		putConverter(LogicalTypeRoot.INTERVAL_YEAR_MONTH, Integer.class, identity());
		putConverter(LogicalTypeRoot.INTERVAL_YEAR_MONTH, int.class, identity());
		putConverter(LogicalTypeRoot.INTERVAL_DAY_TIME, java.time.Duration.class, constructor(DayTimeIntervalDurationConverter::new));
		putConverter(LogicalTypeRoot.INTERVAL_DAY_TIME, Long.class, identity());
		putConverter(LogicalTypeRoot.INTERVAL_DAY_TIME, long.class, identity());
		putConverter(LogicalTypeRoot.ARRAY, ArrayData.class, identity());
		putConverter(LogicalTypeRoot.ARRAY, boolean[].class, constructor(ArrayBooleanArrayConverter::new));
		putConverter(LogicalTypeRoot.ARRAY, byte[].class, constructor(ArrayByteArrayConverter::new));
		putConverter(LogicalTypeRoot.ARRAY, short[].class, constructor(ArrayShortArrayConverter::new));
		putConverter(LogicalTypeRoot.ARRAY, int[].class, constructor(ArrayIntArrayConverter::new));
		putConverter(LogicalTypeRoot.ARRAY, long[].class, constructor(ArrayLongArrayConverter::new));
		putConverter(LogicalTypeRoot.ARRAY, float[].class, constructor(ArrayFloatArrayConverter::new));
		putConverter(LogicalTypeRoot.ARRAY, double[].class, constructor(ArrayDoubleArrayConverter::new));
		putConverter(LogicalTypeRoot.MULTISET, MapData.class, identity());
		putConverter(LogicalTypeRoot.MAP, MapData.class, identity());
		putConverter(LogicalTypeRoot.ROW, Row.class, RowRowConverter::create);
		putConverter(LogicalTypeRoot.ROW, RowData.class, identity());
		putConverter(LogicalTypeRoot.STRUCTURED_TYPE, Row.class, RowRowConverter::create);
		putConverter(LogicalTypeRoot.STRUCTURED_TYPE, RowData.class, identity());
		putConverter(LogicalTypeRoot.RAW, byte[].class, RawByteArrayConverter::create);
		putConverter(LogicalTypeRoot.RAW, RawValueData.class, identity());
	}

	/**
	 * Returns a converter for the given {@link DataType}.
	 */
	@SuppressWarnings("unchecked")
	public static DataStructureConverter<Object, Object> getConverter(DataType dataType) {
		// cast to Object for ease of use
		return (DataStructureConverter<Object, Object>) getConverterInternal(dataType);
	}

	private static DataStructureConverter<?, ?> getConverterInternal(DataType dataType) {
		final LogicalType logicalType = dataType.getLogicalType();
		final DataStructureConverterFactory factory = converters.get(
			new ConverterIdentifier<>(
				logicalType.getTypeRoot(),
				dataType.getConversionClass()));
		if (factory != null) {
			return factory.createConverter(dataType);
		}
		// special cases
		switch (logicalType.getTypeRoot()) {
			case ARRAY:
				return ArrayObjectArrayConverter.create(dataType);
			case MULTISET:
				return MapMapConverter.createForMultisetType(dataType);
			case MAP:
				return MapMapConverter.createForMapType(dataType);
			case DISTINCT_TYPE:
				return getConverterInternal(dataType.getChildren().get(0));
			case STRUCTURED_TYPE:
				return StructuredObjectConverter.create(dataType);
			case RAW:
				return RawObjectConverter.create(dataType);
			default:
				throw new TableException("Could not find converter for data type: " + dataType);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Helper methods
	// --------------------------------------------------------------------------------------------

	private static <E> void putConverter(
			LogicalTypeRoot root,
			Class<E> conversionClass,
			DataStructureConverterFactory factory) {
		converters.put(new ConverterIdentifier<>(root, conversionClass), factory);
	}

	private static DataStructureConverterFactory identity() {
		return constructor(IdentityConverter::new);
	}

	private static DataStructureConverterFactory constructor(Supplier<DataStructureConverter<?, ?>> supplier) {
		return dataType -> supplier.get();
	}

	private static DataStructureConverterFactory unsupported() {
		return dataType -> {
			throw new TableException("Unsupported data type: " + dataType);
		};
	}

	// --------------------------------------------------------------------------------------------
	// Helper classes
	// --------------------------------------------------------------------------------------------

	private static class ConverterIdentifier<E> {

		final LogicalTypeRoot root;

		final Class<E> conversionClass;

		ConverterIdentifier(LogicalTypeRoot root, Class<E> conversionClass) {
			this.root = root;
			this.conversionClass = conversionClass;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ConverterIdentifier<?> that = (ConverterIdentifier<?>) o;
			return root == that.root && conversionClass.equals(that.conversionClass);
		}

		@Override
		public int hashCode() {
			return Objects.hash(root, conversionClass);
		}
	}

	private interface DataStructureConverterFactory {
		DataStructureConverter<?, ?> createConverter(DataType dt);
	}
}
