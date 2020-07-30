/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.extractValueTypeToAvroMap;
import static org.joda.time.DateTimeConstants.MILLIS_PER_DAY;

/** Tool class used to convert from Avro {@link GenericRecord} to {@link RowData}. **/
@Internal
public class AvroToRowDataConverters {

	/**
	 * Runtime converter that converts Avro data structures into objects of Flink Table & SQL
	 * internal data structures.
	 */
	@FunctionalInterface
	public interface AvroToRowDataConverter extends Serializable {
		Object convert(Object object);
	}

	// -------------------------------------------------------------------------------------
	// Runtime Converters
	// -------------------------------------------------------------------------------------

	public static AvroToRowDataConverter createRowConverter(RowType rowType) {
		final AvroToRowDataConverter[] fieldConverters = rowType.getFields().stream()
				.map(RowType.RowField::getType)
				.map(AvroToRowDataConverters::createNullableConverter)
				.toArray(AvroToRowDataConverter[]::new);
		final int arity = rowType.getFieldCount();

		return avroObject -> {
			IndexedRecord record = (IndexedRecord) avroObject;
			GenericRowData row = new GenericRowData(arity);
			for (int i = 0; i < arity; ++i) {
				row.setField(i, fieldConverters[i].convert(record.get(i)));
			}
			return row;
		};
	}

	/**
	 * Creates a runtime converter which is null safe.
	 */
	private static AvroToRowDataConverter createNullableConverter(LogicalType type) {
		final AvroToRowDataConverter converter = createConverter(type);
		return avroObject -> {
			if (avroObject == null) {
				return null;
			}
			return converter.convert(avroObject);
		};
	}

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	private static AvroToRowDataConverter createConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
		case NULL:
			return avroObject -> null;
		case TINYINT:
			return avroObject -> ((Integer) avroObject).byteValue();
		case SMALLINT:
			return avroObject -> ((Integer) avroObject).shortValue();
		case BOOLEAN: // boolean
		case INTEGER: // int
		case INTERVAL_YEAR_MONTH: // long
		case BIGINT: // long
		case INTERVAL_DAY_TIME: // long
		case FLOAT: // float
		case DOUBLE: // double
			return avroObject -> avroObject;
		case DATE:
			return AvroToRowDataConverters::convertToDate;
		case TIME_WITHOUT_TIME_ZONE:
			return AvroToRowDataConverters::convertToTime;
		case TIMESTAMP_WITHOUT_TIME_ZONE:
			return AvroToRowDataConverters::convertToTimestamp;
		case CHAR:
		case VARCHAR:
			return avroObject -> StringData.fromString(avroObject.toString());
		case BINARY:
		case VARBINARY:
			return AvroToRowDataConverters::convertToBytes;
		case DECIMAL:
			return createDecimalConverter((DecimalType) type);
		case ARRAY:
			return createArrayConverter((ArrayType) type);
		case ROW:
			return createRowConverter((RowType) type);
		case MAP:
		case MULTISET:
			return createMapConverter(type);
		case RAW:
		default:
			throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	private static AvroToRowDataConverter createDecimalConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return avroObject -> {
			final byte[] bytes;
			if (avroObject instanceof GenericFixed) {
				bytes = ((GenericFixed) avroObject).bytes();
			} else if (avroObject instanceof ByteBuffer) {
				ByteBuffer byteBuffer = (ByteBuffer) avroObject;
				bytes = new byte[byteBuffer.remaining()];
				byteBuffer.get(bytes);
			} else {
				bytes = (byte[]) avroObject;
			}
			return DecimalData.fromUnscaledBytes(bytes, precision, scale);
		};
	}

	private static AvroToRowDataConverter createArrayConverter(ArrayType arrayType) {
		final AvroToRowDataConverter elementConverter = createNullableConverter(arrayType.getElementType());
		final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());

		return avroObject -> {
			final List<?> list = (List<?>) avroObject;
			final int length = list.size();
			final Object[] array = (Object[]) Array.newInstance(elementClass, length);
			for (int i = 0; i < length; ++i) {
				array[i] = elementConverter.convert(list.get(i));
			}
			return new GenericArrayData(array);
		};
	}

	private static AvroToRowDataConverter createMapConverter(LogicalType type) {
		final AvroToRowDataConverter keyConverter = createConverter(
				DataTypes.STRING().getLogicalType());
		final AvroToRowDataConverter valueConverter = createConverter(
				extractValueTypeToAvroMap(type));

		return avroObject -> {
			final Map<?, ?> map = (Map<?, ?>) avroObject;
			Map<Object, Object> result = new HashMap<>();
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				Object key = keyConverter.convert(entry.getKey());
				Object value = valueConverter.convert(entry.getValue());
				result.put(key, value);
			}
			return new GenericMapData(result);
		};
	}

	private static TimestampData convertToTimestamp(Object object) {
		final long millis;
		if (object instanceof Long) {
			millis = (Long) object;
		} else {
			// use 'provided' Joda time
			final DateTime value = (DateTime) object;
			millis = value.toDate().getTime();
		}
		return toTimestampData(millis);
	}

	private static int convertToDate(Object object) {
		if (object instanceof Integer) {
			return (Integer) object;
		} else {
			// use 'provided' Joda time
			final LocalDate value = (LocalDate) object;
			return (int) (toTimestampData(value.toDate().getTime()).getMillisecond() / MILLIS_PER_DAY);
		}
	}

	private static TimestampData toTimestampData(long timeZoneMills) {
		return TimestampData.fromTimestamp(new Timestamp(timeZoneMills));
	}

	private static int convertToTime(Object object) {
		final int millis;
		if (object instanceof Integer) {
			millis = (Integer) object;
		} else {
			// use 'provided' Joda time
			final org.joda.time.LocalTime value = (org.joda.time.LocalTime) object;
			millis = value.get(DateTimeFieldType.millisOfDay());
		}
		return millis;
	}

	private static byte[] convertToBytes(Object object) {
		if (object instanceof GenericFixed) {
			return ((GenericFixed) object).bytes();
		} else if (object instanceof ByteBuffer) {
			ByteBuffer byteBuffer = (ByteBuffer) object;
			byte[] bytes = new byte[byteBuffer.remaining()];
			byteBuffer.get(bytes);
			return bytes;
		} else {
			return (byte[]) object;
		}
	}
}
