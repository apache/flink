/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.streaming.connectors.fs.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * a map function to Convert Row to GenericRecord .
 */
public class ConvertMapFunction implements MapFunction<Row, GenericRecord> {

	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();
	private String schemaString;

	public ConvertMapFunction(String schemaString) {
		this.schemaString = schemaString;
	}

	@Override
	public GenericRecord map(Row value) throws Exception {
		Schema schema = new Schema.Parser().parse(schemaString);
		return convertRowToAvroRecord(schema, value);
	}

	private GenericRecord convertRowToAvroRecord(Schema schema, Row row) {
		final List<Schema.Field> fields = schema.getFields();
		final int length = fields.size();
		final GenericRecord record = new GenericData.Record(schema);
		for (int i = 0; i < length; i++) {
			final Schema.Field field = fields.get(i);
			record.put(i, convertFlinkType(field.schema(), row.getField(i)));
		}
		return record;
	}

	private Object convertFlinkType(Schema schema, Object object) {
		if (object == null) {
			return null;
		}
		switch (schema.getType()) {
			case RECORD:
				if (object instanceof Row) {
					return convertRowToAvroRecord(schema, (Row) object);
				}
				throw new IllegalStateException("Row expected but was: " + object.getClass());
			case ENUM:
				return new GenericData.EnumSymbol(schema, object.toString());
			case ARRAY:
				final Schema elementSchema = schema.getElementType();
				final Object[] array = (Object[]) object;
				final GenericData.Array<Object> convertedArray = new GenericData.Array<>(
					array.length,
					schema);
				for (Object element : array) {
					convertedArray.add(convertFlinkType(elementSchema, element));
				}
				return convertedArray;
			case MAP:
				final Map<?, ?> map = (Map<?, ?>) object;
				final Map<Utf8, Object> convertedMap = new HashMap<>();
				for (Map.Entry<?, ?> entry : map.entrySet()) {
					convertedMap.put(
						new Utf8(entry.getKey().toString()),
						convertFlinkType(schema.getValueType(), entry.getValue()));
				}
				return convertedMap;
			case UNION:
				final List<Schema> types = schema.getTypes();
				final int size = types.size();
				final Schema actualSchema;
				if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
					actualSchema = types.get(1);
				} else if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
					actualSchema = types.get(0);
				} else if (size == 1) {
					actualSchema = types.get(0);
				} else {
					// generic type
					return object;
				}
				return convertFlinkType(actualSchema, object);
			case FIXED:
				// check for logical type
				if (object instanceof BigDecimal) {
					return new GenericData.Fixed(
						schema,
						convertFromDecimal(schema, (BigDecimal) object));
				}
				return new GenericData.Fixed(schema, (byte[]) object);
			case STRING:
				return new Utf8(object.toString());
			case BYTES:
				// check for logical type
				if (object instanceof BigDecimal) {
					return ByteBuffer.wrap(convertFromDecimal(schema, (BigDecimal) object));
				}
				return ByteBuffer.wrap((byte[]) object);
			case INT:
				// check for logical types
				if (object instanceof Date) {
					return convertFromDate(schema, (Date) object);
				} else if (object instanceof Time) {
					return convertFromTime(schema, (Time) object);
				}
				return object;
			case LONG:
				// check for logical type
				if (object instanceof Timestamp) {
					return convertFromTimestamp(schema, (Timestamp) object);
				}
				return object;
			case FLOAT:
			case DOUBLE:
			case BOOLEAN:
				return object;
		}
		throw new RuntimeException("Unsupported Avro type:" + schema);
	}

	private byte[] convertFromDecimal(Schema schema, BigDecimal decimal) {
		final LogicalType logicalType = schema.getLogicalType();
		if (logicalType instanceof LogicalTypes.Decimal) {
			final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
			// rescale to target type
			final BigDecimal rescaled = decimal.setScale(
				decimalType.getScale(),
				BigDecimal.ROUND_UNNECESSARY);
			// byte array must contain the two's-complement representation of the
			// unscaled integer value in big-endian byte order
			return decimal.unscaledValue().toByteArray();
		} else {
			throw new RuntimeException("Unsupported decimal type.");
		}
	}

	private int convertFromDate(Schema schema, Date date) {
		final LogicalType logicalType = schema.getLogicalType();
		if (logicalType == LogicalTypes.date()) {
			// adopted from Apache Calcite
			final long time = date.getTime();
			final long converted = time + (long) LOCAL_TZ.getOffset(time);
			return (int) (converted / 86400000L);
		} else {
			throw new RuntimeException("Unsupported date type.");
		}
	}

	private int convertFromTime(Schema schema, Time date) {
		final LogicalType logicalType = schema.getLogicalType();
		if (logicalType == LogicalTypes.timeMillis()) {
			// adopted from Apache Calcite
			final long time = date.getTime();
			final long converted = time + (long) LOCAL_TZ.getOffset(time);
			return (int) (converted % 86400000L);
		} else {
			throw new RuntimeException("Unsupported time type.");
		}
	}

	private long convertFromTimestamp(Schema schema, Timestamp date) {
		final LogicalType logicalType = schema.getLogicalType();
		if (logicalType == LogicalTypes.timestampMillis()) {
			// adopted from Apache Calcite
			final long time = date.getTime();
			return time + (long) LOCAL_TZ.getOffset(time);
		} else {
			throw new RuntimeException("Unsupported timestamp type.");
		}
	}
}
