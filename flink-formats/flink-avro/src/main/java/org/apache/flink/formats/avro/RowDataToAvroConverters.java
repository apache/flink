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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.extractValueTypeToAvroMap;

/** Tool class used to convert from {@link RowData} to Avro {@link GenericRecord}. **/
@Internal
public class RowDataToAvroConverters {

	// --------------------------------------------------------------------------------
	// Runtime Converters
	// --------------------------------------------------------------------------------

	/**
	 * Runtime converter that converts objects of Flink Table & SQL internal data structures
	 * to corresponding Avro data structures.
	 */
	@FunctionalInterface
	public interface RowDataToAvroConverter extends Serializable {
		Object convert(Schema schema, Object object);
	}

	/**
	 * Creates a runtime converter accroding to the given logical type that converts objects
	 * of Flink Table & SQL internal data structures to corresponding Avro data structures.
	 */
	public static RowDataToAvroConverter createConverter(LogicalType type) {
		final RowDataToAvroConverter converter;
		switch (type.getTypeRoot()) {
		case NULL:
			converter = (schema, object) -> null;
			break;
		case TINYINT:
			converter = (schema, object) -> ((Byte) object).intValue();
			break;
		case SMALLINT:
			converter = (schema, object) -> ((Short) object).intValue();
			break;
		case BOOLEAN: // boolean
		case INTEGER: // int
		case INTERVAL_YEAR_MONTH: // long
		case BIGINT: // long
		case INTERVAL_DAY_TIME: // long
		case FLOAT: // float
		case DOUBLE: // double
		case TIME_WITHOUT_TIME_ZONE: // int
		case DATE: // int
			converter = (schema, object) -> object;
			break;
		case CHAR:
		case VARCHAR:
			converter = (schema, object) -> new Utf8(object.toString());
			break;
		case BINARY:
		case VARBINARY:
			converter = (schema, object) -> ByteBuffer.wrap((byte[]) object);
			break;
		case TIMESTAMP_WITHOUT_TIME_ZONE:
			converter = (schema, object) -> ((TimestampData) object).toInstant().toEpochMilli();
			break;
		case DECIMAL:
			converter = (schema, object) -> ByteBuffer.wrap(((DecimalData) object).toUnscaledBytes());
			break;
		case ARRAY:
			converter = createArrayConverter((ArrayType) type);
			break;
		case ROW:
			converter = createRowConverter((RowType) type);
			break;
		case MAP:
		case MULTISET:
			converter = createMapConverter(type);
			break;
		case RAW:
		default:
			throw new UnsupportedOperationException("Unsupported type: " + type);
		}

		// wrap into nullable converter
		return (schema, object) -> {
			if (object == null) {
				return null;
			}

			// get actual schema if it is a nullable schema
			Schema actualSchema;
			if (schema.getType() == Schema.Type.UNION) {
				List<Schema> types = schema.getTypes();
				int size = types.size();
				if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
					actualSchema = types.get(0);
				} else if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
					actualSchema = types.get(1);
				} else {
					throw new IllegalArgumentException(
							"The Avro schema is not a nullable type: " + schema.toString());
				}
			} else {
				actualSchema = schema;
			}
			return converter.convert(actualSchema, object);
		};
	}

	private static RowDataToAvroConverter createRowConverter(RowType rowType) {
		final RowDataToAvroConverter[] fieldConverters = rowType.getChildren().stream()
			.map(RowDataToAvroConverters::createConverter)
			.toArray(RowDataToAvroConverter[]::new);
		final LogicalType[] fieldTypes = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.toArray(LogicalType[]::new);
		final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
		for (int i = 0; i < fieldTypes.length; i++) {
			fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
		}
		final int length = rowType.getFieldCount();

		return (schema, object) -> {
			final RowData row = (RowData) object;
			final List<Schema.Field> fields = schema.getFields();
			final GenericRecord record = new GenericData.Record(schema);
			for (int i = 0; i < length; ++i) {
				final Schema.Field schemaField = fields.get(i);
				Object avroObject = fieldConverters[i].convert(
					schemaField.schema(),
					fieldGetters[i].getFieldOrNull(row));
				record.put(i, avroObject);
			}
			return record;
		};
	}

	private static RowDataToAvroConverter createArrayConverter(ArrayType arrayType) {
		LogicalType elementType = arrayType.getElementType();
		final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
		final RowDataToAvroConverter elementConverter = createConverter(arrayType.getElementType());

		return (schema, object) -> {
			final Schema elementSchema = schema.getElementType();
			ArrayData arrayData = (ArrayData) object;
			List<Object> list = new ArrayList<>();
			for (int i = 0; i < arrayData.size(); ++i) {
				list.add(elementConverter.convert(elementSchema, elementGetter.getElementOrNull(arrayData, i)));
			}
			return list;
		};
	}

	private static RowDataToAvroConverter createMapConverter(LogicalType type) {
		LogicalType valueType = extractValueTypeToAvroMap(type);
		final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
		final RowDataToAvroConverter valueConverter = createConverter(valueType);

		return (schema, object) -> {
			final Schema valueSchema = schema.getValueType();
			final MapData mapData = (MapData) object;
			final ArrayData keyArray = mapData.keyArray();
			final ArrayData valueArray = mapData.valueArray();
			final Map<Object, Object> map = new HashMap<>(mapData.size());
			for (int i = 0; i < mapData.size(); ++i) {
				final String key = keyArray.getString(i).toString();
				final Object value = valueConverter.convert(valueSchema, valueGetter.getElementOrNull(valueArray, i));
				map.put(key, value);
			}
			return map;
		};
	}
}
