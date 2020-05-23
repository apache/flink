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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.extractValueTypeToAvroMap;

/**
 * Serialization schema that serializes {@link RowData} into Avro bytes.
 *
 * <p>Serializes objects that are represented in (nested) Flink RowData. It support types that
 * are compatible with Flink's Table & SQL API.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime
 * class {@link AvroRowDataDeserializationSchema} and schema converter {@link AvroSchemaConverter}.
 */
public class AvroRowDataSerializationSchema implements SerializationSchema<RowData> {

	private static final long serialVersionUID = 1L;

	/**
	 * Logical type describing the input type.
	 */
	private final RowType rowType;

	/**
	 * Runtime instance that performs the actual work.
	 */
	private final SerializationRuntimeConverter runtimeConverter;

	/**
	 * Writer to serialize Avro record into a Avro bytes.
	 */
	private transient DatumWriter<IndexedRecord> datumWriter;

	/**
	 * Output stream to serialize records into byte array.
	 */
	private transient ByteArrayOutputStream arrayOutputStream;

	/**
	 * Low-level class for serialization of Avro values.
	 */
	private transient Encoder encoder;

	/**
	 * Creates an Avro serialization schema for the given specific record class.
	 */
	public AvroRowDataSerializationSchema(RowType rowType) {
		this.rowType = Preconditions.checkNotNull(rowType, "RowType cannot be null.");
		this.runtimeConverter = createRowConverter(rowType);
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		final Schema schema = AvroSchemaConverter.convertToSchema(rowType);
		datumWriter = new SpecificDatumWriter<>(schema);
		arrayOutputStream = new ByteArrayOutputStream();
		encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
	}

	@Override
	public byte[] serialize(RowData row) {
		try {
			// convert to record
			final GenericRecord record = (GenericRecord) runtimeConverter.convert(row);
			arrayOutputStream.reset();
			datumWriter.write(record, encoder);
			encoder.flush();
			return arrayOutputStream.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException("Failed to serialize row.", e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final AvroRowDataSerializationSchema that = (AvroRowDataSerializationSchema) o;
		return Objects.equals(rowType, that.rowType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rowType);
	}

	// --------------------------------------------------------------------------------
	// Runtime Converters
	// --------------------------------------------------------------------------------

	/**
	 * Runtime converter that converts objects of Flink Table & SQL internal data structures
	 * to corresponding Avro data structures.
	 */
	interface SerializationRuntimeConverter extends Serializable {
		Object convert(Object object);
	}

	static SerializationRuntimeConverter createRowConverter(RowType rowType) {
		final SerializationRuntimeConverter[] fieldConverters = rowType.getChildren().stream()
			.map(AvroRowDataSerializationSchema::createConverter)
			.toArray(SerializationRuntimeConverter[]::new);
		final Schema schema = AvroSchemaConverter.convertToSchema(rowType);
		final LogicalType[] fieldTypes = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.toArray(LogicalType[]::new);
		final int length = rowType.getFieldCount();

		return object -> {
			final RowData row = (RowData) object;
			final GenericRecord record = new GenericData.Record(schema);
			for (int i = 0; i < length; ++i) {
				record.put(i, fieldConverters[i].convert(RowData.get(row, i, fieldTypes[i])));
			}
			return record;
		};
	}

	private static SerializationRuntimeConverter createConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return object -> null;
			case BOOLEAN: // boolean
			case INTEGER: // int
			case INTERVAL_YEAR_MONTH: // long
			case BIGINT: // long
			case INTERVAL_DAY_TIME: // long
			case FLOAT: // float
			case DOUBLE: // double
			case TIME_WITHOUT_TIME_ZONE: // int
			case DATE: // int
				return avroObject -> avroObject;
			case CHAR:
			case VARCHAR:
				return object -> new Utf8(object.toString());
			case BINARY:
			case VARBINARY:
				return object -> ByteBuffer.wrap((byte[]) object);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return object -> ((TimestampData) object).toTimestamp().getTime();
			case DECIMAL:
				return object -> ByteBuffer.wrap(((DecimalData) object).toUnscaledBytes());
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

	private static SerializationRuntimeConverter createArrayConverter(ArrayType arrayType) {
		final SerializationRuntimeConverter elementConverter = createConverter(arrayType.getElementType());
		final LogicalType elementType = arrayType.getElementType();

		return object -> {
			ArrayData arrayData = (ArrayData) object;
			List<Object> list = new ArrayList<>();
			for (int i = 0; i < arrayData.size(); ++i) {
				list.add(elementConverter.convert(ArrayData.get(arrayData, i, elementType)));
			}
			return list;
		};
	}

	private static SerializationRuntimeConverter createMapConverter(LogicalType type) {
		LogicalType valueType = extractValueTypeToAvroMap(type);
		final SerializationRuntimeConverter valueConverter = createConverter(valueType);

		return object -> {
			final MapData mapData = (MapData) object;
			final ArrayData keyArray = mapData.keyArray();
			final ArrayData valueArray = mapData.valueArray();
			final Map<Object, Object> map = new HashMap<>(mapData.size());
			for (int i = 0; i < mapData.size(); ++i) {
				final String key = keyArray.getString(i).toString();
				final Object value = valueConverter.convert(ArrayData.get(valueArray, i, valueType));
				map.put(key, value);
			}
			return map;
		};
	}
}
