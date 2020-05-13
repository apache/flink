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
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.avro.LogicalTypes;
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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

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

	/**
	 * Used for time conversions from SQL types.
	 */
	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	/**
	 * Logical type describing the input type.
	 */
	private RowType rowType;

	/**
	 * Avro serialization schema.
	 */
	private transient Schema schema;

	/**
	 * Writer to serialize Avro record into a byte array.
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
		this.schema = AvroSchemaConverter.convertToSchema(rowType);
		this.datumWriter = new SpecificDatumWriter<>(schema);
		this.arrayOutputStream = new ByteArrayOutputStream();
		this.encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
		this.rowType = Preconditions.checkNotNull(rowType, "RowType cannot be null.");
	}

	@Override
	public byte[] serialize(RowData row) {
		try {
			// convert to record
			final GenericRecord record = convertRowDataToAvroRecord(schema, row, rowType);
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

	// --------------------------------------------------------------------------------------------

	private GenericRecord convertRowDataToAvroRecord(Schema schema, RowData row, RowType rowType) {
		final List<Schema.Field> fields = schema.getFields();
		final int length = fields.size();
		final GenericRecord record = new GenericData.Record(schema);
		final List<RowType.RowField> rowFields = rowType.getFields();
		for (int i = 0; i < length; i++) {
			final Schema.Field field = fields.get(i);
			final LogicalType fieldType = rowFields.get(i).getType();
			record.put(i, convertFlinkType(
				field.schema(),
				RowData.get(row, i, fieldType),
				fieldType));
		}
		return record;
	}

	private Object convertFlinkType(Schema schema, Object object, LogicalType logicalType) {
		if (object == null) {
			return null;
		}
		switch (schema.getType()) {
			case RECORD:
				if (object instanceof RowData) {
					return convertRowDataToAvroRecord(schema, (RowData) object, (RowType) logicalType);
				}
				throw new IllegalStateException("Row expected but was: " + object.getClass());
			case ENUM:
				return new GenericData.EnumSymbol(schema, object.toString());
			case ARRAY:
				final Schema elementSchema = schema.getElementType();
				final ArrayData array = (ArrayData) object;
				final GenericData.Array<Object> convertedArray = new GenericData.Array<>(array.size(), schema);
				final LogicalType elementType = ((ArrayType) logicalType).getElementType();
				for (int i = 0; i < array.size(); ++i) {
					final Object element = ArrayData.get(array, i, elementType);
					convertedArray.add(convertFlinkType(elementSchema, element, elementType));
				}
				return convertedArray;
			case MAP:
				final MapData map = (MapData) object;
				final Map<Utf8, Object> convertedMap = new HashMap<>();
				final ArrayData keyArray = map.keyArray();
				final ArrayData valueArray = map.valueArray();
				final MapType mapType = (MapType) logicalType;
				final LogicalType keyType = mapType.getKeyType();
				if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
					throw new UnsupportedOperationException(
						"Avro format doesn't support non-string as key type of map. " +
							"The map type is: " + mapType.asSummaryString());
				}
				final LogicalType valueType = mapType.getValueType();
				for (int i = 0; i < map.size(); ++i) {
					final String key = keyArray.getString(i).toString(); // key must be string
					final Object value = ArrayData.get(valueArray, i, valueType);
					convertedMap.put(new Utf8(key), convertFlinkType(
						schema.getValueType(),
						value,
						valueType));
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
					throw new FlinkRuntimeException("Avro union is not supported.");
				}
				return convertFlinkType(actualSchema, object, logicalType);
			case FIXED:
				// check for decimal type
				if (LogicalTypeChecks.hasRoot(logicalType, LogicalTypeRoot.DECIMAL)) {
					return new GenericData.Fixed(schema, convertFromDecimal(schema, (DecimalData) object));
				}
				return new GenericData.Fixed(schema, (byte[]) object);
			case STRING:
				return new Utf8(object.toString());
			case BYTES:
				// check for decimal type
				if (LogicalTypeChecks.hasRoot(logicalType, LogicalTypeRoot.DECIMAL)) {
					return ByteBuffer.wrap(convertFromDecimal(schema, (DecimalData) object));
				}
				return ByteBuffer.wrap((byte[]) object);
			case LONG:
				// check for timestamp type
				if (LogicalTypeChecks.hasRoot(logicalType, LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
					return convertFromTimestamp(schema, (TimestampData) object);
				}
				return object;
			case INT:
			case FLOAT:
			case DOUBLE:
			case BOOLEAN:
				return object;
		}
		throw new RuntimeException("Unsupported Avro type:" + schema);
	}

	private byte[] convertFromDecimal(Schema schema, DecimalData decimal) {
		final org.apache.avro.LogicalType logicalType = schema.getLogicalType();
		if (logicalType instanceof LogicalTypes.Decimal) {
			return decimal.toUnscaledBytes();
		} else {
			throw new RuntimeException("Unsupported decimal type.");
		}
	}

	private long convertFromTimestamp(Schema schema, TimestampData timestamp) {
		final org.apache.avro.LogicalType logicalType = schema.getLogicalType();
		if (logicalType == LogicalTypes.timestampMillis()) {
			final long time = timestamp.getMillisecond();
			return time + (long) LOCAL_TZ.getOffset(time);
		} else {
			throw new RuntimeException("Unsupported timestamp type.");
		}
	}

	private void writeObject(ObjectOutputStream outputStream) throws IOException {
		outputStream.writeObject(rowType);
	}

	private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
		rowType = (RowType) inputStream.readObject();
		schema = AvroSchemaConverter.convertToSchema(rowType);

		datumWriter = new SpecificDatumWriter<>(schema);
		arrayOutputStream = new ByteArrayOutputStream();
		encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
	}
}
