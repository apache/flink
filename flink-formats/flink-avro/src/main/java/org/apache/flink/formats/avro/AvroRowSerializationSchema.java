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
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Serialization schema that serializes {@link Row} into Avro bytes.
 *
 * <p>Serializes objects that are represented in (nested) Flink rows. It support types that
 * are compatible with Flink's Table & SQL API.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime
 * class {@link AvroRowDeserializationSchema} and schema converter {@link AvroSchemaConverter}.
 */
public class AvroRowSerializationSchema implements SerializationSchema<Row> {

	/**
	 * Used for time conversions from SQL types.
	 */
	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	/**
	 * Avro record class for serialization. Might be null if record class is not available.
	 */
	private Class<? extends SpecificRecord> recordClazz;

	/**
	 * Schema string for deserialization.
	 */
	private String schemaString;

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
	 *
	 * @param recordClazz Avro record class used to serialize Flink's row to Avro's record
	 */
	public AvroRowSerializationSchema(Class<? extends SpecificRecord> recordClazz) {
		Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
		this.recordClazz = recordClazz;
		this.schema = SpecificData.get().getSchema(recordClazz);
		this.schemaString = schema.toString();
		this.datumWriter = new SpecificDatumWriter<>(schema);
		this.arrayOutputStream = new ByteArrayOutputStream();
		this.encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
	}

	/**
	 * Creates an Avro serialization schema for the given Avro schema string.
	 *
	 * @param avroSchemaString Avro schema string used to serialize Flink's row to Avro's record
	 */
	public AvroRowSerializationSchema(String avroSchemaString) {
		Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
		this.recordClazz = null;
		this.schemaString = avroSchemaString;
		try {
			this.schema = new Schema.Parser().parse(avroSchemaString);
		} catch (SchemaParseException e) {
			throw new IllegalArgumentException("Could not parse Avro schema string.", e);
		}
		this.datumWriter = new GenericDatumWriter<>(schema);
		this.arrayOutputStream = new ByteArrayOutputStream();
		this.encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
	}

	@Override
	public byte[] serialize(Row row) {
		try {
			// convert to record
			final GenericRecord record = convertRowToAvroRecord(schema, row);
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
		final AvroRowSerializationSchema that = (AvroRowSerializationSchema) o;
		return Objects.equals(recordClazz, that.recordClazz) && Objects.equals(schemaString, that.schemaString);
	}

	@Override
	public int hashCode() {
		return Objects.hash(recordClazz, schemaString);
	}

	// --------------------------------------------------------------------------------------------

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
				final GenericData.Array<Object> convertedArray = new GenericData.Array<>(array.length, schema);
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
				} else if (object instanceof LocalDate) {
					return convertFromDate(schema, Date.valueOf((LocalDate) object));
				} else if (object instanceof Time) {
					return convertFromTime(schema, (Time) object);
				} else if (object instanceof LocalTime) {
					return convertFromTime(schema, Time.valueOf((LocalTime) object));
				}
				return object;
			case LONG:
				// check for logical type
				if (object instanceof Timestamp) {
					return convertFromTimestamp(schema, (Timestamp) object);
				} else if (object instanceof LocalDateTime) {
					return convertFromTimestamp(schema, Timestamp.valueOf((LocalDateTime) object));
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
			final BigDecimal rescaled = decimal.setScale(decimalType.getScale(), BigDecimal.ROUND_UNNECESSARY);
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

	private void writeObject(ObjectOutputStream outputStream) throws IOException {
		outputStream.writeObject(recordClazz);
		outputStream.writeObject(schemaString); // support for null
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
		recordClazz = (Class<? extends SpecificRecord>) inputStream.readObject();
		schemaString = (String) inputStream.readObject();
		if (recordClazz != null) {
			schema = SpecificData.get().getSchema(recordClazz);
		} else {
			schema = new Schema.Parser().parse(schemaString);
		}
		datumWriter = new SpecificDatumWriter<>(schema);
		arrayOutputStream = new ByteArrayOutputStream();
		encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
	}
}
