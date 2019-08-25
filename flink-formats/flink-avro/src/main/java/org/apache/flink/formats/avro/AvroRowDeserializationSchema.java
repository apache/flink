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

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Deserialization schema from Avro bytes to {@link Row}.
 *
 * <p>Deserializes the <code>byte[]</code> messages into (nested) Flink rows. It converts Avro types
 * into types that are compatible with Flink's Table & SQL API.
 *
 * <p>Projects with Avro records containing logical date/time types need to add a JodaTime
 * dependency.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime
 * class {@link AvroRowSerializationSchema} and schema converter {@link AvroSchemaConverter}.
 */
@PublicEvolving
public class AvroRowDeserializationSchema extends AbstractDeserializationSchema<Row> {

	/**
	 * Used for time conversions into SQL types.
	 */
	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	/**
	 * Avro record class for deserialization. Might be null if record class is not available.
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
	 * Type information describing the result type.
	 */
	private transient RowTypeInfo typeInfo;

	/**
	 * Record to deserialize byte array.
	 */
	private transient IndexedRecord record;

	/**
	 * Reader that deserializes byte array into a record.
	 */
	private transient DatumReader<IndexedRecord> datumReader;

	/**
	 * Input stream to read message from.
	 */
	private transient MutableByteArrayInputStream inputStream;

	/**
	 * Avro decoder that decodes binary data.
	 */
	private transient Decoder decoder;

	/**
	 * Creates a Avro deserialization schema for the given specific record class. Having the
	 * concrete Avro record class might improve performance.
	 *
	 * @param recordClazz Avro record class used to deserialize Avro's record to Flink's row
	 */
	public AvroRowDeserializationSchema(Class<? extends SpecificRecord> recordClazz) {
		Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
		this.recordClazz = recordClazz;
		schema = SpecificData.get().getSchema(recordClazz);
		typeInfo = (RowTypeInfo) AvroSchemaConverter.convertToTypeInfo(recordClazz);
		schemaString = schema.toString();
		record = (IndexedRecord) SpecificData.newInstance(recordClazz, schema);
		datumReader = new SpecificDatumReader<>(schema);
		inputStream = new MutableByteArrayInputStream();
		decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
	}

	/**
	 * Creates a Avro deserialization schema for the given Avro schema string.
	 *
	 * @param avroSchemaString Avro schema string to deserialize Avro's record to Flink's row
	 */
	public AvroRowDeserializationSchema(String avroSchemaString) {
		Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
		recordClazz = null;
		final TypeInformation<?> typeInfo = AvroSchemaConverter.convertToTypeInfo(avroSchemaString);
		Preconditions.checkArgument(typeInfo instanceof RowTypeInfo, "Row type information expected.");
		this.typeInfo = (RowTypeInfo) typeInfo;
		schemaString = avroSchemaString;
		schema = new Schema.Parser().parse(avroSchemaString);
		record = new GenericData.Record(schema);
		datumReader = new GenericDatumReader<>(schema);
		inputStream = new MutableByteArrayInputStream();
		decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		try {
			inputStream.setBuffer(message);
			record = datumReader.read(record, decoder);
			return convertAvroRecordToRow(schema, typeInfo, record);
		} catch (Exception e) {
			throw new IOException("Failed to deserialize Avro record.", e);
		}
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final AvroRowDeserializationSchema that = (AvroRowDeserializationSchema) o;
		return Objects.equals(recordClazz, that.recordClazz) &&
			Objects.equals(schemaString, that.schemaString);
	}

	@Override
	public int hashCode() {
		return Objects.hash(recordClazz, schemaString);
	}

	// --------------------------------------------------------------------------------------------

	private Row convertAvroRecordToRow(Schema schema, RowTypeInfo typeInfo, IndexedRecord record) {
		final List<Schema.Field> fields = schema.getFields();
		final TypeInformation<?>[] fieldInfo = typeInfo.getFieldTypes();
		final int length = fields.size();
		final Row row = new Row(length);
		for (int i = 0; i < length; i++) {
			final Schema.Field field = fields.get(i);
			row.setField(i, convertAvroType(field.schema(), fieldInfo[i], record.get(i)));
		}
		return row;
	}

	private Object convertAvroType(Schema schema, TypeInformation<?> info, Object object) {
		// we perform the conversion based on schema information but enriched with pre-computed
		// type information where useful (i.e., for arrays)

		if (object == null) {
			return null;
		}
		switch (schema.getType()) {
			case RECORD:
				if (object instanceof IndexedRecord) {
					return convertAvroRecordToRow(schema, (RowTypeInfo) info, (IndexedRecord) object);
				}
				throw new IllegalStateException("IndexedRecord expected but was: " + object.getClass());
			case ENUM:
			case STRING:
				return object.toString();
			case ARRAY:
				if (info instanceof BasicArrayTypeInfo) {
					final TypeInformation<?> elementInfo = ((BasicArrayTypeInfo<?, ?>) info).getComponentInfo();
					return convertToObjectArray(schema.getElementType(), elementInfo, object);
				} else {
					final TypeInformation<?> elementInfo = ((ObjectArrayTypeInfo<?, ?>) info).getComponentInfo();
					return convertToObjectArray(schema.getElementType(), elementInfo, object);
				}
			case MAP:
				final MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) info;
				final Map<String, Object> convertedMap = new HashMap<>();
				final Map<?, ?> map = (Map<?, ?>) object;
				for (Map.Entry<?, ?> entry : map.entrySet()) {
					convertedMap.put(
						entry.getKey().toString(),
						convertAvroType(schema.getValueType(), mapTypeInfo.getValueTypeInfo(), entry.getValue()));
				}
				return convertedMap;
			case UNION:
				final List<Schema> types = schema.getTypes();
				final int size = types.size();
				final Schema actualSchema;
				if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
					return convertAvroType(types.get(1), info, object);
				} else if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
					return convertAvroType(types.get(0), info, object);
				} else if (size == 1) {
					return convertAvroType(types.get(0), info, object);
				} else {
					// generic type
					return object;
				}
			case FIXED:
				final byte[] fixedBytes = ((GenericFixed) object).bytes();
				if (info == Types.BIG_DEC) {
					return convertToDecimal(schema, fixedBytes);
				}
				return fixedBytes;
			case BYTES:
				final ByteBuffer byteBuffer = (ByteBuffer) object;
				final byte[] bytes = new byte[byteBuffer.remaining()];
				byteBuffer.get(bytes);
				if (info == Types.BIG_DEC) {
					return convertToDecimal(schema, bytes);
				}
				return bytes;
			case INT:
				if (info == Types.SQL_DATE) {
					return convertToDate(object);
				} else if (info == Types.SQL_TIME) {
					return convertToTime(object);
				}
				return object;
			case LONG:
				if (info == Types.SQL_TIMESTAMP) {
					return convertToTimestamp(object);
				}
				return object;
			case FLOAT:
			case DOUBLE:
			case BOOLEAN:
				return object;
		}
		throw new RuntimeException("Unsupported Avro type:" + schema);
	}

	private BigDecimal convertToDecimal(Schema schema, byte[] bytes) {
		final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
		return new BigDecimal(new BigInteger(bytes), decimalType.getScale());
	}

	private Date convertToDate(Object object) {
		final long millis;
		if (object instanceof Integer) {
			final Integer value = (Integer) object;
			// adopted from Apache Calcite
			final long t = (long) value * 86400000L;
			millis = t - (long) LOCAL_TZ.getOffset(t);
		} else {
			// use 'provided' Joda time
			final LocalDate value = (LocalDate) object;
			millis = value.toDate().getTime();
		}
		return new Date(millis);
	}

	private Time convertToTime(Object object) {
		final long millis;
		if (object instanceof Integer) {
			millis = (Integer) object;
		} else {
			// use 'provided' Joda time
			final LocalTime value = (LocalTime) object;
			millis = (long) value.get(DateTimeFieldType.millisOfDay());
		}
		return new Time(millis - LOCAL_TZ.getOffset(millis));
	}

	private Timestamp convertToTimestamp(Object object) {
		final long millis;
		if (object instanceof Long) {
			millis = (Long) object;
		} else {
			// use 'provided' Joda time
			final DateTime value = (DateTime) object;
			millis = value.toDate().getTime();
		}
		return new Timestamp(millis - LOCAL_TZ.getOffset(millis));
	}

	private Object[] convertToObjectArray(Schema elementSchema, TypeInformation<?> elementInfo, Object object) {
		final List<?> list = (List<?>) object;
		final Object[] convertedArray = (Object[]) Array.newInstance(
			elementInfo.getTypeClass(),
			list.size());
		for (int i = 0; i < list.size(); i++) {
			convertedArray[i] = convertAvroType(elementSchema, elementInfo, list.get(i));
		}
		return convertedArray;
	}

	private void writeObject(ObjectOutputStream outputStream) throws IOException {
		outputStream.writeObject(recordClazz);
		outputStream.writeUTF(schemaString);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
		recordClazz = (Class<? extends SpecificRecord>) inputStream.readObject();
		schemaString = inputStream.readUTF();
		typeInfo = (RowTypeInfo) AvroSchemaConverter.<Row>convertToTypeInfo(schemaString);
		schema = new Schema.Parser().parse(schemaString);
		if (recordClazz != null) {
			record = (SpecificRecord) SpecificData.newInstance(recordClazz, schema);
		} else {
			record = new GenericData.Record(schema);
		}
		datumReader = new SpecificDatumReader<>(schema);
		this.inputStream = new MutableByteArrayInputStream();
		decoder = DecoderFactory.get().binaryDecoder(this.inputStream, null);
	}
}
