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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.joda.time.DateTime;

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
 * Deserialization schema from Avro bytes to {@link RowData}.
 *
 * <p>Deserializes the <code>byte[]</code> messages into (nested) Flink RowData. It converts Avro types
 * into types that are compatible with Flink's Table & SQL API.
 *
 * <p>Projects with Avro records containing logical date/time types need to add a JodaTime
 * dependency.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime
 * class {@link AvroRowDataSerializationSchema} and schema converter {@link AvroSchemaConverter}.
 */
@PublicEvolving
public class AvroRowDataDeserializationSchema extends AbstractDeserializationSchema<RowData> {

	/**
	 * Used for time conversions into SQL types.
	 */
	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	/**
	 * Logical type describing the result type.
	 */
	private RowType rowType;

	/**
	 * Type information describing the result type.
	 */
	private TypeInformation<RowData> typeInfo;

	/**
	 * Avro serialization schema.
	 */
	private transient Schema schema;

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
	 * Creates a Avro deserialization schema for the given logical type.
	 *
	 * @param rowType The logical type used to deserialize the data.
	 * @param typeInfo The TypeInformation to be used by {@link AvroRowDataDeserializationSchema#getProducedType()}.
	 */
	public AvroRowDataDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> typeInfo) {
		this.rowType = rowType;
		this.typeInfo = typeInfo;
		this.schema = AvroSchemaConverter.convertToSchema(rowType);
		this.record = new GenericData.Record(schema);
		this.datumReader = new SpecificDatumReader<>(schema);
		this.inputStream = new MutableByteArrayInputStream();
		this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		try {
			inputStream.setBuffer(message);
			record = datumReader.read(record, decoder);
			return convertAvroRecordToRowData(schema, rowType, record);
		} catch (Exception e) {
			throw new IOException("Failed to deserialize Avro record.", e);
		}
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
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
		final AvroRowDataDeserializationSchema that = (AvroRowDataDeserializationSchema) o;
		return Objects.equals(rowType, that.rowType) &&
			Objects.equals(typeInfo, that.typeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rowType, typeInfo);
	}

	// --------------------------------------------------------------------------------------------

	private RowData convertAvroRecordToRowData(Schema schema, RowType rowType, IndexedRecord record) {
		final List<Schema.Field> fields = schema.getFields();
		final int length = fields.size();
		final GenericRowData row = new GenericRowData(length);
		for (int i = 0; i < length; i++) {
			final Schema.Field field = fields.get(i);
			row.setField(i, convertAvroType(field.schema(), rowType.getTypeAt(i), record.get(i)));
		}
		return row;
	}

	private Object convertAvroType(Schema schema, LogicalType logicalType, Object object) {
		if (object == null) {
			return null;
		}
		switch (schema.getType()) {
			case RECORD:
				if (object instanceof IndexedRecord) {
					return convertAvroRecordToRowData(schema, (RowType) logicalType, (IndexedRecord) object);
				}
				throw new IllegalStateException("IndexedRecord expected but was: " + object.getClass());
			case ENUM:
			case STRING:
				return StringData.fromString(object.toString());
			case ARRAY:
					final LogicalType elementType = ((ArrayType) logicalType).getElementType();
					return convertToObjectArray(schema.getElementType(), elementType, object);
			case MAP:
				final Map<?, ?> mapData = (Map<?, ?>) object;
				final Map<StringData, Object> convertedMap = new HashMap<>();

				for (Map.Entry<?, ?> entry : mapData.entrySet()) {
					convertedMap.put(
						StringData.fromString(entry.getKey().toString()), // key must be String.
						convertAvroType(
							schema.getValueType(),
							((MapType) logicalType).getValueType(),
							entry.getValue()));
				}
				return new GenericMapData(convertedMap);
			case UNION:
				final List<Schema> types = schema.getTypes();
				final int size = types.size();
				if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
					return convertAvroType(types.get(1), logicalType, object);
				} else if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
					return convertAvroType(types.get(0), logicalType, object);
				} else if (size == 1) {
					return convertAvroType(types.get(0), logicalType, object);
				} else {
					throw new FlinkRuntimeException("Avro union is not supported.");
				}
			case FIXED:
				final byte[] fixedBytes = ((GenericFixed) object).bytes();
				if (LogicalTypeChecks.hasRoot(logicalType, LogicalTypeRoot.DECIMAL)) {
					return convertToDecimal(schema, fixedBytes);
				}
				return fixedBytes;
			case BYTES:
				final ByteBuffer byteBuffer = (ByteBuffer) object;
				final byte[] bytes = new byte[byteBuffer.remaining()];
				byteBuffer.get(bytes);
				if (LogicalTypeChecks.hasRoot(logicalType, LogicalTypeRoot.DECIMAL)) {
					return convertToDecimal(schema, bytes);
				}
				return bytes;
			case LONG:
				if (LogicalTypeChecks.hasRoot(logicalType, LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
					return convertToTimestamp(object);
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

	private DecimalData convertToDecimal(Schema schema, byte[] bytes) {
		final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
		return DecimalData.fromUnscaledBytes(bytes, decimalType.getPrecision(), decimalType.getScale());
	}

	private TimestampData convertToTimestamp(Object object) {
		final long millis;
		if (object instanceof Long) {
			millis = (Long) object;
		} else {
			// use 'provided' Joda time
			final DateTime value = (DateTime) object;
			millis = value.toDate().getTime();
		}
		return TimestampData.fromEpochMillis(millis - LOCAL_TZ.getOffset(millis));
	}

	private ArrayData convertToObjectArray(Schema elementSchema, LogicalType elementType, Object object) {
		final List<?> list = (List<?>) object;
		final Object[] convertedArray = new Object[list.size()];
		for (int i = 0; i < list.size(); i++) {
			convertedArray[i] = convertAvroType(elementSchema, elementType, list.get(i));
		}
		return new GenericArrayData(convertedArray);
	}

	private void writeObject(ObjectOutputStream outputStream) throws IOException {
		outputStream.writeObject(rowType);
		outputStream.writeObject(typeInfo);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
		this.rowType = (RowType) inputStream.readObject();
		this.typeInfo = (TypeInformation<RowData>) inputStream.readObject();
		this.schema = AvroSchemaConverter.convertToSchema(rowType);
		this.record = new GenericData.Record(schema);
		this.datumReader = new SpecificDatumReader<>(schema);
		this.inputStream = new MutableByteArrayInputStream();
		this.decoder = DecoderFactory.get().binaryDecoder(this.inputStream, null);
	}
}
