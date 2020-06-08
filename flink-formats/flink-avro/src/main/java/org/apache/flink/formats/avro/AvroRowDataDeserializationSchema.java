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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.extractValueTypeToAvroMap;

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
public class AvroRowDataDeserializationSchema implements DeserializationSchema<RowData> {

	private static final long serialVersionUID = 1L;

	/**
	 * Used for converting Date type.
	 */
	private static final int MILLIS_PER_DAY = 86400_000;

	/**
	 * Logical type describing the result type.
	 */
	private final RowType rowType;

	/**
	 * Type information describing the result type.
	 */
	private final TypeInformation<RowData> typeInfo;

	/**
	 * Runtime instance that performs the actual work.
	 */
	private final DeserializationRuntimeConverter runtimeConverter;

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
	 * @param rowType  The logical type used to deserialize the data.
	 * @param typeInfo The TypeInformation to be used by {@link AvroRowDataDeserializationSchema#getProducedType()}.
	 */
	public AvroRowDataDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> typeInfo) {
		this.rowType = rowType;
		this.typeInfo = typeInfo;
		this.runtimeConverter = createRowConverter(rowType);
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		final Schema schema = AvroSchemaConverter.convertToSchema(rowType);
		this.record = new GenericData.Record(schema);
		this.datumReader = new SpecificDatumReader<>(schema);
		this.inputStream = new MutableByteArrayInputStream();
		this.decoder = DecoderFactory.get().binaryDecoder(this.inputStream, null);
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		try {
			inputStream.setBuffer(message);
			record = datumReader.read(record, decoder);
			return (RowData) runtimeConverter.convert(record);
		} catch (Exception e) {
			throw new IOException("Failed to deserialize Avro record.", e);
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
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

	// -------------------------------------------------------------------------------------
	// Runtime Converters
	// -------------------------------------------------------------------------------------

	/**
	 * Runtime converter that converts Avro data structures into objects of Flink Table & SQL
	 * internal data structures.
	 */
	@FunctionalInterface
	interface DeserializationRuntimeConverter extends Serializable {
		Object convert(Object object);
	}

	static DeserializationRuntimeConverter createRowConverter(RowType rowType) {
		final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(AvroRowDataDeserializationSchema::createNullableConverter)
			.toArray(DeserializationRuntimeConverter[]::new);
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
	private static DeserializationRuntimeConverter createNullableConverter(LogicalType type) {
		final DeserializationRuntimeConverter converter = createConverter(type);
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
	private static DeserializationRuntimeConverter createConverter(LogicalType type) {
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
				return AvroRowDataDeserializationSchema::convertToDate;
			case TIME_WITHOUT_TIME_ZONE:
				return AvroRowDataDeserializationSchema::convertToTime;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return AvroRowDataDeserializationSchema::convertToTimestamp;
			case CHAR:
			case VARCHAR:
				return avroObject -> StringData.fromString(avroObject.toString());
			case BINARY:
			case VARBINARY:
				return AvroRowDataDeserializationSchema::convertToBytes;
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

	private static DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
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

	private static DeserializationRuntimeConverter createArrayConverter(ArrayType arrayType) {
		final DeserializationRuntimeConverter elementConverter = createNullableConverter(arrayType.getElementType());
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

	private static DeserializationRuntimeConverter createMapConverter(LogicalType type) {
		final DeserializationRuntimeConverter keyConverter = createConverter(
				DataTypes.STRING().getLogicalType());
		final DeserializationRuntimeConverter valueConverter = createConverter(
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
