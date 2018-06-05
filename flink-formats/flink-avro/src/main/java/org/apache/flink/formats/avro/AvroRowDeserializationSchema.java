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

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroRecordClassConverter;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deserialization schema from Avro bytes over {@link SpecificRecord} to {@link Row}.
 *
 * <p>Deserializes the <code>byte[]</code> messages into (nested) Flink Rows.
 *
 * {@link Utf8} is converted to regular Java Strings.
 */
public class AvroRowDeserializationSchema extends AbstractDeserializationSchema<Row> {

	/**
	 * Avro record class.
	 */
	private Class<? extends SpecificRecord> recordClazz;

	/**
	 * Schema for deterministic field order.
	 */
	private transient Schema schema;

	/**
	 * Reader that deserializes byte array into a record.
	 */
	private transient DatumReader<SpecificRecord> datumReader;

	/**
	 * Input stream to read message from.
	 */
	private transient MutableByteArrayInputStream inputStream;

	/**
	 * Avro decoder that decodes binary data.
	 */
	private transient Decoder decoder;

	/**
	 * Record to deserialize byte array to.
	 */
	private SpecificRecord record;

	/**
	 * Type information describing the result type.
	 */
	private transient TypeInformation<Row> typeInfo;

	/**
	 * Creates a Avro deserialization schema for the given record.
	 *
	 * @param recordClazz Avro record class used to deserialize Avro's record to Flink's row
	 */
	public AvroRowDeserializationSchema(Class<? extends SpecificRecordBase> recordClazz) {
		Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
		this.recordClazz = recordClazz;
		this.schema = SpecificData.get().getSchema(recordClazz);
		this.datumReader = new SpecificDatumReader<>(schema);
		this.record = (SpecificRecord) SpecificData.newInstance(recordClazz, schema);
		this.inputStream = new MutableByteArrayInputStream();
		this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
		this.typeInfo = AvroRecordClassConverter.convert(recordClazz);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		// read record
		try {
			inputStream.setBuffer(message);
			this.record = datumReader.read(record, decoder);
		} catch (IOException e) {
			throw new RuntimeException("Failed to deserialize Row.", e);
		}

		// convert to row
		final Object row = convertToRow(schema, record);
		return (Row) row;
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.writeObject(recordClazz);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		this.recordClazz = (Class<? extends SpecificRecord>) ois.readObject();
		this.schema = SpecificData.get().getSchema(recordClazz);
		this.datumReader = new SpecificDatumReader<>(schema);
		this.record = (SpecificRecord) SpecificData.newInstance(recordClazz, schema);
		this.inputStream = new MutableByteArrayInputStream();
		this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	/**
	 * Converts a (nested) Avro {@link SpecificRecord} into Flink's Row type.
	 * Avro's {@link Utf8} fields are converted into regular Java strings.
	 * Avro's {@link GenericData.Array} fields are converted into regular Java arrays.
	 */
	private static Object convertToRow(Schema schema, Object recordObj) {
		if (recordObj instanceof GenericRecord) {
			// records can be wrapped in a union
			if (schema.getType() == Schema.Type.UNION) {
				final List<Schema> types = schema.getTypes();
				if (types.size() == 2 && types.get(0).getType() == Schema.Type.NULL && types.get(1).getType() == Schema.Type.RECORD) {
					schema = types.get(1);
				} else {
					throw new RuntimeException("Currently we only support schemas of the following form: UNION[null, RECORD]. Given: " + schema);
				}
			} else if (schema.getType() != Schema.Type.RECORD) {
				throw new RuntimeException("Record type for row type expected. But is: " + schema);
			}
			final List<Schema.Field> fields = schema.getFields();
			final Row row = new Row(fields.size());
			final GenericRecord record = (GenericRecord) recordObj;
			for (int i = 0; i < fields.size(); i++) {
				final Schema.Field field = fields.get(i);
				row.setField(i, convertToRow(field.schema(), record.get(field.pos())));
			}
			return row;
		} else if (schema.getType() == Schema.Type.MAP) {
			final Map mapObj = (Map) recordObj;
			Map<String, Object> retMap = new HashMap<>();
			for (Object key : mapObj.keySet()) {
				retMap.put(key.toString(), convertToRow(schema.getValueType(), mapObj.get(key)));
			}
			return retMap;
		} else if (schema.getType() == Schema.Type.ARRAY) {
			final GenericData.Array arrayObj = (GenericData.Array) recordObj;
			Class elementClass = getClassForType(schema.getElementType().getType());
			Object[] retArray = (Object[]) Array.newInstance(elementClass, arrayObj.size());
			for (int i = 0; i < retArray.length; i++) {
				retArray[i] = convertToRow(schema.getElementType(), arrayObj.get(i));
			}
			return retArray;
		} else if (recordObj instanceof Utf8) {
			return recordObj.toString();
		} else {
			return recordObj;
		}
	}

	private static Class<?> getClassForType(Schema.Type type) {
		switch (type) {
			case STRING:
				return String.class;
			case INT:
				return Integer.class;
			case LONG:
				return Long.class;
			case FLOAT:
				return Float.class;
			case DOUBLE:
				return Double.class;
			case BOOLEAN:
				return Boolean.class;
			case MAP:
				return HashMap.class;
			case RECORD:
				return Row.class;
			default:
				throw new UnsupportedOperationException("Unsupported type " + type);
		}
	}
}
