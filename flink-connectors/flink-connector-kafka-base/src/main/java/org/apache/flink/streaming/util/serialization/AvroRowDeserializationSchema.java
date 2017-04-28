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
package org.apache.flink.streaming.util.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * Deserialization schema from Avro bytes over {@link SpecificRecord} to {@link Row}.
 *
 * Deserializes the <code>byte[]</code> messages into (nested) Flink Rows.
 *
 * {@link Utf8} is converted to regular Java Strings.
 */
public class AvroRowDeserializationSchema extends AbstractDeserializationSchema<Row> {

	/**
	 * Schema for deterministic field order.
	 */
	private final Schema schema;

	/**
	 * Reader that deserializes byte array into a record.
	 */
	private final DatumReader<SpecificRecord> datumReader;

	/**
	 * Input stream to read message from.
	 */
	private final MutableByteArrayInputStream inputStream;

	/**
	 * Avro decoder that decodes binary data
	 */
	private final Decoder decoder;

	/**
	 * Record to deserialize byte array to.
	 */
	private SpecificRecord record;

	/**
	 * Creates a Avro deserialization schema for the given record.
	 *
	 * @param recordClazz Avro record class used to deserialize Avro's record to Flink's row
	 */
	public AvroRowDeserializationSchema(Class<? extends SpecificRecord> recordClazz) {
		Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
		this.schema = SpecificData.get().getSchema(recordClazz);
		this.datumReader = new SpecificDatumReader<>(schema);
		this.record = (SpecificRecord) SpecificData.newInstance(recordClazz, schema);
		this.inputStream = new MutableByteArrayInputStream();
		this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
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

	/**
	 * Converts a (nested) Avro {@link SpecificRecord} into Flink's Row type.
	 * Avro's {@link Utf8} fields are converted into regular Java strings.
	 */
	private static Object convertToRow(Schema schema, Object recordObj) {
		if (recordObj instanceof GenericRecord) {
			// records can be wrapped in a union
			if (schema.getType() == Schema.Type.UNION) {
				final List<Schema> types = schema.getTypes();
				if (types.size() == 2 && types.get(0).getType() == Schema.Type.NULL && types.get(1).getType() == Schema.Type.RECORD) {
					schema = types.get(1);
				}
				else {
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
		} else if (recordObj instanceof Utf8) {
			return recordObj.toString();
		} else {
			return recordObj;
		}
	}

	/**
	 * An extension of the ByteArrayInputStream that allows to change a buffer that should be
	 * read without creating a new ByteArrayInputStream instance. This allows to re-use the same
	 * InputStream instance, copying message to process, and creation of Decoder on every new message.
	 */
	private static final class MutableByteArrayInputStream extends ByteArrayInputStream {
		/**
		 * Create MutableByteArrayInputStream
		 */
		public MutableByteArrayInputStream() {
			super(new byte[0]);
		}

		/**
		 * Set buffer that can be read via the InputStream interface and reset the input stream.
		 * This has the same effect as creating a new ByteArrayInputStream with a new buffer.
		 *
		 * @param buf the new buffer to read.
		 */
		public void setBuffer(byte[] buf) {
			this.buf = buf;
			this.pos = 0;
			this.count = buf.length;
		}
	}
}
