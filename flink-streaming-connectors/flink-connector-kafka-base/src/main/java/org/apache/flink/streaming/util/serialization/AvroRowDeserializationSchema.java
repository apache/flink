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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.flink.api.table.Row;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Deserialization schema from Avro to {@link Row}.
 *
 * <p>Deserializes the <code>byte[]</code> messages in Avro format and reads
 * the specified fields.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 */
public class AvroRowDeserializationSchema extends AbstractDeserializationSchema<Row> {

	/** Converts GenericRecord into a Row instance */
	private final GenericRecordToRowConverter converter;
	/** Avro deserialization schema */
	private final Schema schema;
	/** Reader that deserializes byte array into a record */
	private final DatumReader<GenericRecord> datumReader;
	/** Record to deserialize byte array to */
	private final GenericRecord record;
	/** Input stream to read message from */
	private final MutableByteArrayInputStream inputStream;
	/** Avro decoder that decodes binary data */
	private final Decoder decoder;

	/**
	 * Creates a Avro deserializtion schema for the given type classes.
	 *
	 * @param converter converter for transforming GenericRecord into a Row instance
	 * @param schema Avro schema used to deserialize Row record
	 */
	public AvroRowDeserializationSchema(GenericRecordToRowConverter converter, Schema schema) {
		this.converter = converter;
		this.schema = schema;
		this.datumReader = new ReflectDatumReader<>(schema);
		this.record = new GenericData.Record(schema);
		this.inputStream = new MutableByteArrayInputStream();
		this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		readRecord(message);
		return converter.convert(record);
	}

	private void readRecord(byte[] message) throws IOException {
		inputStream.setBuffer(message);
		datumReader.read(record, decoder);
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
