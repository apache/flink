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
package org.apache.flink.streaming.util.serialization;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.BlockingBinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.runtime.AvroSerializer;
import org.apache.flink.api.java.typeutils.runtime.DataInputDecoder;
import org.apache.flink.api.java.typeutils.runtime.DataOutputEncoder;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.util.DataOutputSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Abstract class for implementing serialization schemas for Kafka
 * using Avro serialization library.
 */
@PublicEvolving
public class AvroSerializationSchema<T> implements SerializationSchema<T> {

	private final Class<T> klass;
	private final AvroSerializer<T> avroSerializer;

	public AvroSerializationSchema(Class<T> klass) {
		this.avroSerializer = new AvroSerializer<T>(klass);
		this.klass = klass;
	}

	@Override
	public byte[] serialize(T element) {
		return serialize1(element);
	}

	private byte[] serialize1(T object) {


		// get the reflected schema for packets
		Schema schema = ReflectData.get().getSchema(klass);

		// create a file of packets
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<T> writer = new ReflectDatumWriter<T>(klass);
		final Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

		try {
			writer.write(object, encoder);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return out.toByteArray();
	}

	private static final class TestOutputView extends DataOutputStream implements DataOutputView {

		public TestOutputView() {
			super(new ByteArrayOutputStream(4096));
		}

		public TestInputView getInputView() {
			ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
			return new TestInputView(baos.toByteArray());
		}

		@Override
		public void skipBytesToWrite(int numBytes) throws IOException {
			for (int i = 0; i < numBytes; i++) {
				write(0);
			}
		}

		@Override
		public void write(DataInputView source, int numBytes) throws IOException {
			byte[] buffer = new byte[numBytes];
			source.readFully(buffer);
			write(buffer);
		}
	}

	private static final class TestInputView extends DataInputStream implements DataInputView {

		public TestInputView(byte[] data) {
			super(new ByteArrayInputStream(data));
		}

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			while (numBytes > 0) {
				int skipped = skipBytes(numBytes);
				numBytes -= skipped;
			}
		}
	}
}
