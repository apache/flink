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

package org.apache.flink.api.java.io;

import org.apache.flink.api.io.avro.example.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.avro.Schema;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.apache.flink.api.java.io.AvroOutputFormat.Codec;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link AvroOutputFormat}.
 */
public class AvroOutputFormatTest {

	@Test
	public void testSetCodec() throws Exception {
		// given
		final AvroOutputFormat<User> outputFormat = new AvroOutputFormat<>(User.class);

		// when
		try {
			outputFormat.setCodec(Codec.SNAPPY);
		} catch (Exception ex) {
			// then
			fail("unexpected exception");
		}
	}

	@Test
	public void testSetCodecError() throws Exception {
		// given
		boolean error = false;
		final AvroOutputFormat<User> outputFormat = new AvroOutputFormat<>(User.class);

		// when
		try {
			outputFormat.setCodec(null);
		} catch (Exception ex) {
			error = true;
		}

		// then
		assertTrue(error);
	}

	@Test
	public void testSerialization() throws Exception {

		serializeAndDeserialize(null, null);
		serializeAndDeserialize(null, User.SCHEMA$);
		for (final Codec codec : Codec.values()) {
			serializeAndDeserialize(codec, null);
			serializeAndDeserialize(codec, User.SCHEMA$);
		}
	}

	private void serializeAndDeserialize(final Codec codec, final Schema schema) throws IOException, ClassNotFoundException {
		// given
		final AvroOutputFormat<User> outputFormat = new AvroOutputFormat<>(User.class);
		if (codec != null) {
			outputFormat.setCodec(codec);
		}
		if (schema != null) {
			outputFormat.setSchema(schema);
		}

		final ByteArrayOutputStream bos = new ByteArrayOutputStream();

		// when
		try (final ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			oos.writeObject(outputFormat);
		}
		try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
			// then
			Object o = ois.readObject();
			assertTrue(o instanceof AvroOutputFormat);
			final AvroOutputFormat<User> restored = (AvroOutputFormat<User>) o;
			final Codec restoredCodec = (Codec) Whitebox.getInternalState(restored, "codec");
			final Schema restoredSchema = (Schema) Whitebox.getInternalState(restored, "userDefinedSchema");

			assertTrue(codec != null ? restoredCodec == codec : restoredCodec == null);
			assertTrue(schema != null ? restoredSchema.equals(schema) : restoredSchema == null);
		}
	}

	@Test
	public void testCompression() throws Exception {
		// given
		final Path outputPath = new Path(File.createTempFile("avro-output-file", "avro").getAbsolutePath());
		final AvroOutputFormat<User> outputFormat = new AvroOutputFormat<>(outputPath, User.class);
		outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

		final Path compressedOutputPath = new Path(File.createTempFile("avro-output-file", "compressed.avro").getAbsolutePath());
		final AvroOutputFormat<User> compressedOutputFormat = new AvroOutputFormat<>(compressedOutputPath, User.class);
		compressedOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		compressedOutputFormat.setCodec(Codec.SNAPPY);

		// when
		output(outputFormat);
		output(compressedOutputFormat);

		// then
		assertTrue(fileSize(outputPath) > fileSize(compressedOutputPath));

		// cleanup
		FileSystem fs = FileSystem.getLocalFileSystem();
		fs.delete(outputPath, false);
		fs.delete(compressedOutputPath, false);
	}

	private long fileSize(Path path) throws IOException {
		return path.getFileSystem().getFileStatus(path).getLen();
	}

	private void output(final AvroOutputFormat<User> outputFormat) throws IOException {
		outputFormat.configure(new Configuration());
		outputFormat.open(1, 1);
		for (int i = 0; i < 100; i++) {
			outputFormat.writeRecord(new User("testUser", 1, "blue"));
		}
		outputFormat.close();
	}
}
