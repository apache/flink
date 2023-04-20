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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.generated.Colors;
import org.apache.flink.formats.avro.generated.Fixed2;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.mock.Whitebox;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link AvroOutputFormat}. */
class AvroOutputFormatTest {

    @Test
    void testSetCodec() {
        // given
        final AvroOutputFormat<User> outputFormat = new AvroOutputFormat<>(User.class);

        // when
        try {
            outputFormat.setCodec(AvroOutputFormat.Codec.SNAPPY);
        } catch (Exception ex) {
            // then
            fail("unexpected exception");
        }
    }

    @Test
    void testSetCodecError() {
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
        assertThat(error).isTrue();
    }

    @Test
    void testSerialization() throws Exception {

        serializeAndDeserialize(null, null);
        serializeAndDeserialize(null, User.SCHEMA$);
        for (final AvroOutputFormat.Codec codec : AvroOutputFormat.Codec.values()) {
            serializeAndDeserialize(codec, null);
            serializeAndDeserialize(codec, User.SCHEMA$);
        }
    }

    private void serializeAndDeserialize(final AvroOutputFormat.Codec codec, final Schema schema)
            throws IOException, ClassNotFoundException {
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
        try (final ObjectInputStream ois =
                new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            // then
            Object o = ois.readObject();
            assertThat(o).isInstanceOf(AvroOutputFormat.class);
            @SuppressWarnings("unchecked")
            final AvroOutputFormat<User> restored = (AvroOutputFormat<User>) o;
            final AvroOutputFormat.Codec restoredCodec =
                    (AvroOutputFormat.Codec) Whitebox.getInternalState(restored, "codec");
            final Schema restoredSchema =
                    (Schema) Whitebox.getInternalState(restored, "userDefinedSchema");

            assertThat(codec).isSameAs(restoredCodec);
            assertThat(schema).isEqualTo(restoredSchema);
        }
    }

    @Test
    void testCompression() throws Exception {
        // given
        final Path outputPath =
                new Path(File.createTempFile("avro-output-file", "avro").getAbsolutePath());
        final AvroOutputFormat<User> outputFormat = new AvroOutputFormat<>(outputPath, User.class);
        outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        final Path compressedOutputPath =
                new Path(
                        File.createTempFile("avro-output-file", "compressed.avro")
                                .getAbsolutePath());
        final AvroOutputFormat<User> compressedOutputFormat =
                new AvroOutputFormat<>(compressedOutputPath, User.class);
        compressedOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        compressedOutputFormat.setCodec(AvroOutputFormat.Codec.SNAPPY);

        // when
        output(outputFormat);
        output(compressedOutputFormat);

        // then
        assertThat(fileSize(outputPath)).isGreaterThan(fileSize(compressedOutputPath));

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
            User user = new User();
            user.setName("testUser");
            user.setFavoriteNumber(1);
            user.setFavoriteColor("blue");
            user.setTypeBoolTest(true);
            user.setTypeArrayString(Collections.emptyList());
            user.setTypeArrayBoolean(Collections.emptyList());
            user.setTypeEnum(Colors.BLUE);
            user.setTypeMap(Collections.emptyMap());
            user.setTypeBytes(ByteBuffer.allocate(10));
            user.setTypeDate(LocalDate.parse("2014-03-01"));
            user.setTypeTimeMillis(LocalTime.parse("12:12:12"));
            user.setTypeTimeMicros(LocalTime.ofSecondOfDay(0).plus(123456L, ChronoUnit.MICROS));
            user.setTypeTimestampMillis(Instant.parse("2014-03-01T12:12:12.321Z"));
            user.setTypeTimestampMicros(Instant.ofEpochSecond(0).plus(123456L, ChronoUnit.MICROS));

            // 20.00
            user.setTypeDecimalBytes(
                    ByteBuffer.wrap(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()));
            // 20.00
            user.setTypeDecimalFixed(
                    new Fixed2(BigDecimal.valueOf(2000, 2).unscaledValue().toByteArray()));

            outputFormat.writeRecord(user);
        }
        outputFormat.close();
    }

    @Test
    void testGenericRecord() throws IOException {
        final Path outputPath =
                new Path(File.createTempFile("avro-output-file", "generic.avro").getAbsolutePath());
        final AvroOutputFormat<GenericRecord> outputFormat =
                new AvroOutputFormat<>(outputPath, GenericRecord.class);
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\", \"name\":\"user\", \"fields\": [{\"name\":\"user_name\", \"type\":\"string\"}, {\"name\":\"favorite_number\", \"type\":\"int\"}, {\"name\":\"favorite_color\", \"type\":\"string\"}]}");
        outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        outputFormat.setSchema(schema);
        output(outputFormat, schema);

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(new File(outputPath.getPath()), reader);

        while (dataFileReader.hasNext()) {
            GenericRecord record = dataFileReader.next();
            assertThat(record.get("user_name").toString()).isEqualTo("testUser");
            assertThat(record.get("favorite_number")).isEqualTo(1);
            assertThat(record.get("favorite_color").toString()).isEqualTo("blue");
        }

        // cleanup
        FileSystem fs = FileSystem.getLocalFileSystem();
        fs.delete(outputPath, false);
    }

    private void output(final AvroOutputFormat<GenericRecord> outputFormat, Schema schema)
            throws IOException {
        outputFormat.configure(new Configuration());
        outputFormat.open(1, 1);
        for (int i = 0; i < 100; i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("user_name", "testUser");
            record.put("favorite_number", 1);
            record.put("favorite_color", "blue");
            outputFormat.writeRecord(record);
        }
        outputFormat.close();
    }
}
