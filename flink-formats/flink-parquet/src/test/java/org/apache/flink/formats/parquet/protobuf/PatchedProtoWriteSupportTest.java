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

package org.apache.flink.formats.parquet.protobuf;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.proto.ProtoParquetReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.formats.parquet.protobuf.SimpleRecord.SimpleProtoRecord;
import static org.apache.flink.formats.parquet.protobuf.TestProto2.TestProto2Record;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PatchedProtoWriteSupport} to verify protobuf 4.x compatibility.
 *
 * <p>This test validates that the patched string-based syntax detection correctly handles both
 * proto2 and proto3 messages when using protobuf 4.x, where the enum-based Syntax API was removed.
 */
class PatchedProtoWriteSupportTest {

    @TempDir File tempDir;

    /**
     * Tests that proto3 messages can be written and read correctly with the patched write support.
     */
    @Test
    void testProto3SyntaxDetection() throws IOException {
        File outputFile = new File(tempDir, "proto3_test.parquet");
        Path path = new Path(outputFile.toURI());

        // Create a proto3 message
        SimpleProtoRecord record =
                SimpleProtoRecord.newBuilder()
                        .setFoo("test_foo")
                        .setBar("test_bar")
                        .setNum(42)
                        .build();

        // Write using PatchedProtoWriteSupport directly
        try (ParquetWriter<SimpleProtoRecord> writer =
                new ParquetWriter<>(
                        path,
                        new PatchedProtoWriteSupport<>(SimpleProtoRecord.class),
                        CompressionCodecName.SNAPPY,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE)) {
            writer.write(record);
        }

        // Read back and verify
        try (ParquetReader<SimpleProtoRecord.Builder> reader =
                ProtoParquetReader.<SimpleProtoRecord.Builder>builder(path).build()) {
            SimpleProtoRecord.Builder readRecord = reader.read();
            assertThat(readRecord).isNotNull();
            assertThat(readRecord.build()).isEqualTo(record);
        }
    }

    /**
     * Tests that proto2 messages can be written and read correctly with the patched write support.
     */
    @Test
    void testProto2SyntaxDetection() throws IOException {
        File outputFile = new File(tempDir, "proto2_test.parquet");
        Path path = new Path(outputFile.toURI());

        // Create a proto2 message with only some fields set
        TestProto2Record record =
                TestProto2Record.newBuilder().setName("test_name").setValue(123).build();

        // Write using PatchedProtoWriteSupport directly
        try (ParquetWriter<TestProto2Record> writer =
                new ParquetWriter<>(
                        path,
                        new PatchedProtoWriteSupport<>(TestProto2Record.class),
                        CompressionCodecName.SNAPPY,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE)) {
            writer.write(record);
        }

        // Read back and verify
        try (ParquetReader<TestProto2Record.Builder> reader =
                ProtoParquetReader.<TestProto2Record.Builder>builder(path).build()) {
            TestProto2Record.Builder readRecord = reader.read();
            assertThat(readRecord).isNotNull();
            TestProto2Record result = readRecord.build();
            assertThat(result.getName()).isEqualTo("test_name");
            assertThat(result.getValue()).isEqualTo(123);
            // flag field was not set, should be default
            assertThat(result.hasFlag()).isFalse();
        }
    }

    /**
     * Tests that proto3 messages with default values are handled correctly.
     *
     * <p>In proto3, all fields are written including those with default values.
     */
    @Test
    void testProto3WithDefaults() throws IOException {
        File outputFile = new File(tempDir, "proto3_defaults.parquet");
        Path path = new Path(outputFile.toURI());

        // Create a proto3 message with default values
        SimpleProtoRecord record =
                SimpleProtoRecord.newBuilder().setFoo("").setBar("").setNum(0).build();

        // Write using PatchedProtoWriteSupport
        try (ParquetWriter<SimpleProtoRecord> writer =
                new ParquetWriter<>(
                        path,
                        new PatchedProtoWriteSupport<>(SimpleProtoRecord.class),
                        CompressionCodecName.SNAPPY,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE)) {
            writer.write(record);
        }

        // Read back and verify - proto3 should read all fields even if default
        try (ParquetReader<SimpleProtoRecord.Builder> reader =
                ProtoParquetReader.<SimpleProtoRecord.Builder>builder(path).build()) {
            SimpleProtoRecord.Builder readRecord = reader.read();
            assertThat(readRecord).isNotNull();
            assertThat(readRecord.build()).isEqualTo(record);
        }
    }

    /**
     * Tests that proto2 only writes fields that have been explicitly set.
     *
     * <p>In proto2, unset optional fields should not be written to the file.
     */
    @Test
    void testProto2OnlyWritesSetFields() throws IOException {
        File outputFile = new File(tempDir, "proto2_partial.parquet");
        Path path = new Path(outputFile.toURI());

        // Create a proto2 message with only one field set
        TestProto2Record record = TestProto2Record.newBuilder().setName("only_name").build();

        // Write using PatchedProtoWriteSupport
        try (ParquetWriter<TestProto2Record> writer =
                new ParquetWriter<>(
                        path,
                        new PatchedProtoWriteSupport<>(TestProto2Record.class),
                        CompressionCodecName.SNAPPY,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE)) {
            writer.write(record);
        }

        // Read back and verify
        try (ParquetReader<TestProto2Record.Builder> reader =
                ProtoParquetReader.<TestProto2Record.Builder>builder(path).build()) {
            TestProto2Record.Builder readRecord = reader.read();
            assertThat(readRecord).isNotNull();
            TestProto2Record result = readRecord.build();
            assertThat(result.getName()).isEqualTo("only_name");
            // value and flag were not set
            assertThat(result.hasValue()).isFalse();
            assertThat(result.hasFlag()).isFalse();
        }
    }

    /**
     * Integration test using ParquetProtoWriters (Flink's production API).
     *
     * <p>This validates that PatchedProtoWriteSupport works correctly when used through Flink's
     * ParquetProtoWriters factory, which is the actual production code path.
     */
    @Test
    void testViaParquetProtoWritersForProto3() throws IOException {
        File outputFile = new File(tempDir, "proto3_via_writers.parquet");
        Path hadoopPath = new Path(outputFile.toURI());
        OutputFile outputFileObj =
                HadoopOutputFile.fromPath(hadoopPath, new org.apache.hadoop.conf.Configuration());

        // Create a proto3 message
        SimpleProtoRecord record =
                SimpleProtoRecord.newBuilder()
                        .setFoo("via_writers")
                        .setBar("test")
                        .setNum(99)
                        .build();

        // Write using ParquetProtoWriters (production code path)
        try (ParquetWriter<SimpleProtoRecord> writer =
                new ParquetProtoWriters.ParquetProtoWriterBuilder<>(
                                outputFileObj, SimpleProtoRecord.class)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .build()) {
            writer.write(record);
        }

        // Read back and verify
        try (ParquetReader<SimpleProtoRecord.Builder> reader =
                ProtoParquetReader.<SimpleProtoRecord.Builder>builder(hadoopPath).build()) {
            SimpleProtoRecord.Builder readRecord = reader.read();
            assertThat(readRecord).isNotNull();
            assertThat(readRecord.build()).isEqualTo(record);
        }
    }

    /**
     * Integration test using ParquetProtoWriters for proto2 messages.
     *
     * <p>Verifies that proto2 syntax detection works correctly through the production API.
     */
    @Test
    void testViaParquetProtoWritersForProto2() throws IOException {
        File outputFile = new File(tempDir, "proto2_via_writers.parquet");
        Path hadoopPath = new Path(outputFile.toURI());
        OutputFile outputFileObj =
                HadoopOutputFile.fromPath(hadoopPath, new org.apache.hadoop.conf.Configuration());

        // Create a proto2 message with partial fields
        TestProto2Record record =
                TestProto2Record.newBuilder().setName("proto2_writer").setFlag(true).build();

        // Write using ParquetProtoWriters (production code path)
        try (ParquetWriter<TestProto2Record> writer =
                new ParquetProtoWriters.ParquetProtoWriterBuilder<>(
                                outputFileObj, TestProto2Record.class)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .build()) {
            writer.write(record);
        }

        // Read back and verify
        try (ParquetReader<TestProto2Record.Builder> reader =
                ProtoParquetReader.<TestProto2Record.Builder>builder(hadoopPath).build()) {
            TestProto2Record.Builder readRecord = reader.read();
            assertThat(readRecord).isNotNull();
            TestProto2Record result = readRecord.build();
            assertThat(result.getName()).isEqualTo("proto2_writer");
            assertThat(result.getFlag()).isTrue();
            // value was not set
            assertThat(result.hasValue()).isFalse();
        }
    }
}
