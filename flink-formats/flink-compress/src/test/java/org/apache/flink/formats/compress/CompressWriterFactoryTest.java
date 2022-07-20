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

package org.apache.flink.formats.compress;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.compress.extractor.DefaultExtractor;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.UniqueBucketAssigner;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CompressWriterFactory}. */
class CompressWriterFactoryTest {

    @TempDir public static java.nio.file.Path tmpDir;

    private static Configuration confWithCustomCodec;

    @BeforeAll
    static void before() {
        confWithCustomCodec = new Configuration();
        confWithCustomCodec.set(
                "io.compression.codecs",
                "org.apache.flink.formats.compress.CustomCompressionCodec");
    }

    @Test
    void testBzip2CompressByAlias() throws Exception {
        testCompressByName("Bzip2");
    }

    @Test
    void testBzip2CompressByName() throws Exception {
        testCompressByName("Bzip2Codec");
    }

    @Test
    void testGzipCompressByAlias() throws Exception {
        testCompressByName("Gzip");
    }

    @Test
    void testGzipCompressByName() throws Exception {
        testCompressByName("GzipCodec");
    }

    @Test
    void testDeflateCompressByAlias() throws Exception {
        testCompressByName("deflate");
    }

    @Test
    void testDeflateCompressByClassName() throws Exception {
        testCompressByName("org.apache.hadoop.io.compress.DeflateCodec");
    }

    @Test
    void testDefaultCompressByName() throws Exception {
        testCompressByName("DefaultCodec");
    }

    @Test
    void testDefaultCompressByClassName() throws Exception {
        testCompressByName("org.apache.hadoop.io.compress.DefaultCodec");
    }

    @Test
    void testCompressFailureWithUnknownCodec() {
        assertThatThrownBy(() -> testCompressByName("com.bla.bla.UnknownCodec"))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testCustomCompressionCodecByClassName() throws Exception {
        testCompressByName(
                "org.apache.flink.formats.compress.CustomCompressionCodec", confWithCustomCodec);
    }

    @Test
    void testCustomCompressionCodecByAlias() throws Exception {
        testCompressByName("CustomCompressionCodec", confWithCustomCodec);
    }

    @Test
    void testCustomCompressionCodecByName() throws Exception {
        testCompressByName("CustomCompression", confWithCustomCodec);
    }

    private void testCompressByName(String codec) throws Exception {
        testCompressByName(codec, new Configuration());
    }

    private void testCompressByName(String codec, Configuration conf) throws Exception {
        CompressWriterFactory<String> writer =
                CompressWriters.forExtractor(new DefaultExtractor<String>())
                        .withHadoopCompression(codec, conf);
        List<String> lines = Arrays.asList("line1", "line2", "line3");

        File directory = prepareCompressedFile(codec, writer, lines);

        validateResults(directory, lines, new CompressionCodecFactory(conf).getCodecByName(codec));
    }

    private File prepareCompressedFile(
            String codec, CompressWriterFactory<String> writer, List<String> lines)
            throws Exception {
        final File outDir = tmpDir.resolve(codec).toFile();
        assertThat(outDir.mkdirs()).isTrue();

        StreamingFileSink<String> sink =
                StreamingFileSink.forBulkFormat(new Path(outDir.toURI()), writer)
                        .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                        .build();

        try (OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 1, 1, 0)) {
            testHarness.setup();
            testHarness.open();

            int time = 0;
            for (String line : lines) {
                testHarness.processElement(new StreamRecord<>(line, ++time));
            }

            testHarness.snapshot(1, ++time);
            testHarness.notifyOfCompletedCheckpoint(1);
        }

        return outDir;
    }

    private void validateResults(File folder, List<String> expected, CompressionCodec codec)
            throws Exception {
        File[] buckets = folder.listFiles();
        assertThat(buckets).isNotNull();
        assertThat(buckets).hasSize(1);

        final File[] partFiles = buckets[0].listFiles();
        assertThat(partFiles).isNotNull();
        assertThat(partFiles).hasSize(1);

        for (File partFile : partFiles) {
            assertThat(partFile.length()).isGreaterThan(0);
            final List<String> fileContent = readFile(partFile, codec);
            assertThat(fileContent).isEqualTo(expected);
        }
    }

    private List<String> readFile(File file, CompressionCodec codec) throws Exception {
        try (FileInputStream inputStream = new FileInputStream(file)) {
            try (InputStreamReader readerStream =
                    new InputStreamReader(
                            (codec == null) ? inputStream : codec.createInputStream(inputStream))) {
                try (BufferedReader reader = new BufferedReader(readerStream)) {
                    return reader.lines().collect(Collectors.toList());
                }
            }
        }
    }
}
