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

package org.apache.flink.connector.file.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.TestDataGenerators;
import org.apache.flink.connector.file.sink.compactor.ConcatFileCompactor;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.compress.CompressWriters;
import org.apache.flink.formats.compress.extractor.DefaultExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.UniqueBucketAssigner;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the proper compaction of concatenable compressed data. */
@ExtendWith(MiniClusterExtension.class)
class CompressedCompactionITCase {

    private static final List<String> TEST_DATA = Arrays.asList("line1", "line2", "line3");

    @TempDir private File tmpDir;

    /**
     * BZip2 codec is also supported, but there is a bug in Hadoop 2.x which breaks reading with
     * that codec. For details see <a
     * href="https://issues.apache.org/jira/browse/HADOOP-6852">HADOOP-6852</a>.
     */
    @ParameterizedTest
    @ValueSource(strings = {"Deflate", "Gzip"})
    void testCompactionOfConcatenableCompressedFiles(String compressionCodecName) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        DataStream<String> ds =
                env.fromSource(
                        TestDataGenerators.fromDataWithSnapshotsLatch(TEST_DATA, Types.STRING),
                        WatermarkStrategy.noWatermarks(),
                        "Test Source");

        DataStreamSink<String> sink =
                ds.sinkTo(
                        FileSink.forBulkFormat(
                                        Path.fromLocalFile(tmpDir),
                                        CompressWriters.forExtractor(new DefaultExtractor<String>())
                                                .withHadoopCompression(compressionCodecName))
                                .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                                .enableCompact(
                                        FileCompactStrategy.Builder.newBuilder()
                                                .setSizeThreshold(10000)
                                                .build(),
                                        new ConcatFileCompactor(null))
                                .build());
        sink.uid("test-compressed-sink");

        env.execute();

        // TestDataGenerators#fromDataWithSnapshotsLatch emits the given data twice.
        List<String> expected = new ArrayList<>();
        expected.addAll(TEST_DATA);
        expected.addAll(TEST_DATA);

        validateResults(
                tmpDir,
                expected,
                new CompressionCodecFactory(new Configuration())
                        .getCodecByName(compressionCodecName));
    }

    private static void validateResults(File folder, List<String> expected, CompressionCodec codec)
            throws Exception {
        File[] buckets = folder.listFiles();
        assertThat(buckets).isNotNull();
        assertThat(buckets).hasSize(1);

        File[] partFiles = buckets[0].listFiles();
        assertThat(partFiles).isNotNull();
        assertThat(partFiles).hasSize(1);

        File compactedFile = partFiles[0];
        assertThat(compactedFile.length()).isGreaterThan(0);

        List<String> compactedContent = readFile(compactedFile, codec);
        assertThat(compactedContent).containsExactlyElementsOf(expected);
    }

    private static List<String> readFile(File file, CompressionCodec codec) throws Exception {
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
