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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.compress.extractor.DefaultExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration test case for writing bulk encoded files with the {@link StreamingFileSink} and
 * Hadoop Compression Codecs.
 */
public class CompressionFactoryITCase extends AbstractTestBase {

    private final Configuration configuration = new Configuration();

    private static final String TEST_CODEC_NAME = "Bzip2";

    private final List<String> testData = Arrays.asList("line1", "line2", "line3");

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(20);

    @Test
    public void testWriteCompressedFile() throws Exception {
        final File folder = TEMPORARY_FOLDER.newFolder();
        final Path testPath = Path.fromLocalFile(folder);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        DataStream<String> stream =
                env.addSource(new FiniteTestSource<>(testData), TypeInformation.of(String.class));

        stream.map(str -> str)
                .addSink(
                        StreamingFileSink.forBulkFormat(
                                        testPath,
                                        CompressWriters.forExtractor(new DefaultExtractor<String>())
                                                .withHadoopCompression(TEST_CODEC_NAME))
                                .build());

        env.execute();

        validateResults(
                folder,
                testData,
                new CompressionCodecFactory(configuration).getCodecByName(TEST_CODEC_NAME));
    }

    private List<String> readFile(File file, CompressionCodec codec) throws Exception {
        try (FileInputStream inputStream = new FileInputStream(file);
                InputStreamReader readerStream =
                        new InputStreamReader(codec.createInputStream(inputStream));
                BufferedReader reader = new BufferedReader(readerStream)) {
            return reader.lines().collect(Collectors.toList());
        }
    }

    private void validateResults(File folder, List<String> expected, CompressionCodec codec)
            throws Exception {
        File[] buckets = folder.listFiles();
        assertNotNull(buckets);
        assertEquals(1, buckets.length);

        final File[] partFiles = buckets[0].listFiles();
        assertNotNull(partFiles);
        assertEquals(2, partFiles.length);

        for (File partFile : partFiles) {
            assertTrue(partFile.length() > 0);

            final List<String> fileContent = readFile(partFile, codec);
            assertEquals(expected, fileContent);
        }
    }
}
