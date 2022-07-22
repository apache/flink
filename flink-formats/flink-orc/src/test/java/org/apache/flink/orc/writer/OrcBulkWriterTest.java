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

package org.apache.flink.orc.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.data.Record;
import org.apache.flink.orc.util.OrcBulkWriterTestUtil;
import org.apache.flink.orc.vector.RecordVectorizer;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.UniqueBucketAssigner;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.CompressionKind;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.InMemoryKeystore;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.orc.util.OrcBulkWriterTestUtil.getOrcReader;
import static org.apache.flink.orc.util.OrcBulkWriterTestUtil.validateBucketAndFileSize;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for the ORC BulkWriter implementation. */
public class OrcBulkWriterTest {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private final String schema = "struct<_col0:string,_col1:int>";
    private final List<Record> input =
            Arrays.asList(new Record("Shiv", 44), new Record("Jesse", 23), new Record("Walt", 50));

    @Test
    public void testOrcBulkWriter() throws Exception {
        writeOrcFileWithCodec(CompressionKind.LZ4);
        writeOrcFileWithCodec(CompressionKind.SNAPPY);
        writeOrcFileWithCodec(CompressionKind.ZLIB);
        writeOrcFileWithCodec(CompressionKind.LZO);
        writeOrcFileWithCodec(CompressionKind.ZSTD);
    }

    private void writeOrcFileWithCodec(CompressionKind codec) throws Exception {
        final File outDir = TEMPORARY_FOLDER.newFolder();
        final Properties writerProps = new Properties();
        writerProps.setProperty("orc.compress", codec.name());

        OrcBulkWriterFactory<Record> writer =
                new OrcBulkWriterFactory<>(
                        new RecordVectorizer(schema), writerProps, new Configuration());

        writeRecordsIntoOrcFile(outDir, writer);

        // validate records and compression kind
        OrcBulkWriterTestUtil.validate(outDir, input, codec);
    }

    @Test
    public void testOrcColumnEncryption() throws Exception {
        final File outDir = TEMPORARY_FOLDER.newFolder();

        // Simple configuration for column encryption.
        Configuration conf = new Configuration();
        conf.set("orc.encrypt", "pii:_col0");
        conf.set("orc.mask", "sha256:_col0");
        OrcFile.WriterOptions writerOptions =
                OrcFile.writerOptions(conf)
                        .encrypt(OrcConf.ENCRYPTION.getString(conf))
                        .masks(OrcConf.DATA_MASK.getString(conf))
                        .setKeyProvider(new DummyInMemoryKeystore());

        OrcBulkWriterFactory<Record> writer =
                new OrcBulkWriterFactory<>(new RecordVectorizer(schema), writerOptions);

        writeRecordsIntoOrcFile(outDir, writer);

        validateBucketAndFileSize(outDir, 1);
        final File[] buckets = outDir.listFiles();
        final File[] partFiles = buckets[0].listFiles();

        // Validate data in orc and file schema attribute value.
        for (File partFile : partFiles) {
            validateEncryptedColumn(partFile);
        }
    }

    private void validateEncryptedColumn(File orcFile) throws IOException {
        Reader reader = getOrcReader(orcFile);
        assertThat(reader.getNumberOfRows()).isEqualTo(input.size());

        List<String> fieldNames = reader.getSchema().getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (StringUtils.equals(fieldNames.get(i), "_col0")) {
                String encrypt =
                        reader.getSchema().getChildren().get(i).getAttributeValue("encrypt");
                assertThat(encrypt).isEqualTo("pii");
            }
        }

        List<Record> results = OrcBulkWriterTestUtil.getResults(reader);
        for (int i = 0; i < results.size(); i++) {
            assertThat(results.get(i).getName()).isNotNull();
            assertThat(results.get(i).getName()).isNotEqualTo(input.get(i).getName());
            assertThat(results.get(i).getAge()).isEqualTo(input.get(i).getAge());
        }
    }

    private void writeRecordsIntoOrcFile(File outDir, OrcBulkWriterFactory<Record> writer)
            throws Exception {
        StreamingFileSink<Record> sink =
                StreamingFileSink.forBulkFormat(new Path(outDir.toURI()), writer)
                        .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                        .withBucketCheckInterval(10000)
                        .build();

        try (OneInputStreamOperatorTestHarness<Record, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 1, 1, 0)) {

            testHarness.setup();
            testHarness.open();

            int time = 0;
            for (final Record record : input) {
                testHarness.processElement(record, ++time);
            }

            testHarness.snapshot(1, ++time);
            testHarness.notifyOfCompletedCheckpoint(1);
        }
    }

    /** A fake in-memory keystore for testing. */
    private static class DummyInMemoryKeystore extends InMemoryKeystore {

        public DummyInMemoryKeystore() {
            super();

            try {
                byte[] kmsKey = "secret123".getBytes(StandardCharsets.UTF_8);
                addKey("pii", EncryptionAlgorithm.AES_CTR_128, kmsKey);
            } catch (IOException e) {
                throw new RuntimeException("Failed to initial memory keystore.");
            }
        }
    }
}
