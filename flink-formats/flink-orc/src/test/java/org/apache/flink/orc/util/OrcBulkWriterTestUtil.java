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

package org.apache.flink.orc.util;

import org.apache.flink.orc.data.Record;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Util class for the OrcBulkWriter tests. */
public class OrcBulkWriterTestUtil {

    public static final String USER_METADATA_KEY = "userKey";
    public static final ByteBuffer USER_METADATA_VALUE = ByteBuffer.wrap("hello".getBytes());

    public static void validate(File files, List<Record> expected, CompressionKind compressionKind)
            throws IOException {
        assertThat(files).isNotNull();
        assertThat(files.exists()).isTrue();

        assertThat(expected).isNotNull();
        assertThat(expected).isNotEmpty();
        validateBucketAndFileSize(files, 1);

        final File[] buckets = files.listFiles();
        final File[] partFiles = buckets[0].listFiles();

        for (File partFile : partFiles) {
            try (final Reader reader = createOrcReader(partFile)) {

                assertThat(reader.getNumberOfRows()).isEqualTo(expected.size());
                assertThat(reader.getSchema().getFieldNames())
                        .hasSize(expected.get(0).getClass().getDeclaredFields().length);
                assertThat(reader.getCompressionKind()).isSameAs(compressionKind);
                assertThat(reader.hasMetadataValue(USER_METADATA_KEY)).isTrue();
                assertThat(reader.getMetadataKeys()).contains(USER_METADATA_KEY);

                final List<Record> results = getResults(reader);

                assertThat(results).hasSize(expected.size());
                assertThat(results).isEqualTo(expected);
            }
        }
    }

    private static List<Record> getResults(Reader reader) throws IOException {
        final List<Record> results = new ArrayList<>();

        final RecordReader recordReader = reader.rows();
        final VectorizedRowBatch batch = reader.getSchema().createRowBatch();

        while (recordReader.nextBatch(batch)) {
            final BytesColumnVector stringVector = (BytesColumnVector) batch.cols[0];
            final LongColumnVector intVector = (LongColumnVector) batch.cols[1];
            for (int r = 0; r < batch.size; r++) {
                final String name =
                        new String(
                                stringVector.vector[r],
                                stringVector.start[r],
                                stringVector.length[r]);
                final int age = (int) intVector.vector[r];

                results.add(new Record(name, age));
            }
            recordReader.close();
        }

        return results;
    }

    private static void validateBucketAndFileSize(File outputDir, int bucketCount) {
        final File[] buckets = outputDir.listFiles();
        assertThat(buckets).isNotNull();
        assertThat(buckets.length).isEqualTo(bucketCount);

        final File[] partFiles = buckets[0].listFiles();
        assertThat(partFiles.length).isNotNull();
    }

    private static Reader createOrcReader(File orcFile) throws IOException {
        assertThat(orcFile.exists()).isTrue();
        assertThat(orcFile.length()).isGreaterThan(0);

        final OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(new Configuration());
        return OrcFile.createReader(new org.apache.hadoop.fs.Path(orcFile.toURI()), readerOptions);
    }
}
