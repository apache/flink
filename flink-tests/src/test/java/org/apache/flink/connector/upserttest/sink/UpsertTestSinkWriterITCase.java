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

package org.apache.flink.connector.upserttest.sink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link UpsertTestSinkWriter}. */
@ExtendWith(TestLoggerExtension.class)
class UpsertTestSinkWriterITCase {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @TempDir private File tempDir;
    private File outputFile;
    private UpsertTestSinkWriter<Tuple2<String, String>> writer;
    private List<Tuple2<String, String>> expectedRecords;

    @BeforeEach
    void setup() {
        outputFile = tempDir.toPath().resolve(Paths.get("dir", "records.out")).toFile();
        writer = createSinkWriter(outputFile);
        expectedRecords = writeTestData(writer);
    }

    @AfterEach
    void tearDown() throws Exception {
        writer.close();
    }

    @Test
    void testWrite() throws Exception {
        writer.close();
        testRecordPresence(outputFile, expectedRecords);
    }

    @Test
    void testWriteOnCheckpoint() throws Exception {
        writer.flush(false);
        testRecordPresence(outputFile, expectedRecords);
    }

    private UpsertTestSinkWriter<Tuple2<String, String>> createSinkWriter(File outputFile) {
        SerializationSchema<Tuple2<String, String>> keySerializationSchema =
                element -> element.f0.getBytes();
        SerializationSchema<Tuple2<String, String>> valueSerializationSchema =
                element -> element.f1.getBytes();

        return new UpsertTestSinkWriter<>(
                outputFile, keySerializationSchema, valueSerializationSchema);
    }

    private List<Tuple2<String, String>> writeTestData(
            UpsertTestSinkWriter<Tuple2<String, String>> writer) {
        final List<Tuple2<String, String>> expectedRecords = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Tuple2<String, String> record = Tuple2.of("Key #" + i, "Value #" + i);
            expectedRecords.add(record);
            writer.write(record, null);
        }
        return expectedRecords;
    }

    private void testRecordPresence(File outputFile, List<Tuple2<String, String>> expectedRecords)
            throws IOException {
        DeserializationSchema<String> keyDeserializationSchema = new SimpleStringSchema();
        DeserializationSchema<String> valueDeserializationSchema = new SimpleStringSchema();

        Map<String, String> resultMap =
                UpsertTestFileUtil.readRecords(
                        outputFile, keyDeserializationSchema, valueDeserializationSchema);

        for (Tuple2<String, String> record : expectedRecords) {
            assertThat(resultMap).containsEntry(record.f0, record.f1);
        }
    }
}
