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

package org.apache.flink.formats.hadoop.bulk;

import org.apache.flink.formats.hadoop.bulk.HadoopPathBasedPartFileWriter.HadoopPathBasedPendingFileRecoverable;
import org.apache.flink.formats.hadoop.bulk.HadoopPathBasedPartFileWriter.HadoopPathBasedPendingFileRecoverableSerializer;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link HadoopPathBasedPendingFileRecoverableSerializer} that verify we can still
 * read the recoverable serialized by the previous versions.
 */
class HadoopPathBasedPendingFileRecoverableSerializerMigrationTest {

    private static final int CURRENT_VERSION = 1;

    public Integer previousVersion = 1;

    private static final org.apache.hadoop.fs.Path TARGET_PATH =
            new org.apache.hadoop.fs.Path("file://target");
    private static final org.apache.hadoop.fs.Path TEMP_PATH =
            new org.apache.hadoop.fs.Path("file://temp");

    private static final java.nio.file.Path CASE_PATH =
            Paths.get("src/test/resources/")
                    .resolve("pending-file-recoverable-serializer-migration");

    @Test
    @Disabled
    void prepareDeserialization() throws IOException {
        String scenario = "common";
        java.nio.file.Path versionPath = resolveVersionPath(CURRENT_VERSION, scenario);
        HadoopPathBasedPendingFileRecoverableSerializer serializer =
                new HadoopPathBasedPendingFileRecoverableSerializer();
        HadoopPathBasedPendingFileRecoverable recoverable =
                new HadoopPathBasedPendingFileRecoverable(TARGET_PATH, TEMP_PATH);
        byte[] bytes = serializer.serialize(recoverable);
        Files.write(versionPath, bytes);
    }

    @Test
    void testSerialization() throws IOException {
        String scenario = "common";
        java.nio.file.Path versionPath = resolveVersionPath(previousVersion, scenario);
        HadoopPathBasedPendingFileRecoverableSerializer serializer =
                new HadoopPathBasedPendingFileRecoverableSerializer();

        HadoopPathBasedPendingFileRecoverable recoverable =
                serializer.deserialize(previousVersion, Files.readAllBytes(versionPath));

        assertThat(recoverable.getTargetFilePath()).isEqualTo(TARGET_PATH);
        assertThat(recoverable.getTempFilePath()).isEqualTo(TEMP_PATH);
    }

    private java.nio.file.Path resolveVersionPath(long version, String scenario) {
        return CASE_PATH.resolve(scenario + "-v" + version);
    }
}
