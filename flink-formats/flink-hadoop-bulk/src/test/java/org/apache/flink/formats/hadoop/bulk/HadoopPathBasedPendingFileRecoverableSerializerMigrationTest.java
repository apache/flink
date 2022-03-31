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
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;

/**
 * Tests for the {@link HadoopPathBasedPendingFileRecoverableSerializer} that verify we can still
 * read the recoverable serialized by the previous versions.
 */
@RunWith(Parameterized.class)
public class HadoopPathBasedPendingFileRecoverableSerializerMigrationTest extends TestLogger {

    private static final int CURRENT_VERSION = 1;

    @Parameterized.Parameters(name = "Previous Version = {0}")
    public static Collection<Integer> previousVersions() {
        return Collections.singletonList(1);
    }

    @Parameterized.Parameter public Integer previousVersion;

    private static final org.apache.hadoop.fs.Path TARGET_PATH =
            new org.apache.hadoop.fs.Path("file://target");
    private static final org.apache.hadoop.fs.Path TEMP_PATH =
            new org.apache.hadoop.fs.Path("file://temp");

    private static final java.nio.file.Path CASE_PATH =
            Paths.get("src/test/resources/")
                    .resolve("pending-file-recoverable-serializer-migration");

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    @Ignore
    public void prepareDeserialization() throws IOException {
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
    public void testSerialization() throws IOException {
        String scenario = "common";
        java.nio.file.Path versionPath = resolveVersionPath(previousVersion, scenario);
        HadoopPathBasedPendingFileRecoverableSerializer serializer =
                new HadoopPathBasedPendingFileRecoverableSerializer();

        HadoopPathBasedPendingFileRecoverable recoverable =
                serializer.deserialize(previousVersion, Files.readAllBytes(versionPath));

        Assert.assertEquals(TARGET_PATH, recoverable.getTargetFilePath());
        Assert.assertEquals(TEMP_PATH, recoverable.getTempFilePath());
    }

    private java.nio.file.Path resolveVersionPath(long version, String scenario) {
        return CASE_PATH.resolve(scenario + "-v" + version);
    }
}
