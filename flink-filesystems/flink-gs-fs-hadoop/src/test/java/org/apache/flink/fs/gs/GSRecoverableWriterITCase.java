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

package org.apache.flink.fs.gs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the Google Cloud Storage {@link RecoverableWriter} against a real GCS
 * bucket. Exercises the write / checkpoint ({@code persist}) / recover / commit flow that backs
 * exactly-once {@code FileSink} checkpointing on GCS.
 *
 * <p>Skipped unless a GCS bucket is configured; see {@link GSTestCredentials}.
 */
class GSRecoverableWriterITCase {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    private static FileSystem fileSystem;
    private static Path basePath;

    private Path targetFile;

    @BeforeAll
    static void checkCredentialsAndSetup() throws Exception {
        GSTestCredentials.assumeCredentialsAvailable();
        FileSystem.initialize(new Configuration(), null);
        basePath = new Path(GSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
        fileSystem = basePath.getFileSystem();
    }

    @AfterAll
    static void teardown() throws Exception {
        if (fileSystem != null) {
            fileSystem.delete(basePath, true);
        }
        FileSystem.initialize(new Configuration(), null);
    }

    @BeforeEach
    void beforeEach() {
        targetFile = new Path(basePath, UUID.randomUUID().toString());
    }

    @AfterEach
    void afterEach() throws Exception {
        fileSystem.delete(targetFile, false);
    }

    @Test
    void testWriteAndCommit() throws Exception {
        final byte[] data = randomData(4 * 1024 * 1024);

        final RecoverableWriter writer = fileSystem.createRecoverableWriter();
        final RecoverableFsDataOutputStream stream = writer.open(targetFile);
        stream.write(data);
        stream.closeForCommit().commit();

        assertThat(readFully(targetFile)).isEqualTo(data);
    }

    @Test
    void testPersistRecoverAndCommit() throws Exception {
        final byte[] data = randomData(5 * 1024 * 1024);
        final int checkpointOffset = 2 * 1024 * 1024;

        final RecoverableWriter writer = fileSystem.createRecoverableWriter();

        // write up to the checkpoint offset, then take a checkpoint
        final RecoverableFsDataOutputStream stream = writer.open(targetFile);
        stream.write(data, 0, checkpointOffset);
        final RecoverableWriter.ResumeRecoverable checkpoint = stream.persist();

        // simulate a failure: abandon the original stream and recover from the checkpoint
        final RecoverableFsDataOutputStream recovered = writer.recover(checkpoint);
        recovered.write(data, checkpointOffset, data.length - checkpointOffset);
        recovered.closeForCommit().commit();

        // the committed object must contain exactly the bytes written across the recovery
        assertThat(readFully(targetFile)).isEqualTo(data);
    }

    private static byte[] randomData(int size) {
        final byte[] data = new byte[size];
        new Random(42).nextBytes(data);
        return data;
    }

    private static byte[] readFully(Path path) throws Exception {
        try (FSDataInputStream in = fileSystem.open(path);
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = in.read(buffer)) > 0) {
                out.write(buffer, 0, read);
            }
            return out.toByteArray();
        }
    }
}
