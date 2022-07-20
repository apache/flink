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

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.RecoverableWriter.CommitRecoverable;
import org.apache.flink.core.fs.RecoverableWriter.ResumeRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link FileSinkCommittableSerializer} that verify we can still read the recoverable
 * serialized by the previous versions.
 */
class FileSinkCommittableSerializerMigrationTest {

    private static final int CURRENT_VERSION = 1;

    static Stream<Integer> previousVersions() {
        return Stream.of(1);
    }

    private static final String IN_PROGRESS_CONTENT = "writing";
    private static final String PENDING_CONTENT = "wrote";

    private static final java.nio.file.Path BASE_PATH =
            Paths.get("src/test/resources/").resolve("committable-serializer-migration");

    @Test
    @Disabled
    void prepareDeserializationInProgressToCleanup() throws IOException {
        String scenario = "in-progress";
        java.nio.file.Path path = resolveVersionPath(CURRENT_VERSION, scenario);

        BucketWriter<String, String> bucketWriter = createBucketWriter();
        RecoverableWriter writer = FileSystem.getLocalFileSystem().createRecoverableWriter();
        FileSinkCommittableSerializer serializer =
                new FileSinkCommittableSerializer(
                        bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                        bucketWriter.getProperties().getInProgressFileRecoverableSerializer());

        RecoverableFsDataOutputStream outputStream =
                writer.open(new Path(path.resolve("content").toString()));
        outputStream.write(IN_PROGRESS_CONTENT.getBytes(StandardCharsets.UTF_8));
        ResumeRecoverable resumeRecoverable = outputStream.persist();

        OutputStreamBasedInProgressFileRecoverable recoverable =
                new OutputStreamBasedInProgressFileRecoverable(resumeRecoverable);
        FileSinkCommittable committable = new FileSinkCommittable("0", recoverable);

        byte[] bytes = serializer.serialize(committable);
        Files.write(path.resolve("committable"), bytes);
    }

    @ParameterizedTest(name = "Previous Version = {0}")
    @MethodSource("previousVersions")
    void testSerializationInProgressToCleanup(Integer previousVersion) throws IOException {
        String scenario = "in-progress";
        java.nio.file.Path path = resolveVersionPath(previousVersion, scenario);

        BucketWriter<String, String> bucketWriter = createBucketWriter();
        FileSinkCommittableSerializer serializer =
                new FileSinkCommittableSerializer(
                        bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                        bucketWriter.getProperties().getInProgressFileRecoverableSerializer());

        FileSinkCommittable committable =
                serializer.deserialize(
                        previousVersion, Files.readAllBytes(path.resolve("committable")));

        assertThat(committable.hasInProgressFileToCleanup()).isTrue();
        assertThat(committable.hasPendingFile()).isFalse();
    }

    @Test
    @Disabled
    void prepareDeserializationPending() throws IOException {
        String scenario = "pending";
        java.nio.file.Path path = resolveVersionPath(CURRENT_VERSION, scenario);

        BucketWriter<String, String> bucketWriter = createBucketWriter();
        RecoverableWriter writer = FileSystem.getLocalFileSystem().createRecoverableWriter();
        FileSinkCommittableSerializer serializer =
                new FileSinkCommittableSerializer(
                        bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                        bucketWriter.getProperties().getInProgressFileRecoverableSerializer());

        RecoverableFsDataOutputStream outputStream =
                writer.open(new Path(path.resolve("content").toString()));
        outputStream.write(PENDING_CONTENT.getBytes(StandardCharsets.UTF_8));
        CommitRecoverable commitRecoverable = outputStream.closeForCommit().getRecoverable();

        OutputStreamBasedPendingFileRecoverable recoverable =
                new OutputStreamBasedPendingFileRecoverable(commitRecoverable);
        FileSinkCommittable committable = new FileSinkCommittable("0", recoverable);

        byte[] bytes = serializer.serialize(committable);
        Files.write(path.resolve("committable"), bytes);
    }

    @ParameterizedTest(name = "Previous Version = {0}")
    @MethodSource("previousVersions")
    void testSerializationPending(Integer previousVersion) throws IOException {
        String scenario = "pending";
        java.nio.file.Path path = resolveVersionPath(previousVersion, scenario);

        BucketWriter<String, String> bucketWriter = createBucketWriter();
        FileSinkCommittableSerializer serializer =
                new FileSinkCommittableSerializer(
                        bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                        bucketWriter.getProperties().getInProgressFileRecoverableSerializer());

        FileSinkCommittable committable =
                serializer.deserialize(
                        previousVersion, Files.readAllBytes(path.resolve("committable")));

        assertThat(committable.hasPendingFile()).isTrue();
        assertThat(committable.hasInProgressFileToCleanup()).isFalse();
    }

    private java.nio.file.Path resolveVersionPath(long version, String scenario) {
        return BASE_PATH.resolve(scenario + "-v" + version);
    }

    private static RowWiseBucketWriter<String, String> createBucketWriter() throws IOException {
        return new RowWiseBucketWriter<>(
                FileSystem.getLocalFileSystem().createRecoverableWriter(),
                new SimpleStringEncoder<>());
    }
}
