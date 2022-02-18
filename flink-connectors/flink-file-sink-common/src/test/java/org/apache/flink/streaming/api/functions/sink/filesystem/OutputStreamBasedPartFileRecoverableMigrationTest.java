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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.RecoverableWriter.CommitRecoverable;
import org.apache.flink.core.fs.RecoverableWriter.ResumeRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.InProgressFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileRecoverableSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverableSerializer;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;

/**
 * Tests for the {@link OutputStreamBasedInProgressFileRecoverableSerializer} and the {@link
 * OutputStreamBasedPendingFileRecoverableSerializer}that verify we can still read the recoverable
 * serialized by the previous versions.
 */
@RunWith(Parameterized.class)
public class OutputStreamBasedPartFileRecoverableMigrationTest extends TestLogger {

    private static final int CURRENT_VERSION = 1;

    @Parameterized.Parameters(name = "Previous Version = {0}")
    public static Collection<Integer> previousVersions() {
        return Collections.singletonList(1);
    }

    @Parameterized.Parameter public Integer previousVersion;

    private static final String IN_PROGRESS_CONTENT = "writing";
    private static final String PENDING_CONTENT = "wrote";

    private static final java.nio.file.Path BASE_PATH =
            Paths.get("src/test/resources/").resolve("recoverable-serializer-migration");

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    @Ignore
    public void prepareDeserializationInProgress() throws IOException {
        String scenario = "in-progress";
        java.nio.file.Path path = resolveVersionPath(CURRENT_VERSION, scenario);

        RecoverableWriter writer = FileSystem.getLocalFileSystem().createRecoverableWriter();
        OutputStreamBasedInProgressFileRecoverableSerializer serializer =
                new OutputStreamBasedInProgressFileRecoverableSerializer(
                        writer.getResumeRecoverableSerializer());

        RecoverableFsDataOutputStream outputStream =
                writer.open(new Path(path.resolve("content").toString()));
        outputStream.write(IN_PROGRESS_CONTENT.getBytes(StandardCharsets.UTF_8));
        ResumeRecoverable resumeRecoverable = outputStream.persist();

        OutputStreamBasedInProgressFileRecoverable recoverable =
                new OutputStreamBasedInProgressFileRecoverable(resumeRecoverable);
        byte[] bytes = serializer.serialize(recoverable);
        Files.write(path.resolve("recoverable"), bytes);
    }

    @Test
    public void testSerializationInProgress() throws IOException {
        String scenario = "in-progress";
        java.nio.file.Path path = resolveVersionPath(previousVersion, scenario);

        RecoverableWriter writer = FileSystem.getLocalFileSystem().createRecoverableWriter();
        OutputStreamBasedInProgressFileRecoverableSerializer serializer =
                new OutputStreamBasedInProgressFileRecoverableSerializer(
                        writer.getResumeRecoverableSerializer());

        InProgressFileRecoverable recoverable =
                serializer.deserialize(
                        previousVersion, Files.readAllBytes(path.resolve("recoverable")));

        Assert.assertTrue(recoverable instanceof OutputStreamBasedInProgressFileRecoverable);
        // make sure the ResumeRecoverable is valid
        writer.recover(
                ((OutputStreamBasedInProgressFileRecoverable) recoverable).getResumeRecoverable());
    }

    @Test
    @Ignore
    public void prepareDeserializationPending() throws IOException {
        String scenario = "pending";
        java.nio.file.Path path = resolveVersionPath(CURRENT_VERSION, scenario);

        RecoverableWriter writer = FileSystem.getLocalFileSystem().createRecoverableWriter();
        OutputStreamBasedPendingFileRecoverableSerializer serializer =
                new OutputStreamBasedPendingFileRecoverableSerializer(
                        writer.getCommitRecoverableSerializer());

        RecoverableFsDataOutputStream outputStream =
                writer.open(new Path(path.resolve("content").toString()));
        outputStream.write(PENDING_CONTENT.getBytes(StandardCharsets.UTF_8));
        CommitRecoverable commitRecoverable = outputStream.closeForCommit().getRecoverable();

        OutputStreamBasedPendingFileRecoverable recoverable =
                new OutputStreamBasedPendingFileRecoverable(commitRecoverable);
        byte[] bytes = serializer.serialize(recoverable);
        Files.write(path.resolve("recoverable"), bytes);
    }

    @Test
    public void testSerializationPending() throws IOException {
        String scenario = "pending";
        java.nio.file.Path path = resolveVersionPath(previousVersion, scenario);

        RecoverableWriter writer = FileSystem.getLocalFileSystem().createRecoverableWriter();
        OutputStreamBasedPendingFileRecoverableSerializer serializer =
                new OutputStreamBasedPendingFileRecoverableSerializer(
                        writer.getCommitRecoverableSerializer());

        PendingFileRecoverable recoverable =
                serializer.deserialize(
                        previousVersion, Files.readAllBytes(path.resolve("recoverable")));

        Assert.assertTrue(recoverable instanceof OutputStreamBasedPendingFileRecoverable);
        // make sure the CommitRecoverable is valid
        writer.recoverForCommit(
                ((OutputStreamBasedPendingFileRecoverable) recoverable).getCommitRecoverable());
    }

    private java.nio.file.Path resolveVersionPath(long version, String scenario) {
        return BASE_PATH.resolve(scenario + "-v" + version);
    }
}
