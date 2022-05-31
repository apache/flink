/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.LocalChangelogRegistry;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.shaded.guava30.com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.flink.util.ExceptionUtils.rethrowIOException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link FsStateChangelogWriter} test. */
class FsStateChangelogWriterTest {
    private static final int KEY_GROUP = 0;
    private final Random random = new Random();

    @Test
    void testAppend() throws Exception {
        withWriter(
                (writer, uploader) -> {
                    writer.append(KEY_GROUP, getBytes());
                    writer.append(KEY_GROUP, getBytes());
                    writer.append(KEY_GROUP, getBytes());
                    assertNoUpload(uploader, "append shouldn't persist");
                });
    }

    @Test
    void testPreUpload() throws Exception {
        int threshold = 1000;
        withWriter(
                threshold,
                (writer, uploader) -> {
                    byte[] bytes = getBytes(threshold);
                    SequenceNumber sqn = append(writer, bytes);
                    assertSubmittedOnly(uploader, bytes);
                    uploader.reset();
                    writer.persist(sqn);
                    assertNoUpload(uploader, "changes should have been pre-uploaded");
                });
    }

    @Test
    void testPersist() throws Exception {
        withWriter(
                (writer, uploader) -> {
                    byte[] bytes = getBytes();
                    CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> future =
                            writer.persist(append(writer, bytes));
                    assertSubmittedOnly(uploader, bytes);
                    uploader.completeUpload();
                    assertThat(
                                    getOnlyElement(
                                                    future.get()
                                                            .getJobManagerOwnedSnapshot()
                                                            .getHandlesAndOffsets())
                                            .f0
                                            .asBytesIfInMemory()
                                            .get())
                            .isEqualTo(bytes);
                });
    }

    @Test
    void testPersistAgain() throws Exception {
        withWriter(
                (writer, uploader) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn = append(writer, bytes);
                    writer.persist(sqn);
                    uploader.completeUpload();
                    uploader.reset();
                    writer.confirm(sqn, writer.nextSequenceNumber(), 1L);
                    writer.persist(sqn);
                    assertNoUpload(uploader, "confirmed changes shouldn't be re-uploaded");
                });
    }

    @Test
    void testNoReUploadBeforeCompletion() throws Exception {
        withWriter(
                (writer, uploader) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn = append(writer, bytes);
                    writer.persist(sqn);
                    uploader.reset();
                    writer.persist(sqn);
                    assertNoUpload(uploader, "no re-upload should happen");
                });
    }

    @Test
    void testPersistNewlyAppended() throws Exception {
        withWriter(
                (writer, uploader) -> {
                    SequenceNumber sqn = append(writer, getBytes());
                    writer.persist(sqn);
                    uploader.reset();
                    byte[] bytes = getBytes();
                    sqn = append(writer, bytes);
                    writer.persist(sqn);
                    assertSubmittedOnly(uploader, bytes);
                });
    }

    /** Emulates checkpoint abortion followed by a new checkpoint. */
    @Test
    void testPersistAfterReset() throws Exception {
        withWriter(
                (writer, uploader) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn = append(writer, bytes);
                    writer.reset(sqn, SequenceNumber.of(Long.MAX_VALUE), Long.MAX_VALUE);
                    uploader.reset();
                    writer.persist(sqn);
                    assertSubmittedOnly(uploader, bytes);
                });
    }

    @Test
    void testPersistFailure() {
        assertThatThrownBy(
                        () ->
                                withWriter(
                                        (writer, uploader) -> {
                                            byte[] bytes = getBytes();
                                            SequenceNumber sqn = append(writer, bytes);
                                            CompletableFuture<
                                                            SnapshotResult<
                                                                    ChangelogStateHandleStreamImpl>>
                                                    future = writer.persist(sqn);
                                            uploader.failUpload(new RuntimeException("test"));
                                            try {
                                                future.get();
                                            } catch (ExecutionException e) {
                                                rethrowIOException(e.getCause());
                                            }
                                        }))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testPersistFailedChanges() {
        assertThatThrownBy(
                        () ->
                                withWriter(
                                        (writer, uploader) -> {
                                            byte[] bytes = getBytes();
                                            SequenceNumber sqn = append(writer, bytes);
                                            writer.persist(sqn); // future result ignored
                                            uploader.failUpload(new RuntimeException("test"));
                                            writer.persist(sqn); // should fail right away
                                        }))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testPersistNonFailedChanges() throws Exception {
        withWriter(
                (writer, uploader) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn1 = append(writer, bytes);
                    writer.persist(sqn1); // future result ignored
                    uploader.failUpload(new RuntimeException("test"));
                    uploader.reset();
                    SequenceNumber sqn2 = append(writer, bytes);
                    CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> future =
                            writer.persist(sqn2);
                    uploader.completeUpload();
                    future.get();
                });
    }

    @Test
    void testTruncate() {
        assertThatThrownBy(
                        () ->
                                withWriter(
                                        (writer, uploader) -> {
                                            SequenceNumber sqn = append(writer, getBytes());
                                            writer.truncate(sqn.next());
                                            writer.persist(sqn);
                                        }))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void withWriter(
            BiConsumerWithException<FsStateChangelogWriter, TestingStateChangeUploader, Exception>
                    test)
            throws Exception {
        withWriter(1000, test);
    }

    private void withWriter(
            int appendPersistThreshold,
            BiConsumerWithException<FsStateChangelogWriter, TestingStateChangeUploader, Exception>
                    test)
            throws Exception {
        TestingStateChangeUploader uploader = new TestingStateChangeUploader();
        try (FsStateChangelogWriter writer =
                new FsStateChangelogWriter(
                        UUID.randomUUID(),
                        KeyGroupRange.of(KEY_GROUP, KEY_GROUP),
                        StateChangeUploadScheduler.directScheduler(uploader),
                        appendPersistThreshold,
                        new SyncMailboxExecutor(),
                        TaskChangelogRegistry.NO_OP,
                        TestLocalRecoveryConfig.disabled(),
                        LocalChangelogRegistry.NO_OP)) {
            test.accept(writer, uploader);
        }
    }

    private void assertSubmittedOnly(TestingStateChangeUploader uploader, byte[] bytes) {
        assertThat(getOnlyElement(getOnlyElement(uploader.getUploaded()).getChanges()).getChange())
                .isEqualTo(bytes);
    }

    private SequenceNumber append(FsStateChangelogWriter writer, byte[] bytes) throws IOException {
        SequenceNumber sequenceNumber = writer.nextSequenceNumber();
        writer.append(KEY_GROUP, bytes);
        return sequenceNumber;
    }

    private byte[] getBytes() {
        return getBytes(10);
    }

    private byte[] getBytes(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    private static void assertNoUpload(TestingStateChangeUploader uploader, String message) {
        assertThat(uploader.getUploaded()).isEmpty();
    }
}
