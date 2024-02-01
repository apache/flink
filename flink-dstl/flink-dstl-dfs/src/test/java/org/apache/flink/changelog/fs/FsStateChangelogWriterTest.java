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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.LocalChangelogRegistry;
import org.apache.flink.runtime.state.changelog.LocalChangelogRegistryImpl;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.shaded.guava31.com.google.common.collect.Iterables.getOnlyElement;
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
                    writer.persist(sqn, 1L);
                    assertNoUpload(uploader, "changes should have been pre-uploaded");
                });
    }

    @Test
    void testPersist() throws Exception {
        withWriter(
                (writer, uploader) -> {
                    byte[] bytes = getBytes();
                    CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> future =
                            writer.persist(append(writer, bytes), 1L);
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
                    writer.persist(sqn, 1L);
                    uploader.completeUpload();
                    uploader.reset();
                    writer.confirm(sqn, writer.nextSequenceNumber(), 1L);
                    writer.persist(sqn, 2L);
                    assertNoUpload(uploader, "confirmed changes shouldn't be re-uploaded");
                });
    }

    @Test
    void testFileAvailableAfterPreUpload() throws Exception {
        long appendPersistThreshold = 100;

        TaskChangelogRegistry taskChangelogRegistry =
                new TaskChangelogRegistryImpl(Executors.directExecutor());

        try (DiscardRecordableStateChangeUploader uploader =
                        new DiscardRecordableStateChangeUploader(taskChangelogRegistry);
                TestingBatchingUploadScheduler uploadScheduler =
                        new TestingBatchingUploadScheduler(uploader);
                FsStateChangelogWriter writer =
                        new FsStateChangelogWriter(
                                UUID.randomUUID(),
                                KeyGroupRange.of(KEY_GROUP, KEY_GROUP),
                                uploadScheduler,
                                appendPersistThreshold,
                                new SyncMailboxExecutor(),
                                taskChangelogRegistry,
                                TestLocalRecoveryConfig.disabled(),
                                LocalChangelogRegistry.NO_OP)) {
            SequenceNumber initialSqn = writer.initialSequenceNumber();

            writer.append(KEY_GROUP, getBytes(10)); // sqn: 0

            long checkpointId = 1L;
            // checkpoint 1 trigger
            SequenceNumber checkpoint1sqn = writer.nextSequenceNumber();
            writer.persist(initialSqn, checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 1 completed
            writer.confirm(initialSqn, checkpoint1sqn, checkpointId);

            writer.append(KEY_GROUP, getBytes(10)); // sqn: 1

            // materialization 1 trigger
            SequenceNumber materializationSqn = writer.nextSequenceNumber();

            writer.append(KEY_GROUP, getBytes(10)); // sqn: 2

            // materialization 1 completed
            // checkpoint 2 trigger
            SequenceNumber checkpoint2sqn = writer.nextSequenceNumber();
            writer.persist(materializationSqn, ++checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 2 completed
            writer.confirm(materializationSqn, checkpoint2sqn, checkpointId);

            // checkpoint 1 subsumed
            writer.truncate(
                    materializationSqn.compareTo(checkpoint1sqn) < 0
                            ? materializationSqn
                            : checkpoint1sqn);

            writer.append(KEY_GROUP, getBytes(10)); // sqn: 3

            // checkpoint 3 trigger
            SequenceNumber checkpoint3sqn = writer.nextSequenceNumber();
            writer.persist(materializationSqn, ++checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 3 completed
            writer.confirm(materializationSqn, checkpoint3sqn, checkpointId);

            // trigger pre-emptive upload
            writer.append(KEY_GROUP, getBytes(100)); // sqn: 4
            uploadScheduler.scheduleAll();

            // checkpoint 2 subsumed
            writer.truncate(
                    materializationSqn.compareTo(checkpoint2sqn) < 0
                            ? materializationSqn
                            : checkpoint2sqn);

            // checkpoint 4 trigger
            SequenceNumber checkpoint4sqn = writer.nextSequenceNumber();
            CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> future =
                    writer.persist(materializationSqn, ++checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 4 completed
            writer.confirm(materializationSqn, checkpoint4sqn, checkpointId);

            SnapshotResult<ChangelogStateHandleStreamImpl> result = future.get();
            ChangelogStateHandleStreamImpl resultHandle = result.getJobManagerOwnedSnapshot();

            for (Tuple2<StreamStateHandle, Long> handleAndOffset :
                    resultHandle.getHandlesAndOffsets()) {
                assertThat(uploader.isDiscarded(handleAndOffset.f0))
                        .isFalse(); // all handles should not be discarded
            }
        }
    }

    @Test
    void testFileAvailableAfterClose() throws Exception {
        long appendPersistThreshold = 100;

        TaskChangelogRegistry taskChangelogRegistry =
                new TaskChangelogRegistryImpl(Executors.directExecutor());

        try (DiscardRecordableStateChangeUploader uploader =
                        new DiscardRecordableStateChangeUploader(taskChangelogRegistry);
                TestingBatchingUploadScheduler uploadScheduler =
                        new TestingBatchingUploadScheduler(uploader)) {
            FsStateChangelogWriter writer =
                    new FsStateChangelogWriter(
                            UUID.randomUUID(),
                            KeyGroupRange.of(KEY_GROUP, KEY_GROUP),
                            uploadScheduler,
                            appendPersistThreshold,
                            new SyncMailboxExecutor(),
                            taskChangelogRegistry,
                            TestLocalRecoveryConfig.disabled(),
                            LocalChangelogRegistry.NO_OP);

            SequenceNumber initialSqn = writer.initialSequenceNumber();

            writer.append(KEY_GROUP, getBytes(10)); // sqn: 0

            // checkpoint 1 trigger
            SequenceNumber checkpoint1sqn = writer.nextSequenceNumber();
            CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> future =
                    writer.persist(initialSqn, 1L);

            // trigger pre-emptive upload
            writer.append(KEY_GROUP, getBytes(100)); // sqn: 1

            uploadScheduler.scheduleAll(); // checkpoint 1 completed

            // close task before confirm checkpoint 1
            writer.truncateAndClose(checkpoint1sqn);

            SnapshotResult<ChangelogStateHandleStreamImpl> result = future.get();
            ChangelogStateHandleStreamImpl resultHandle = result.getJobManagerOwnedSnapshot();

            for (Tuple2<StreamStateHandle, Long> handleAndOffset :
                    resultHandle.getHandlesAndOffsets()) {
                assertThat(uploader.isDiscarded(handleAndOffset.f0))
                        .isFalse(); // all handles should not be discarded
            }
        }
    }

    @Test
    void testLocalFileDiscard() throws Exception {
        long appendPersistThreshold = 100;
        TaskChangelogRegistry taskChangelogRegistry =
                new TaskChangelogRegistryImpl(Executors.directExecutor());

        try (DiscardRecordableStateChangeUploader uploader =
                        new DiscardRecordableStateChangeUploader(taskChangelogRegistry);
                TestingBatchingUploadScheduler uploadScheduler =
                        new TestingBatchingUploadScheduler(uploader);
                FsStateChangelogWriter writer =
                        new FsStateChangelogWriter(
                                UUID.randomUUID(),
                                KeyGroupRange.of(KEY_GROUP, KEY_GROUP),
                                uploadScheduler,
                                appendPersistThreshold,
                                new SyncMailboxExecutor(),
                                taskChangelogRegistry,
                                TestLocalRecoveryConfig.enabledForTest(),
                                new LocalChangelogRegistryImpl(
                                        Executors.newDirectExecutorService()))) {
            SequenceNumber initialSqn = writer.initialSequenceNumber();

            writer.append(KEY_GROUP, getBytes(10));

            long checkpointId = 1L;
            // checkpoint 1 trigger
            SequenceNumber checkpoint1sqn = writer.nextSequenceNumber();
            writer.persist(initialSqn, checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 1 completed
            writer.confirm(initialSqn, checkpoint1sqn, checkpointId);

            // trigger pre-emptive upload
            writer.append(KEY_GROUP, getBytes(100));
            uploadScheduler.scheduleAll();
            writer.append(KEY_GROUP, getBytes(10));
            // checkpoint 2 trigger
            SequenceNumber checkpoint2sqn = writer.nextSequenceNumber();
            CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> future2 =
                    writer.persist(initialSqn, ++checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 2 completed
            writer.confirm(initialSqn, checkpoint2sqn, checkpointId);
            SnapshotResult<ChangelogStateHandleStreamImpl> result2 = future2.get();
            for (Tuple2<StreamStateHandle, Long> handleAndOffset :
                    result2.getTaskLocalSnapshot().getHandlesAndOffsets()) {
                assertThat(uploader.isDiscarded(handleAndOffset.f0)).isFalse();
            }

            // materialization 1 trigger
            SequenceNumber materializationSqn = writer.nextSequenceNumber();
            writer.append(KEY_GROUP, getBytes(10));

            // materialization 1 completed
            // checkpoint 3 trigger
            SequenceNumber checkpoint3sqn = writer.nextSequenceNumber();
            writer.persist(materializationSqn, ++checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 3 completed
            writer.confirm(materializationSqn, checkpoint3sqn, checkpointId);
            for (Tuple2<StreamStateHandle, Long> handleAndOffset :
                    result2.getTaskLocalSnapshot().getHandlesAndOffsets()) {
                assertThat(uploader.isDiscarded(handleAndOffset.f0)).isTrue();
            }
        }
    }

    @Test
    void testLocalFileAfterMaterialize() throws Exception {
        // If register local files when confirm(), the following case will fail:
        // cp1 trigger: file1,file1'(local)
        // JM: register [file1] to sharedRegistry
        // cp1 complete: stopTracking [file1], register [file1'] to localRegistry
        // cp2 trigger: file1,file1',file2,file2'
        // JM: register [file1,file2] to sharedRegistry
        // cp2 complete: stopTracking [file1,file1',file2,file2'], register [file1',file2'] to
        // localRegistry
        // cp1 subsume
        // cp3 trigger:  file1,file1',file2,file2',file3,file3'
        // materialization: uploaded.clear()
        // JM: register [file1,file2,file3] to sharedRegistry
        // cp3 complete: stopTracking [], register [] to localRegistry
        // cp2 subsume: [file1', file2'] are discarded
        // if restore from cp3: local file1',file2' are not found
        long appendPersistThreshold = 100;
        TaskChangelogRegistry taskChangelogRegistry =
                new TaskChangelogRegistryImpl(Executors.directExecutor());

        try (DiscardRecordableStateChangeUploader uploader =
                        new DiscardRecordableStateChangeUploader(taskChangelogRegistry);
                TestingBatchingUploadScheduler uploadScheduler =
                        new TestingBatchingUploadScheduler(uploader);
                FsStateChangelogWriter writer =
                        new FsStateChangelogWriter(
                                UUID.randomUUID(),
                                KeyGroupRange.of(KEY_GROUP, KEY_GROUP),
                                uploadScheduler,
                                appendPersistThreshold,
                                new SyncMailboxExecutor(),
                                taskChangelogRegistry,
                                TestLocalRecoveryConfig.enabledForTest(),
                                new LocalChangelogRegistryImpl(
                                        Executors.newDirectExecutorService()))) {
            SequenceNumber initialSqn = writer.initialSequenceNumber();

            writer.append(KEY_GROUP, getBytes(10));

            long checkpointId = 1L;
            // checkpoint 1 trigger
            SequenceNumber checkpoint1sqn = writer.nextSequenceNumber();
            writer.persist(initialSqn, checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 1 completed
            writer.confirm(initialSqn, checkpoint1sqn, checkpointId);

            writer.append(KEY_GROUP, getBytes(10));
            // checkpoint 2 trigger
            SequenceNumber checkpoint2sqn = writer.nextSequenceNumber();
            writer.persist(initialSqn, ++checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 2 completed
            writer.confirm(initialSqn, checkpoint2sqn, checkpointId);

            writer.append(KEY_GROUP, getBytes(10));
            // checkpoint 3 trigger
            SequenceNumber checkpoint3sqn = writer.nextSequenceNumber();
            CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> future3 =
                    writer.persist(initialSqn, ++checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 3 completed

            // materialization 1 trigger
            SequenceNumber materializationSqn = writer.nextSequenceNumber();
            writer.truncate(
                    materializationSqn.compareTo(checkpoint1sqn) < 0
                            ? materializationSqn
                            : checkpoint1sqn);
            // materialization 1 completed
            // checkpoint 3 confirm
            writer.confirm(materializationSqn, checkpoint3sqn, checkpointId);

            writer.append(KEY_GROUP, getBytes(10));

            SnapshotResult<ChangelogStateHandleStreamImpl> result3 = future3.get();

            for (Tuple2<StreamStateHandle, Long> handleAndOffset :
                    result3.getJobManagerOwnedSnapshot().getHandlesAndOffsets()) {
                assertThat(uploader.isDiscarded(handleAndOffset.f0)).isFalse();
            }

            for (Tuple2<StreamStateHandle, Long> handleAndOffset :
                    result3.getTaskLocalSnapshot().getHandlesAndOffsets()) {
                assertThat(uploader.isDiscarded(handleAndOffset.f0)).isFalse();
            }
        }
    }

    @Test
    void testLocalFileAbort() throws Exception {
        long appendPersistThreshold = 100;
        TaskChangelogRegistry taskChangelogRegistry =
                new TaskChangelogRegistryImpl(Executors.directExecutor());

        try (DiscardRecordableStateChangeUploader uploader =
                        new DiscardRecordableStateChangeUploader(taskChangelogRegistry);
                TestingBatchingUploadScheduler uploadScheduler =
                        new TestingBatchingUploadScheduler(uploader);
                FsStateChangelogWriter writer =
                        new FsStateChangelogWriter(
                                UUID.randomUUID(),
                                KeyGroupRange.of(KEY_GROUP, KEY_GROUP),
                                uploadScheduler,
                                appendPersistThreshold,
                                new SyncMailboxExecutor(),
                                taskChangelogRegistry,
                                TestLocalRecoveryConfig.enabledForTest(),
                                new LocalChangelogRegistryImpl(
                                        Executors.newDirectExecutorService()))) {
            SequenceNumber initialSqn = writer.initialSequenceNumber();

            writer.append(KEY_GROUP, getBytes(10));

            long checkpointId = 1L;
            // checkpoint 1 trigger
            SequenceNumber checkpoint1sqn = writer.nextSequenceNumber();
            writer.persist(initialSqn, checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 1 completed
            writer.confirm(initialSqn, checkpoint1sqn, checkpointId);

            writer.append(KEY_GROUP, getBytes(200));
            uploadScheduler.scheduleAll();
            writer.append(KEY_GROUP, getBytes(10));
            // checkpoint 2 trigger
            SequenceNumber checkpoint2sqn = writer.nextSequenceNumber();
            CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> future2 =
                    writer.persist(initialSqn, ++checkpointId);
            uploadScheduler.scheduleAll();

            // checkpoint 2 abort, all local dstl files are deleted
            writer.reset(initialSqn, checkpoint2sqn, checkpointId);
            SnapshotResult<ChangelogStateHandleStreamImpl> result2 = future2.get();
            assertThat(result2.getTaskLocalSnapshot().getHandlesAndOffsets())
                    .allMatch(tuple -> uploader.isDiscarded(tuple.f0));

            writer.append(KEY_GROUP, getBytes(10));
            // checkpoint 3 trigger
            SequenceNumber checkpoint3sqn = writer.nextSequenceNumber();
            CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> future3 =
                    writer.persist(initialSqn, ++checkpointId);
            uploadScheduler.scheduleAll(); // checkpoint 3 completed
            SnapshotResult<ChangelogStateHandleStreamImpl> result3 = future3.get();
            // checkpoint 3 confirm, delete files of checkpoint 1,2
            writer.confirm(initialSqn, checkpoint3sqn, checkpointId);
            assertThat(result3.getTaskLocalSnapshot().getHandlesAndOffsets())
                    .anyMatch(tuple -> !uploader.isDiscarded(tuple.f0));
        }
    }

    @Test
    void testNoReUploadBeforeCompletion() throws Exception {
        withWriter(
                (writer, uploader) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn = append(writer, bytes);
                    writer.persist(sqn, 1L);
                    uploader.reset();
                    writer.persist(sqn, 2L);
                    assertNoUpload(uploader, "no re-upload should happen");
                });
    }

    @Test
    void testPersistNewlyAppended() throws Exception {
        withWriter(
                (writer, uploader) -> {
                    SequenceNumber sqn = append(writer, getBytes());
                    writer.persist(sqn, 1L);
                    uploader.reset();
                    byte[] bytes = getBytes();
                    sqn = append(writer, bytes);
                    writer.persist(sqn, 2L);
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
                    writer.persist(sqn, 1L);
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
                                                    future = writer.persist(sqn, 1L);
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
                                            writer.persist(sqn, 1L); // future result ignored
                                            uploader.failUpload(new RuntimeException("test"));
                                            writer.persist(sqn, 2L); // should fail right away
                                        }))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testPersistNonFailedChanges() throws Exception {
        withWriter(
                (writer, uploader) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn1 = append(writer, bytes);
                    writer.persist(sqn1, 1L); // future result ignored
                    uploader.failUpload(new RuntimeException("test"));
                    uploader.reset();
                    SequenceNumber sqn2 = append(writer, bytes);
                    CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> future =
                            writer.persist(sqn2, 2L);
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
                                            writer.persist(sqn, 1L);
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
