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

package org.apache.flink.connector.file.sink.writer;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.utils.NoOpCommitter;
import org.apache.flink.connector.file.sink.utils.NoOpRecoverable;
import org.apache.flink.connector.file.sink.utils.NoOpRecoverableFsDataOutputStream;
import org.apache.flink.connector.file.sink.utils.NoOpRecoverableWriter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.fs.local.LocalRecoverableWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/** Tests for {@link FileWriterBucket}. */
public class FileWriterBucketTest {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Test
    public void testOnCheckpointNoPendingRecoverable() throws IOException {
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());

        TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);

        FileWriterBucket<String> bucket =
                createBucket(
                        recoverableWriter,
                        path,
                        DEFAULT_ROLLING_POLICY,
                        OutputFileConfig.builder().build());
        bucket.write("test-element", 0);
        List<FileSinkCommittable> fileSinkCommittables = bucket.prepareCommit(false);
        FileWriterBucketState bucketState = bucket.snapshotState();

        compareNumberOfPendingAndInProgress(fileSinkCommittables, 0, 0);
        assertEquals(BUCKET_ID, bucketState.getBucketId());
        assertEquals(path, bucketState.getBucketPath());
        assertNotNull(
                "The bucket should have in-progress recoverable",
                bucketState.getInProgressFileRecoverable());
    }

    @Test
    public void testOnCheckpointRollingOnCheckpoint() throws IOException {
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());

        TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);

        FileWriterBucket<String> bucket =
                createBucket(
                        recoverableWriter,
                        path,
                        ON_CHECKPOING_ROLLING_POLICY,
                        OutputFileConfig.builder().build());
        bucket.write("test-element", 0);
        List<FileSinkCommittable> fileSinkCommittables = bucket.prepareCommit(false);
        FileWriterBucketState bucketState = bucket.snapshotState();

        compareNumberOfPendingAndInProgress(fileSinkCommittables, 1, 0);
        assertEquals(BUCKET_ID, bucketState.getBucketId());
        assertEquals(path, bucketState.getBucketPath());
        assertNull(
                "The bucket should not have in-progress recoverable",
                bucketState.getInProgressFileRecoverable());
    }

    @Test
    public void testOnCheckpointMultiplePendingFiles() throws IOException {
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());

        TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);

        FileWriterBucket<String> bucket =
                createBucket(
                        recoverableWriter,
                        path,
                        EACH_ELEMENT_ROLLING_POLICY,
                        OutputFileConfig.builder().build());
        bucket.write("test-element", 0);
        bucket.write("test-element", 0);
        bucket.write("test-element", 0);
        List<FileSinkCommittable> fileSinkCommittables = bucket.prepareCommit(false);
        FileWriterBucketState bucketState = bucket.snapshotState();

        // The last element would not roll
        compareNumberOfPendingAndInProgress(fileSinkCommittables, 2, 0);
        assertEquals(BUCKET_ID, bucketState.getBucketId());
        assertEquals(path, bucketState.getBucketPath());
        assertNotNull(
                "The bucket should not have in-progress recoverable",
                bucketState.getInProgressFileRecoverable());
    }

    @Test
    public void testOnCheckpointWithInProgressFileToCleanup() throws IOException {
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());

        TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);

        FileWriterBucket<String> bucket =
                createBucket(
                        recoverableWriter,
                        path,
                        DEFAULT_ROLLING_POLICY,
                        OutputFileConfig.builder().build());
        bucket.write("test-element", 0);

        bucket.prepareCommit(false);
        bucket.snapshotState();

        // One more checkpoint
        bucket.write("test-element", 0);
        List<FileSinkCommittable> fileSinkCommittables = bucket.prepareCommit(false);
        FileWriterBucketState bucketState = bucket.snapshotState();

        compareNumberOfPendingAndInProgress(fileSinkCommittables, 0, 1);
        assertEquals(BUCKET_ID, bucketState.getBucketId());
        assertEquals(path, bucketState.getBucketPath());
        assertNotNull(
                "The bucket should not have in-progress recoverable",
                bucketState.getInProgressFileRecoverable());
    }

    @Test
    public void testFlush() throws IOException {
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());

        TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);

        FileWriterBucket<String> bucket =
                createBucket(
                        recoverableWriter,
                        path,
                        DEFAULT_ROLLING_POLICY,
                        OutputFileConfig.builder().build());
        bucket.write("test-element", 0);

        List<FileSinkCommittable> fileSinkCommittables = bucket.prepareCommit(true);

        compareNumberOfPendingAndInProgress(fileSinkCommittables, 1, 0);
        assertNull(
                "The bucket should not have in-progress part after flushed",
                bucket.getInProgressPart());
    }

    @Test
    public void testRollingOnProcessingTime() throws IOException {
        File outDir = TEMP_FOLDER.newFolder();
        Path path = new Path(outDir.toURI());

        RollingPolicy<String, String> onProcessingTimeRollingPolicy =
                DefaultRollingPolicy.builder().withRolloverInterval(10).build();

        TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);
        FileWriterBucket<String> bucket =
                createBucket(
                        recoverableWriter,
                        path,
                        onProcessingTimeRollingPolicy,
                        OutputFileConfig.builder().build());
        bucket.write("test-element", 11);
        bucket.write("test-element", 12);

        bucket.onProcessingTime(20);
        assertNotNull(
                "The bucket should not roll since interval is not reached",
                bucket.getInProgressPart());

        bucket.write("test-element", 21);
        bucket.onProcessingTime(21);
        assertNull("The bucket should roll since interval is reached", bucket.getInProgressPart());
        List<FileSinkCommittable> fileSinkCommittables = bucket.prepareCommit(false);
        compareNumberOfPendingAndInProgress(fileSinkCommittables, 1, 0);
    }

    // --------------------------- Checking Restore ---------------------------
    @Test
    public void testRestoreWithInprogressFileNotSupportResume() throws IOException {
        StubNonResumableWriter nonResumableWriter = new StubNonResumableWriter();
        FileWriterBucket<String> bucket =
                getRestoredBucketWithOnlyInProgressPart(nonResumableWriter, DEFAULT_ROLLING_POLICY);
        assertNull("The in-progress file should be pre-committed", bucket.getInProgressPart());

        List<FileSinkCommittable> fileSinkCommittables = bucket.prepareCommit(false);
        FileWriterBucketState bucketState = bucket.snapshotState();
        compareNumberOfPendingAndInProgress(fileSinkCommittables, 1, 0);
        assertNull(
                "The bucket should not have in-progress recoverable",
                bucketState.getInProgressFileRecoverable());
    }

    @Test
    public void testRestoreWithInprogressFileSupportResume() throws IOException {
        StubResumableWriter resumableWriter = new StubResumableWriter();
        FileWriterBucket<String> bucket =
                getRestoredBucketWithOnlyInProgressPart(resumableWriter, DEFAULT_ROLLING_POLICY);
        assertNotNull("The in-progress file should be recovered", bucket.getInProgressPart());

        List<FileSinkCommittable> fileSinkCommittables = bucket.prepareCommit(false);
        FileWriterBucketState bucketState = bucket.snapshotState();
        compareNumberOfPendingAndInProgress(fileSinkCommittables, 0, 0);
        assertNotNull(
                "The bucket should have in-progress recoverable",
                bucketState.getInProgressFileRecoverable());
    }

    /**
     * Tests restoring with state containing pending files. This might happen if we are migrating
     * from {@code StreamingFileSink}.
     */
    @Test
    public void testRestoringWithOnlyPendingFiles() throws IOException {
        final int noOfPendingFileCheckpoints = 4;

        StubResumableWriter resumableWriter = new StubResumableWriter();
        FileWriterBucket<String> bucket =
                getRestoredBucketWithOnlyPendingFiles(
                        resumableWriter, DEFAULT_ROLLING_POLICY, noOfPendingFileCheckpoints);
        assertNull("There should be no in-progress file", bucket.getInProgressPart());
        // There is one pending file for each checkpoint
        assertEquals(noOfPendingFileCheckpoints, bucket.getPendingFiles().size());

        List<FileSinkCommittable> fileSinkCommittables = bucket.prepareCommit(false);
        bucket.snapshotState();
        compareNumberOfPendingAndInProgress(fileSinkCommittables, noOfPendingFileCheckpoints, 0);
    }

    @Test
    public void testMergeWithInprogressFileNotSupportResume() throws IOException {
        FileWriterBucket<String> bucket1 =
                getRestoredBucketWithOnlyInProgressPart(
                        new StubNonResumableWriter(), DEFAULT_ROLLING_POLICY);
        FileWriterBucket<String> bucket2 =
                getRestoredBucketWithOnlyInProgressPart(
                        new StubNonResumableWriter(), DEFAULT_ROLLING_POLICY);
        bucket1.merge(bucket2);
        assertNull("The in-progress file should be pre-committed", bucket1.getInProgressPart());

        List<FileSinkCommittable> fileSinkCommittables = bucket1.prepareCommit(false);
        FileWriterBucketState bucketState = bucket1.snapshotState();
        compareNumberOfPendingAndInProgress(fileSinkCommittables, 2, 0);
        assertNull(
                "The bucket should have in-progress recoverable",
                bucketState.getInProgressFileRecoverable());
    }

    @Test
    public void testMergeWithInprogressFileSupportResume() throws IOException {
        FileWriterBucket<String> bucket1 =
                getRestoredBucketWithOnlyInProgressPart(
                        new StubResumableWriter(), DEFAULT_ROLLING_POLICY);
        FileWriterBucket<String> bucket2 =
                getRestoredBucketWithOnlyInProgressPart(
                        new StubResumableWriter(), DEFAULT_ROLLING_POLICY);
        bucket1.merge(bucket2);
        assertNotNull("The in-progress file should be recovered", bucket1.getInProgressPart());

        List<FileSinkCommittable> fileSinkCommittables = bucket1.prepareCommit(false);
        FileWriterBucketState bucketState = bucket1.snapshotState();
        compareNumberOfPendingAndInProgress(fileSinkCommittables, 1, 0);
        assertNotNull(
                "The bucket should not have in-progress recoverable",
                bucketState.getInProgressFileRecoverable());
    }

    // ------------------------------- Mock Classes --------------------------------

    private static class TestRecoverableWriter extends LocalRecoverableWriter {

        private int cleanupCallCounter = 0;

        TestRecoverableWriter(LocalFileSystem fs) {
            super(fs);
        }

        int getCleanupCallCounter() {
            return cleanupCallCounter;
        }

        @Override
        public boolean requiresCleanupOfRecoverableState() {
            // here we return true so that the cleanupRecoverableState() is called.
            return true;
        }

        @Override
        public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
            cleanupCallCounter++;
            return false;
        }

        @Override
        public String toString() {
            return "TestRecoverableWriter has called discardRecoverableState() "
                    + cleanupCallCounter
                    + " times.";
        }
    }

    /**
     * A test implementation of a {@link RecoverableWriter} that does not support resuming, i.e.
     * keep on writing to the in-progress file at the point we were before the failure.
     */
    private static class StubResumableWriter extends BaseStubWriter {

        StubResumableWriter() {
            super(true);
        }
    }

    /**
     * A test implementation of a {@link RecoverableWriter} that does not support resuming, i.e.
     * keep on writing to the in-progress file at the point we were before the failure.
     */
    private static class StubNonResumableWriter extends BaseStubWriter {

        StubNonResumableWriter() {
            super(false);
        }
    }

    /**
     * A test implementation of a {@link RecoverableWriter} that does not support resuming, i.e.
     * keep on writing to the in-progress file at the point we were before the failure.
     */
    private static class BaseStubWriter extends NoOpRecoverableWriter {

        private final boolean supportsResume;

        private BaseStubWriter(final boolean supportsResume) {
            this.supportsResume = supportsResume;
        }

        @Override
        public RecoverableFsDataOutputStream recover(RecoverableWriter.ResumeRecoverable resumable)
                throws IOException {
            return new NoOpRecoverableFsDataOutputStream() {
                @Override
                public RecoverableFsDataOutputStream.Committer closeForCommit() throws IOException {
                    return new NoOpCommitter();
                }
            };
        }

        @Override
        public RecoverableFsDataOutputStream.Committer recoverForCommit(
                RecoverableWriter.CommitRecoverable resumable) throws IOException {
            checkArgument(resumable instanceof NoOpRecoverable);
            return new NoOpCommitter();
        }

        @Override
        public boolean supportsResume() {
            return supportsResume;
        }
    }

    private static class EachElementRollingPolicy implements RollingPolicy<String, String> {

        @Override
        public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState)
                throws IOException {
            return false;
        }

        @Override
        public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, String element)
                throws IOException {
            return true;
        }

        @Override
        public boolean shouldRollOnProcessingTime(
                PartFileInfo<String> partFileState, long currentTime) throws IOException {
            return false;
        }
    }

    // ------------------------------- Utility Methods --------------------------------

    private static final String BUCKET_ID = "testing-bucket";

    private static final Encoder<String> ENCODER = new SimpleStringEncoder<>();

    private static final RollingPolicy<String, String> DEFAULT_ROLLING_POLICY =
            DefaultRollingPolicy.builder().build();

    private static final RollingPolicy<String, String> ON_CHECKPOING_ROLLING_POLICY =
            OnCheckpointRollingPolicy.build();

    private static final EachElementRollingPolicy EACH_ELEMENT_ROLLING_POLICY =
            new EachElementRollingPolicy();

    private static FileWriterBucket<String> createBucket(
            RecoverableWriter writer,
            Path bucketPath,
            RollingPolicy<String, String> rollingPolicy,
            OutputFileConfig outputFileConfig) {

        return FileWriterBucket.getNew(
                BUCKET_ID,
                bucketPath,
                new RowWiseBucketWriter<>(writer, ENCODER),
                rollingPolicy,
                outputFileConfig);
    }

    private static TestRecoverableWriter getRecoverableWriter(Path path) {
        try {
            final FileSystem fs = FileSystem.get(path.toUri());
            if (!(fs instanceof LocalFileSystem)) {
                fail(
                        "Expected Local FS but got a "
                                + fs.getClass().getName()
                                + " for path: "
                                + path);
            }
            return new TestRecoverableWriter((LocalFileSystem) fs);
        } catch (IOException e) {
            fail();
        }
        return null;
    }

    private void compareNumberOfPendingAndInProgress(
            List<FileSinkCommittable> fileSinkCommittables,
            int expectedPendingFiles,
            int expectedInProgressFiles) {
        int numPendingFiles = 0;
        int numInProgressFiles = 0;

        for (FileSinkCommittable committable : fileSinkCommittables) {
            if (committable.getPendingFile() != null) {
                numPendingFiles++;
            }

            if (committable.getInProgressFileToCleanup() != null) {
                numInProgressFiles++;
            }
        }

        assertEquals(expectedPendingFiles, numPendingFiles);
        assertEquals(expectedInProgressFiles, numInProgressFiles);
    }

    private FileWriterBucket<String> getRestoredBucketWithOnlyInProgressPart(
            BaseStubWriter writer, RollingPolicy<String, String> rollingPolicy) throws IOException {
        FileWriterBucketState stateWithOnlyInProgressFile =
                new FileWriterBucketState(
                        "test",
                        new Path("file:///fake/fakefile"),
                        12345L,
                        new OutputStreamBasedPartFileWriter
                                .OutputStreamBasedInProgressFileRecoverable(new NoOpRecoverable()));

        return FileWriterBucket.restore(
                new RowWiseBucketWriter<>(writer, ENCODER),
                rollingPolicy,
                stateWithOnlyInProgressFile,
                OutputFileConfig.builder().build());
    }

    private FileWriterBucket<String> getRestoredBucketWithOnlyPendingFiles(
            BaseStubWriter writer,
            RollingPolicy<String, String> rollingPolicy,
            int numberOfPendingParts)
            throws IOException {

        Map<Long, List<InProgressFileWriter.PendingFileRecoverable>> completePartsPerCheckpoint =
                createPendingPartsPerCheckpoint(numberOfPendingParts);

        FileWriterBucketState state =
                new FileWriterBucketState(
                        "test",
                        new Path("file:///fake/fakefile"),
                        12345L,
                        null,
                        completePartsPerCheckpoint);

        return FileWriterBucket.restore(
                new RowWiseBucketWriter<>(writer, ENCODER),
                rollingPolicy,
                state,
                OutputFileConfig.builder().build());
    }

    private Map<Long, List<InProgressFileWriter.PendingFileRecoverable>>
            createPendingPartsPerCheckpoint(int noOfCheckpoints) {
        final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>>
                pendingCommittablesPerCheckpoint = new HashMap<>();
        for (int checkpointId = 0; checkpointId < noOfCheckpoints; checkpointId++) {
            final List<InProgressFileWriter.PendingFileRecoverable> pending = new ArrayList<>();
            pending.add(
                    new OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable(
                            new NoOpRecoverable()));
            pendingCommittablesPerCheckpoint.put((long) checkpointId, pending);
        }
        return pendingCommittablesPerCheckpoint;
    }
}
