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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManagerBuilder;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingType;
import org.apache.flink.runtime.checkpoint.filemerging.SegmentFileStateHandle;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.flink.core.fs.Path.fromLocalFile;
import static org.apache.flink.core.fs.local.LocalFileSystem.getSharedInstance;
import static org.apache.flink.runtime.state.CheckpointedStateScope.EXCLUSIVE;
import static org.apache.flink.runtime.state.CheckpointedStateScope.SHARED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link FsMergingCheckpointStorageLocation}. */
public class FsMergingCheckpointStorageLocationTest {
    @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

    public static Path checkpointBaseDir;

    public static Path sharedStateDir;
    public static Path taskOwnedStateDir;

    private final Random random = new Random();

    private static final String SNAPSHOT_MGR_ID = "snapshotMgrId";
    private static final int FILE_STATE_SIZE_THRESHOLD = 1024;
    private static final int WRITE_BUFFER_SIZE = 1024;

    private static final FileMergingSnapshotManager.SubtaskKey SUBTASK_KEY =
            new FileMergingSnapshotManager.SubtaskKey("opId", 1, 1);

    @Before
    public void prepareDirectories() {
        checkpointBaseDir = new Path(tmpFolder.toString());
        sharedStateDir =
                new Path(
                        checkpointBaseDir,
                        AbstractFsCheckpointStorageAccess.CHECKPOINT_SHARED_STATE_DIR);
        taskOwnedStateDir =
                new Path(
                        checkpointBaseDir,
                        AbstractFsCheckpointStorageAccess.CHECKPOINT_TASK_OWNED_STATE_DIR);
    }

    @Test
    public void testWriteMultipleStateFilesWithinCheckpoint() throws Exception {
        testWriteMultipleStateFiles();
    }

    private void testWriteMultipleStateFiles() throws Exception {
        FileMergingSnapshotManager snapshotManager = createFileMergingSnapshotManager();
        long checkpointID = 1;
        FsMergingCheckpointStorageLocation storageLocation =
                createFsMergingCheckpointStorageLocation(checkpointID, snapshotManager);
        FileSystem fs = storageLocation.getFileSystem();

        assertThat(fs.exists(snapshotManager.getManagedDir(SUBTASK_KEY, EXCLUSIVE))).isTrue();
        assertThat(fs.exists(snapshotManager.getManagedDir(SUBTASK_KEY, SHARED))).isTrue();

        int numStates = 3;
        List<byte[]> states = generateRandomByteStates(numStates, 2, 16);
        List<SegmentFileStateHandle> stateHandles = new ArrayList<>(numStates);
        for (byte[] s : states) {
            SegmentFileStateHandle segmentFileStateHandle =
                    uploadOneStateFileAndGetStateHandle(checkpointID, storageLocation, s);
            stateHandles.add(segmentFileStateHandle);
        }

        //        snapshotManager.notifyCheckpointComplete(checkpointID);

        // verify there is only one physical file
        verifyStateHandlesAllPointToTheSameFile(stateHandles);
    }

    @Test
    public void testCheckpointStreamClosedExceptionally() throws Exception {
        try (FileMergingSnapshotManager snapshotManager = createFileMergingSnapshotManager()) {
            Path filePath1 = null;
            try (FileMergingCheckpointStateOutputStream stream1 =
                    snapshotManager.createCheckpointStateOutputStream(SUBTASK_KEY, 1, EXCLUSIVE)) {
                stream1.flushToFile();
                filePath1 = stream1.getFilePath();
                assertPathNotNullAndCheckExistence(filePath1, true);
                throw new IOException();
            } catch (IOException ignored) {
            }
            assertPathNotNullAndCheckExistence(filePath1, false);
        }
    }

    private void assertPathNotNullAndCheckExistence(Path path, boolean exist) throws IOException {
        assertThat(path).isNotNull();
        assertThat(path.getFileSystem().exists(path)).isEqualTo(exist);
    }

    @Test
    public void testWritingToClosedStream() {
        FileMergingSnapshotManager snapshotManager = createFileMergingSnapshotManager();
        FsMergingCheckpointStorageLocation storageLocation =
                createFsMergingCheckpointStorageLocation(1, snapshotManager);
        try (FileMergingCheckpointStateOutputStream stream =
                storageLocation.createCheckpointStateOutputStream(EXCLUSIVE)) {
            stream.flushToFile();
            stream.closeAndGetHandle();
            stream.flushToFile();
            fail("Expected IOException");
        } catch (IOException e) {
            assertThat(e.getMessage()).isEqualTo("Cannot call flushToFile() to a closed stream.");
        }
    }

    @Test
    public void testWriteAndReadPositionInformation() throws Exception {
        long maxFileSize = 128;
        FileMergingSnapshotManager snapshotManager = createFileMergingSnapshotManager(maxFileSize);
        FsMergingCheckpointStorageLocation storageLocation1 =
                createFsMergingCheckpointStorageLocation(1, snapshotManager);

        // ------------------- Checkpoint: 1 -------------------
        int stateSize1 = 10;
        List<byte[]> cp1States = generateRandomByteStates(1, stateSize1, stateSize1);
        uploadCheckpointStates(1, cp1States, storageLocation1);
        //        snapshotManager.notifyCheckpointComplete(1);

        // ------------------- Checkpoint: 2-9 -------------------
        for (int checkpointId = 2; checkpointId < 10; checkpointId++) {
            testWriteAndReadPositionInformationInCheckpoint(
                    checkpointId, maxFileSize, snapshotManager);
        }
    }

    private void testWriteAndReadPositionInformationInCheckpoint(
            long checkpointId, long maxFileSize, FileMergingSnapshotManager snapshotManager)
            throws IOException {
        FsMergingCheckpointStorageLocation storageLocation =
                createFsMergingCheckpointStorageLocation(checkpointId, snapshotManager);
        // test whether the input and output streams perform position-related operations correctly
        try (FileMergingCheckpointStateOutputStream stateOutputStream =
                storageLocation.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED)) {

            // 1. Write some bytes to the file.
            int stateSize = 64;
            byte[] expectedBytes = new byte[10];
            byte[] stateValues = generateRandomBytes(stateSize);
            stateOutputStream.write(stateValues);

            // 2. Write some positions, which should be relative values in the file segments.
            //    Each of them points to a previously written byte in the file.
            for (int i = 0; i < 10; i++) {
                int position = random.nextInt(stateSize);
                byte[] positionBytes = longToBytes(position);
                expectedBytes[i] = stateValues[position];
                stateOutputStream.write(positionBytes);
            }
            SegmentFileStateHandle cpStateHandle = stateOutputStream.closeAndGetHandle();
            assertThat(cpStateHandle).isNotNull();

            // 3. Read from the file.
            //    It repeatedly reads a position value -> seek to the corresponding position to read
            //    the expected bytes -> seek back to read the next position value.
            byte[] actualBytes = new byte[10];
            byte[] oneByte = new byte[1];
            FSDataInputStream inputStream = cpStateHandle.openInputStream();
            assertThat(inputStream).isNotNull();
            inputStream.seek(stateSize);
            for (int i = 0; i < 10; i++) {
                byte[] longBytes = new byte[8];
                int readContent = inputStream.read(longBytes);
                assertThat(readContent).isEqualTo(8);
                long curPos = inputStream.getPos();
                inputStream.seek(bytesToLong(longBytes));
                assertThat(inputStream.read(oneByte) >= 0).isTrue();
                actualBytes[i] = oneByte[0];
                inputStream.seek(curPos);
            }
            assertThat(actualBytes).isEqualTo(expectedBytes);
        }
    }

    private FileMergingSnapshotManager createFileMergingSnapshotManager() {
        return createFileMergingSnapshotManager(-1);
    }

    private FileMergingSnapshotManager createFileMergingSnapshotManager(long maxFileSize) {
        FileMergingSnapshotManager mgr =
                new FileMergingSnapshotManagerBuilder(
                                SNAPSHOT_MGR_ID, FileMergingType.MERGE_WITHIN_CHECKPOINT)
                        .build();

        mgr.initFileSystem(
                getSharedInstance(),
                checkpointBaseDir,
                sharedStateDir,
                taskOwnedStateDir,
                WRITE_BUFFER_SIZE);

        mgr.registerSubtaskForSharedStates(SUBTASK_KEY);
        return mgr;
    }

    public FsMergingCheckpointStorageLocation createFsMergingCheckpointStorageLocation(
            long checkpointId, @Nonnull FileMergingSnapshotManager snapshotManager) {
        LocalFileSystem fs = getSharedInstance();
        CheckpointStorageLocationReference cslReference =
                AbstractFsCheckpointStorageAccess.encodePathAsReference(
                        fromLocalFile(fs.pathToFile(checkpointBaseDir)));
        assertThat(snapshotManager).isNotNull();

        return new FsMergingCheckpointStorageLocation(
                SUBTASK_KEY,
                getSharedInstance(),
                checkpointBaseDir,
                sharedStateDir,
                taskOwnedStateDir,
                cslReference,
                FILE_STATE_SIZE_THRESHOLD,
                WRITE_BUFFER_SIZE,
                snapshotManager,
                checkpointId);
    }

    private SegmentFileStateHandle uploadOneStateFileAndGetStateHandle(
            long checkpointID,
            FsMergingCheckpointStorageLocation storageLocation,
            byte[] stateContent)
            throws IOException {

        // upload a (logical) state file
        try (FileMergingCheckpointStateOutputStream stateOutputStream =
                storageLocation.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED)) {
            stateOutputStream.write(stateContent);
            return stateOutputStream.closeAndGetHandle();
        }
    }

    private boolean bytesEqual(byte[] bytes1, byte[] bytes2) {
        if (bytes1 == null || bytes2 == null) {
            return false;
        }

        if (bytes1.length == bytes2.length) {
            for (int i = 0; i < bytes1.length; i++) {
                if (bytes1[i] != bytes2[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private List<SegmentFileStateHandle> uploadCheckpointStates(
            long checkpointID,
            List<byte[]> states,
            FsMergingCheckpointStorageLocation storageLocation)
            throws IOException {
        List<SegmentFileStateHandle> stateHandles = new ArrayList<>(states.size());
        for (byte[] state : states) {
            SegmentFileStateHandle segmentFileStateHandle =
                    uploadOneStateFileAndGetStateHandle(checkpointID, storageLocation, state);
            stateHandles.add(segmentFileStateHandle);
        }
        return stateHandles;
    }

    private byte[] generateRandomBytes(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    private List<byte[]> generateRandomByteStates(
            int numStates, int perStateMinSize, int perStateMaxSize) {
        List<byte[]> result = new ArrayList<>(numStates);
        for (int i = 0; i < numStates; i++) {
            int stateSize = random.nextInt(perStateMaxSize - perStateMinSize + 1) + perStateMinSize;
            result.add(generateRandomBytes(stateSize));
        }
        return result;
    }

    private void verifyStateHandlesAllPointToTheSameFile(
            List<SegmentFileStateHandle> stateHandles) {
        Path lastFilePath = null;
        for (SegmentFileStateHandle stateHandle : stateHandles) {
            assertThat(lastFilePath == null || lastFilePath.equals(stateHandle.getFilePath()))
                    .isTrue();
            lastFilePath = stateHandle.getFilePath();
        }
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }
}
