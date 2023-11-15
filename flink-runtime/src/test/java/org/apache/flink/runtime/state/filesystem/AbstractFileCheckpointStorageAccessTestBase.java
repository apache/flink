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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorageAccess;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test base for file-system-based checkoint storage, such as the {@link
 * MemoryBackendCheckpointStorageAccess} and the {@link FsCheckpointStorageAccess}.
 */
public abstract class AbstractFileCheckpointStorageAccessTestBase {

    @TempDir protected java.nio.file.Path tmp;

    // ------------------------------------------------------------------------
    //  factories for the actual state storage to be tested
    // ------------------------------------------------------------------------

    protected abstract CheckpointStorageAccess createCheckpointStorage(Path checkpointDir)
            throws Exception;

    protected abstract CheckpointStorageAccess createCheckpointStorageWithSavepointDir(
            Path checkpointDir, Path savepointDir) throws Exception;

    // ------------------------------------------------------------------------
    //  pointers
    // ------------------------------------------------------------------------

    @Test
    void testPointerPathResolution() throws Exception {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        final Path metadataFile =
                new Path(
                        Path.fromLocalFile(TempDirUtils.newFolder(tmp)),
                        AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME);

        final String basePointer = metadataFile.getParent().toString();

        final String pointer1 = metadataFile.toString();
        final String pointer2 = metadataFile.getParent().toString();
        final String pointer3 = metadataFile.getParent().toString() + '/';

        // create the storage for some random checkpoint directory
        final CheckpointStorageAccess storage = createCheckpointStorage(randomTempPath());

        final byte[] data = new byte[23686];
        new Random().nextBytes(data);
        try (FSDataOutputStream out = fs.create(metadataFile, WriteMode.NO_OVERWRITE)) {
            out.write(data);
        }

        CompletedCheckpointStorageLocation completed1 = storage.resolveCheckpoint(pointer1);
        CompletedCheckpointStorageLocation completed2 = storage.resolveCheckpoint(pointer2);
        CompletedCheckpointStorageLocation completed3 = storage.resolveCheckpoint(pointer3);

        assertThat(completed1.getExternalPointer()).isEqualTo(basePointer);
        assertThat(completed2.getExternalPointer()).isEqualTo(basePointer);
        assertThat(completed3.getExternalPointer()).isEqualTo(basePointer);

        StreamStateHandle handle1 = completed1.getMetadataHandle();
        StreamStateHandle handle2 = completed2.getMetadataHandle();
        StreamStateHandle handle3 = completed3.getMetadataHandle();

        assertThat(handle1).isNotNull();
        assertThat(handle2).isNotNull();
        assertThat(handle3).isNotNull();

        validateContents(handle1, data);
        validateContents(handle2, data);
        validateContents(handle3, data);
    }

    @Test
    void testFailingPointerPathResolution() throws Exception {
        // create the storage for some random checkpoint directory
        final CheckpointStorageAccess storage = createCheckpointStorage(randomTempPath());

        // null value
        assertThatThrownBy(() -> storage.resolveCheckpoint(null))
                .isInstanceOf(NullPointerException.class);

        // empty string
        assertThatThrownBy(() -> storage.resolveCheckpoint(""))
                .isInstanceOf(IllegalArgumentException.class);

        // not a file path at all
        assertThatThrownBy(() -> storage.resolveCheckpoint("this-is_not/a#filepath.at.all"))
                .isInstanceOf(IOException.class);

        // non-existing file
        assertThatThrownBy(
                        () ->
                                storage.resolveCheckpoint(
                                        TempDirUtils.newFile(tmp).toURI().toString()
                                                + "_not_existing"))
                .isInstanceOf(IOException.class);
    }

    // ------------------------------------------------------------------------
    //  checkpoints
    // ------------------------------------------------------------------------

    /**
     * Validates that multiple checkpoints from different jobs with the same checkpoint ID do not
     * interfere with each other.
     */
    @Test
    void testPersistMultipleMetadataOnlyCheckpoints() throws Exception {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        final Path checkpointDir = new Path(TempDirUtils.newFolder(tmp).toURI());

        final long checkpointId = 177;

        final CheckpointStorageAccess storage1 = createCheckpointStorage(checkpointDir);
        storage1.initializeBaseLocationsForCheckpoint();
        final CheckpointStorageAccess storage2 = createCheckpointStorage(checkpointDir);
        storage2.initializeBaseLocationsForCheckpoint();

        final CheckpointStorageLocation loc1 =
                storage1.initializeLocationForCheckpoint(checkpointId);
        final CheckpointStorageLocation loc2 =
                storage2.initializeLocationForCheckpoint(checkpointId);

        final byte[] data1 = {77, 66, 55, 99, 88};
        final byte[] data2 = {1, 3, 2, 5, 4};

        final CompletedCheckpointStorageLocation completedLocation1;
        try (CheckpointMetadataOutputStream out = loc1.createMetadataOutputStream()) {
            out.write(data1);
            completedLocation1 = out.closeAndFinalizeCheckpoint();
        }
        final String result1 = completedLocation1.getExternalPointer();

        final CompletedCheckpointStorageLocation completedLocation2;
        try (CheckpointMetadataOutputStream out = loc2.createMetadataOutputStream()) {
            out.write(data2);
            completedLocation2 = out.closeAndFinalizeCheckpoint();
        }
        final String result2 = completedLocation2.getExternalPointer();

        // check that this went to a file, but in a nested directory structure

        // one directory per storage
        FileStatus[] files = fs.listStatus(checkpointDir);
        assertThat(files).hasSize(2);

        // in each per-storage directory, one for the checkpoint
        FileStatus[] job1Files = fs.listStatus(files[0].getPath());
        FileStatus[] job2Files = fs.listStatus(files[1].getPath());
        assertThat(job1Files).hasSizeGreaterThanOrEqualTo(1);
        assertThat(job2Files).hasSizeGreaterThanOrEqualTo(1);

        assertThat(
                        fs.exists(
                                new Path(
                                        result1,
                                        AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME)))
                .isTrue();
        assertThat(
                        fs.exists(
                                new Path(
                                        result2,
                                        AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME)))
                .isTrue();

        // check that both storages can resolve each others contents
        validateContents(storage1.resolveCheckpoint(result1).getMetadataHandle(), data1);
        validateContents(storage1.resolveCheckpoint(result2).getMetadataHandle(), data2);
        validateContents(storage2.resolveCheckpoint(result1).getMetadataHandle(), data1);
        validateContents(storage2.resolveCheckpoint(result2).getMetadataHandle(), data2);
    }

    @Test
    void writeToAlreadyExistingCheckpointFails() throws Exception {
        final byte[] data = {8, 8, 4, 5, 2, 6, 3};
        final long checkpointId = 177;

        final CheckpointStorageAccess storage = createCheckpointStorage(randomTempPath());
        storage.initializeBaseLocationsForCheckpoint();
        final CheckpointStorageLocation loc = storage.initializeLocationForCheckpoint(checkpointId);

        // write to the metadata file for the checkpoint

        try (CheckpointMetadataOutputStream out = loc.createMetadataOutputStream()) {
            out.write(data);
            out.closeAndFinalizeCheckpoint();
        }

        // create another writer to the metadata file for the checkpoint
        assertThatThrownBy(loc::createMetadataOutputStream).isInstanceOf(IOException.class);
    }

    // ------------------------------------------------------------------------
    //  savepoints
    // ------------------------------------------------------------------------

    @Test
    void testSavepointPathConfiguredAndTarget() throws Exception {
        final Path savepointDir = randomTempPath();
        final Path customDir = randomTempPath();
        testSavepoint(savepointDir, customDir, customDir);
    }

    @Test
    void testSavepointPathConfiguredNoTarget() throws Exception {
        final Path savepointDir = randomTempPath();
        testSavepoint(savepointDir, null, savepointDir);
    }

    @Test
    void testNoSavepointPathConfiguredAndTarget() throws Exception {
        final Path customDir = Path.fromLocalFile(TempDirUtils.newFolder(tmp));
        testSavepoint(null, customDir, customDir);
    }

    @Test
    void testNoSavepointPathConfiguredNoTarget() throws Exception {
        final CheckpointStorageAccess storage = createCheckpointStorage(randomTempPath());

        assertThatThrownBy(() -> storage.initializeLocationForSavepoint(1337, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void testSavepoint(
            @Nullable Path savepointDir, @Nullable Path customDir, Path expectedParent)
            throws Exception {

        final CheckpointStorageAccess storage =
                savepointDir == null
                        ? createCheckpointStorage(randomTempPath())
                        : createCheckpointStorageWithSavepointDir(randomTempPath(), savepointDir);

        final String customLocation = customDir == null ? null : customDir.toString();

        final CheckpointStorageLocation savepointLocation =
                storage.initializeLocationForSavepoint(52452L, customLocation);

        final byte[] data = {77, 66, 55, 99, 88};

        final CompletedCheckpointStorageLocation completed;
        try (CheckpointMetadataOutputStream out = savepointLocation.createMetadataOutputStream()) {
            out.write(data);
            completed = out.closeAndFinalizeCheckpoint();
        }

        // we need to do this step to make sure we have a slash (or not) in the same way as the
        // expected path has it
        final Path normalizedWithSlash =
                Path.fromLocalFile(
                        new File(new Path(completed.getExternalPointer()).getParent().getPath()));

        assertThat(normalizedWithSlash).isEqualTo(expectedParent);
        validateContents(completed.getMetadataHandle(), data);

        // validate that the correct directory was used
        FileStateHandle fileStateHandle = (FileStateHandle) completed.getMetadataHandle();

        // we need to recreate that path in the same way as the "expected path" (via File and URI)
        // to make
        // sure the either both use '/' suffixes, or neither uses them (a bit of an annoying
        // ambiguity)
        Path usedSavepointDir =
                new Path(
                        new File(fileStateHandle.getFilePath().getParent().getParent().getPath())
                                .toURI());

        assertThat(usedSavepointDir).isEqualTo(expectedParent);
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    public Path randomTempPath() throws IOException {
        return Path.fromLocalFile(TempDirUtils.newFolder(tmp));
    }

    private static void validateContents(StreamStateHandle handle, byte[] expected)
            throws IOException {
        try (FSDataInputStream in = handle.openInputStream()) {
            validateContents(in, expected);
        }
    }

    private static void validateContents(InputStream in, byte[] expected) throws IOException {
        final byte[] buffer = new byte[expected.length];

        int pos = 0;
        int remaining = expected.length;
        while (remaining > 0) {
            int read = in.read(buffer, pos, remaining);
            if (read == -1) {
                throw new EOFException();
            }
            pos += read;
            remaining -= read;
        }

        assertThat(buffer).isEqualTo(expected);
    }
}
