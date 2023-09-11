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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.DuplicatingFileSystem;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.NotDuplicatingCheckpointStateToolset;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;
import org.apache.flink.testutils.TestFileSystem;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link FsCheckpointStorageAccess}, which implements the checkpoint storage aspects
 * of the {@link FsStateBackend}.
 */
class FsCheckpointStorageAccessTest extends AbstractFileCheckpointStorageAccessTestBase {

    private static final int FILE_SIZE_THRESHOLD = 1024;
    private static final int WRITE_BUFFER_SIZE = 4096;

    // ------------------------------------------------------------------------
    //  General Fs-based checkpoint storage tests, inherited
    // ------------------------------------------------------------------------

    @Override
    protected CheckpointStorageAccess createCheckpointStorage(Path checkpointDir) throws Exception {
        return new FsCheckpointStorageAccess(
                checkpointDir, null, new JobID(), FILE_SIZE_THRESHOLD, WRITE_BUFFER_SIZE);
    }

    @Override
    protected CheckpointStorageAccess createCheckpointStorageWithSavepointDir(
            Path checkpointDir, Path savepointDir) throws Exception {
        return new FsCheckpointStorageAccess(
                checkpointDir, savepointDir, new JobID(), FILE_SIZE_THRESHOLD, WRITE_BUFFER_SIZE);
    }

    // ------------------------------------------------------------------------
    //  FsCheckpointStorage-specific tests
    // ------------------------------------------------------------------------

    @Test
    void testSavepointsInOneDirectoryDefaultLocation() throws Exception {
        final Path defaultSavepointDir = Path.fromLocalFile(TempDirUtils.newFolder(tmp));

        final FsCheckpointStorageAccess storage =
                new FsCheckpointStorageAccess(
                        Path.fromLocalFile(TempDirUtils.newFolder(tmp)),
                        defaultSavepointDir,
                        new JobID(),
                        FILE_SIZE_THRESHOLD,
                        WRITE_BUFFER_SIZE);

        final FsCheckpointStorageLocation savepointLocation =
                (FsCheckpointStorageLocation) storage.initializeLocationForSavepoint(52452L, null);

        // all state types should be in the expected location
        assertParent(defaultSavepointDir, savepointLocation.getCheckpointDirectory());
        assertParent(defaultSavepointDir, savepointLocation.getSharedStateDirectory());
        assertParent(defaultSavepointDir, savepointLocation.getTaskOwnedStateDirectory());

        // cleanup
        savepointLocation.disposeOnFailure();
    }

    @Test
    void testSavepointsInOneDirectoryCustomLocation() throws Exception {
        final Path savepointDir = Path.fromLocalFile(TempDirUtils.newFolder(tmp));

        final FsCheckpointStorageAccess storage =
                new FsCheckpointStorageAccess(
                        Path.fromLocalFile(TempDirUtils.newFolder(tmp)),
                        null,
                        new JobID(),
                        FILE_SIZE_THRESHOLD,
                        WRITE_BUFFER_SIZE);

        final FsCheckpointStorageLocation savepointLocation =
                (FsCheckpointStorageLocation)
                        storage.initializeLocationForSavepoint(52452L, savepointDir.toString());

        // all state types should be in the expected location
        assertParent(savepointDir, savepointLocation.getCheckpointDirectory());
        assertParent(savepointDir, savepointLocation.getSharedStateDirectory());
        assertParent(savepointDir, savepointLocation.getTaskOwnedStateDirectory());

        // cleanup
        savepointLocation.disposeOnFailure();
    }

    @Test
    void testTaskOwnedStateStream() throws Exception {
        final List<String> state = Arrays.asList("Flopsy", "Mopsy", "Cotton Tail", "Peter");

        // we chose a small size threshold here to force the state to disk
        final FsCheckpointStorageAccess storage =
                new FsCheckpointStorageAccess(
                        Path.fromLocalFile(TempDirUtils.newFolder(tmp)),
                        null,
                        new JobID(),
                        10,
                        WRITE_BUFFER_SIZE);

        final StreamStateHandle stateHandle;

        try (CheckpointStateOutputStream stream = storage.createTaskOwnedStateStream()) {
            assertThat(stream).isInstanceOf(FsCheckpointStateOutputStream.class);

            new ObjectOutputStream(stream).writeObject(state);
            stateHandle = stream.closeAndGetHandle();
        }

        // the state must have gone to disk
        FileStateHandle fileStateHandle = (FileStateHandle) stateHandle;

        // check that the state is in the correct directory
        String parentDirName = fileStateHandle.getFilePath().getParent().getName();
        assertThat(parentDirName)
                .isEqualTo(FsCheckpointStorageAccess.CHECKPOINT_TASK_OWNED_STATE_DIR);

        // validate the contents
        try (ObjectInputStream in = new ObjectInputStream(stateHandle.openInputStream())) {
            assertThat(in.readObject()).isEqualTo(state);
        }
    }

    @Test
    void testDirectoriesForExclusiveAndSharedState() throws Exception {
        final FileSystem fs = LocalFileSystem.getSharedInstance();
        final Path checkpointDir = randomTempPath();
        final Path sharedStateDir = randomTempPath();

        FsCheckpointStorageLocation storageLocation =
                new FsCheckpointStorageLocation(
                        fs,
                        checkpointDir,
                        sharedStateDir,
                        randomTempPath(),
                        CheckpointStorageLocationReference.getDefault(),
                        FILE_SIZE_THRESHOLD,
                        WRITE_BUFFER_SIZE);

        assertThat(storageLocation.getSharedStateDirectory())
                .isNotEqualTo(storageLocation.getCheckpointDirectory());

        assertThat(fs.listStatus(storageLocation.getCheckpointDirectory())).isEmpty();
        assertThat(fs.listStatus(storageLocation.getSharedStateDirectory())).isEmpty();

        // create exclusive state

        FsCheckpointStateOutputStream exclusiveStream =
                storageLocation.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);

        exclusiveStream.write(42);
        exclusiveStream.flushToFile();
        StreamStateHandle exclusiveHandle = exclusiveStream.closeAndGetHandle();

        assertThat(fs.listStatus(storageLocation.getCheckpointDirectory())).hasSize(1);
        assertThat(fs.listStatus(storageLocation.getSharedStateDirectory())).isEmpty();

        // create shared state

        FsCheckpointStateOutputStream sharedStream =
                storageLocation.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);

        sharedStream.write(42);
        sharedStream.flushToFile();
        StreamStateHandle sharedHandle = sharedStream.closeAndGetHandle();

        assertThat(fs.listStatus(storageLocation.getCheckpointDirectory())).hasSize(1);
        assertThat(fs.listStatus(storageLocation.getSharedStateDirectory())).hasSize(1);

        // drop state

        exclusiveHandle.discardState();
        sharedHandle.discardState();
    }

    /**
     * This test checks that the expected mkdirs action for checkpoint storage, only be called when
     * initializeBaseLocations and not called when resolveCheckpointStorageLocation.
     */
    @Test
    void testStorageLocationMkdirs() throws Exception {
        FsCheckpointStorageAccess storage =
                new FsCheckpointStorageAccess(
                        randomTempPath(),
                        null,
                        new JobID(),
                        FILE_SIZE_THRESHOLD,
                        WRITE_BUFFER_SIZE);

        File baseDir = new File(storage.getCheckpointsDirectory().getPath());
        assertThat(baseDir).doesNotExist();

        // mkdirs would only be called when initializeBaseLocations
        storage.initializeBaseLocationsForCheckpoint();
        assertThat(baseDir).exists();

        // mkdir would not be called when resolveCheckpointStorageLocation
        storage =
                new FsCheckpointStorageAccess(
                        randomTempPath(),
                        null,
                        new JobID(),
                        FILE_SIZE_THRESHOLD,
                        WRITE_BUFFER_SIZE);

        FsCheckpointStorageLocation location =
                (FsCheckpointStorageLocation)
                        storage.resolveCheckpointStorageLocation(
                                177, CheckpointStorageLocationReference.getDefault());

        Path checkpointPath = location.getCheckpointDirectory();
        File checkpointDir = new File(checkpointPath.getPath());
        assertThat(checkpointDir).doesNotExist();
    }

    @Test
    void testResolveCheckpointStorageLocation() throws Exception {
        final FileSystem checkpointFileSystem = mock(FileSystem.class);
        final FsCheckpointStorageAccess storage =
                new FsCheckpointStorageAccess(
                        new TestingPath("hdfs:///checkpoint/", checkpointFileSystem),
                        null,
                        new JobID(),
                        FILE_SIZE_THRESHOLD,
                        WRITE_BUFFER_SIZE);

        final FsCheckpointStorageLocation checkpointStreamFactory =
                (FsCheckpointStorageLocation)
                        storage.resolveCheckpointStorageLocation(
                                1L, CheckpointStorageLocationReference.getDefault());
        assertThat(checkpointStreamFactory.getFileSystem()).isEqualTo(checkpointFileSystem);

        final CheckpointStorageLocationReference savepointLocationReference =
                AbstractFsCheckpointStorageAccess.encodePathAsReference(
                        new Path("file:///savepoint/"));

        final FsCheckpointStorageLocation savepointStreamFactory =
                (FsCheckpointStorageLocation)
                        storage.resolveCheckpointStorageLocation(2L, savepointLocationReference);
        final FileSystem fileSystem = savepointStreamFactory.getFileSystem();
        assertThat(fileSystem).isInstanceOf(LocalFileSystem.class);
    }

    @Test
    void testNotDuplicationCheckpointStateToolset() throws Exception {
        CheckpointStorageAccess checkpointStorage = createCheckpointStorage(randomTempPath());
        assertThat(checkpointStorage.createTaskOwnedCheckpointStateToolset())
                .isInstanceOf(NotDuplicatingCheckpointStateToolset.class);
    }

    @Test
    void testDuplicationCheckpointStateToolset() throws Exception {
        CheckpointStorageAccess checkpointStorage =
                new FsCheckpointStorageAccess(
                        new TestDuplicatingFileSystem(),
                        randomTempPath(),
                        null,
                        new JobID(),
                        FILE_SIZE_THRESHOLD,
                        WRITE_BUFFER_SIZE);

        assertThat(checkpointStorage.createTaskOwnedCheckpointStateToolset())
                .isInstanceOf(FsCheckpointStateToolset.class);
    }

    private static final class TestDuplicatingFileSystem extends TestFileSystem
            implements DuplicatingFileSystem {

        @Override
        public boolean canFastDuplicate(Path source, Path destination) throws IOException {
            return !source.equals(destination);
        }

        @Override
        public void duplicate(List<CopyRequest> requests) throws IOException {}
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private void assertParent(Path parent, Path child) {
        Path path = new Path(parent, child.getName());
        assertThat(child).isEqualTo(path);
    }

    private static final class TestingPath extends Path {

        private static final long serialVersionUID = 2560119808844230488L;

        @SuppressWarnings("TransientFieldNotInitialized")
        @Nonnull
        private final transient FileSystem fileSystem;

        TestingPath(String pathString, @Nonnull FileSystem fileSystem) {
            super(pathString);
            this.fileSystem = fileSystem;
        }

        @Override
        public FileSystem getFileSystem() {
            return fileSystem;
        }
    }
}
