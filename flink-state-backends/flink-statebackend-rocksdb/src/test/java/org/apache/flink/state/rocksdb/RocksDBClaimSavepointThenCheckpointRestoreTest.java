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

package org.apache.flink.state.rocksdb;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.core.fs.Path.fromLocalFile;
import static org.apache.flink.core.fs.local.LocalFileSystem.getSharedInstance;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that checkpoint metadata keeps its file references self-consistent when a checkpoint's
 * shared state contains files that live outside the checkpoint's own exclusive directory.
 *
 * <p>A NATIVE savepoint writes its SST files into the savepoint directory as {@link
 * RelativeFileStateHandle}s (see {@code RocksIncrementalSnapshotStrategy}, which writes savepoint
 * SSTs with the EXCLUSIVE state scope but stores them in the {@code sharedState} collection of the
 * {@link IncrementalRemoteKeyedStateHandle}). A backend restored from such a savepoint (as happens
 * when a job is resumed from it in CLAIM mode) reuses those files, so the first incremental
 * checkpoint taken afterwards references relative handles whose files live in the <i>savepoint</i>
 * directory, not in the checkpoint's own exclusive directory.
 *
 * <p>On deserialization, a relative handle is resolved against the directory of the metadata being
 * read ({@code new Path(exclusiveDirPath, relativePath)}). The metadata serializer ({@code
 * MetadataV2V3SerializerBase}) must therefore use the relative encoding only for handles that
 * actually belong to the exclusive directory being written, and persist the absolute path for all
 * others: a foreign handle written as relative would resolve to {@code checkpointDir/<uuid>} while
 * the bytes live at {@code savepointDir/<uuid>}, and restoring from the checkpoint would fail with
 * a file-not-found error.
 *
 * <p>This test verifies the full round trip in process: it takes a NATIVE savepoint, restores a
 * backend from it, takes the first incremental checkpoint, round-trips that checkpoint's metadata
 * through an on-disk metadata file, and restores a RocksDB backend from the reloaded handles. The
 * JobManager wiring it bypasses (metadata finalization through {@code
 * CheckpointMetadataOutputStream}) is covered by {@code
 * ResumeCheckpointAfterClaimedNativeSavepointITCase} in flink-tests.
 */
class RocksDBClaimSavepointThenCheckpointRestoreTest {

    private static final int NUM_KEYS = 1000;
    private static final int NUM_KEY_GROUPS = 2;
    private static final KeyGroupRange KEY_GROUP_RANGE = new KeyGroupRange(0, 1);

    @TempDir private Path tmp;

    @Test
    void testFirstCheckpointSurvivesMetadataRoundTrip() throws Exception {

        final ValueStateDescriptor<String> stateDescriptor =
                new ValueStateDescriptor<>("state", String.class);

        // backend1: write state, take a normal incremental checkpoint to force real SSTs,
        // write more state, then take a NATIVE savepoint.
        final File savepointDir = TempDirUtils.newFolder(tmp, "savepoint");
        final File savepointSharedDir = TempDirUtils.newFolder(tmp, "savepoint-shared");
        final File cp1Dir = TempDirUtils.newFolder(tmp, "cp1");
        final File cp1SharedDir = TempDirUtils.newFolder(tmp, "cp1-shared");

        final KeyedStateHandle savepointHandle;
        RocksDBKeyedStateBackend<Integer> backend1 =
                buildBackend(TempDirUtils.newFolder(tmp, "backend1"), Collections.emptyList());
        try {
            writeKeys(backend1, stateDescriptor, 0, NUM_KEYS);
            // A normal incremental checkpoint forces RocksDB to flush memtables into SST files.
            takeSnapshot(
                    backend1,
                    1L,
                    streamFactory(cp1Dir, cp1SharedDir),
                    CheckpointOptions.forCheckpointWithDefaultLocation());
            // More writes on top of the flushed SSTs, so the savepoint covers both previously
            // flushed files and fresh memtable data.
            writeKeys(backend1, stateDescriptor, 0, NUM_KEYS);

            savepointHandle =
                    takeSnapshot(
                            backend1,
                            2L,
                            streamFactory(savepointDir, savepointSharedDir),
                            new CheckpointOptions(
                                    SavepointType.savepoint(SavepointFormatType.NATIVE),
                                    CheckpointStorageLocationReference.getDefault()));
        } finally {
            IOUtils.closeQuietly(backend1);
            backend1.dispose();
        }

        // Precondition: the NATIVE savepoint stores its SSTs as relative handles in shared state.
        assertThat(savepointHandle).isInstanceOf(IncrementalRemoteKeyedStateHandle.class);
        assertThat(((IncrementalRemoteKeyedStateHandle) savepointHandle).getSharedState())
                .as("NATIVE savepoint should write its SST files as RelativeFileStateHandles")
                .anyMatch(
                        handleAndPath ->
                                handleAndPath.getHandle() instanceof RelativeFileStateHandle);

        // backend2: restore from the savepoint (CLAIM-mode reuse at this level) and take the
        // first incremental checkpoint to a DIFFERENT checkpoint directory.
        final File checkpointDir = TempDirUtils.newFolder(tmp, "cp2");
        final File checkpointSharedDir = TempDirUtils.newFolder(tmp, "cp2-shared");

        final IncrementalRemoteKeyedStateHandle checkpointHandle;
        RocksDBKeyedStateBackend<Integer> backend2 =
                buildBackend(
                        TempDirUtils.newFolder(tmp, "backend2"),
                        Collections.singletonList(savepointHandle));
        try {
            checkpointHandle =
                    (IncrementalRemoteKeyedStateHandle)
                            takeSnapshot(
                                    backend2,
                                    3L,
                                    streamFactory(checkpointDir, checkpointSharedDir),
                                    CheckpointOptions.forCheckpointWithDefaultLocation());
        } finally {
            IOUtils.closeQuietly(backend2);
            backend2.dispose();
        }

        // Precondition: the first checkpoint reuses the savepoint's SST files. They show up as
        // placeholders that will resolve, via the SharedStateRegistry, to whatever was registered
        // for those files during the CLAIM-mode restore of the savepoint.
        assertThat(checkpointHandle.getSharedState())
                .as(
                        "First incremental checkpoint after claiming the savepoint should reuse the "
                                + "savepoint's SSTs (carried as placeholders in shared state)")
                .anyMatch(
                        handleAndPath ->
                                handleAndPath.getHandle() instanceof PlaceholderStreamStateHandle);

        // Register shared state as the JobManager does (the relevant subset):
        //   a) register the claimed savepoint on CLAIM-mode restore
        //      (SharedStateRegistry#registerAllAfterRestored -> registerSharedStates), then
        //   b) register the first completed incremental checkpoint, whose placeholders now
        //      resolve against the entries registered in (a).
        // After this step the checkpoint's shared state holds relative handles pointing at
        // files in the savepoint directory, which is what the metadata serializer sees when
        // the JobManager finalizes the checkpoint.
        final SharedStateRegistry registry = new SharedStateRegistryImpl();
        IncrementalRemoteKeyedStateHandle savepointIncremental =
                (IncrementalRemoteKeyedStateHandle) savepointHandle;
        savepointIncremental.registerSharedStates(registry, savepointIncremental.getCheckpointId());
        checkpointHandle.registerSharedStates(registry, 3L);

        final IncrementalRemoteKeyedStateHandle restoredHandle =
                metadataRoundTrip(checkpointHandle, checkpointDir, 3L);

        // RocksDB-independent assertion: EVERY shared file referenced by the reloaded
        // checkpoint must physically exist on disk. This covers any SSTs the checkpoint
        // uploaded itself (absolute handles in the shared-state directory) as well as the
        // inherited savepoint SSTs. In this scenario nothing was written after the restore,
        // so the shared state holds only the reused savepoint files. If such a handle were
        // written with the relative encoding, it would be resolved against checkpointDir/<uuid>
        // (which does not exist) instead of savepointDir/<uuid>.
        List<File> referencedSharedFiles =
                restoredHandle.getSharedState().stream()
                        .map(HandleAndLocalPath::getHandle)
                        .filter(FileStateHandle.class::isInstance)
                        .map(handle -> new File(((FileStateHandle) handle).getFilePath().getPath()))
                        .collect(Collectors.toList());
        assertThat(referencedSharedFiles)
                .as("The reloaded checkpoint must reference at least one shared file on disk")
                .isNotEmpty()
                .allSatisfy(file -> assertThat(file).exists());

        // backend3: restore from the reloaded checkpoint handle and verify the state.
        RocksDBKeyedStateBackend<Integer> backend3 =
                buildBackend(
                        TempDirUtils.newFolder(tmp, "backend3"),
                        Collections.singletonList(restoredHandle));
        try {
            assertStateRestored(backend3, stateDescriptor, 0, NUM_KEYS);
        } finally {
            IOUtils.closeQuietly(backend3);
            backend3.dispose();
        }
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private RocksDBKeyedStateBackend<Integer> buildBackend(
            File instanceBasePath, Collection<KeyedStateHandle> restoreHandles) throws Exception {
        return RocksDBTestUtils.builderForTestDefaults(
                        instanceBasePath,
                        IntSerializer.INSTANCE,
                        NUM_KEY_GROUPS,
                        KEY_GROUP_RANGE,
                        restoreHandles)
                .setEnableIncrementalCheckpointing(true)
                .build();
    }

    private FsCheckpointStreamFactory streamFactory(File checkpointDir, File sharedStateDir) {
        return new FsCheckpointStreamFactory(
                getSharedInstance(),
                fromLocalFile(checkpointDir),
                fromLocalFile(sharedStateDir),
                1,
                4096);
    }

    private void writeKeys(
            RocksDBKeyedStateBackend<Integer> backend,
            ValueStateDescriptor<String> descriptor,
            int fromInclusive,
            int toExclusive)
            throws Exception {
        for (int key = fromInclusive; key < toExclusive; key++) {
            backend.setCurrentKey(key);
            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
            state.update(valueFor(key));
        }
    }

    private void assertStateRestored(
            RocksDBKeyedStateBackend<Integer> backend,
            ValueStateDescriptor<String> descriptor,
            int fromInclusive,
            int toExclusive)
            throws Exception {
        for (int key = fromInclusive; key < toExclusive; key++) {
            backend.setCurrentKey(key);
            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
            assertThat(state.value()).isEqualTo(valueFor(key));
        }
    }

    private static String valueFor(int key) {
        return "value-for-key-" + key + "-0123456789abcdefghijklmnopqrstuvwxyz";
    }

    private static KeyedStateHandle takeSnapshot(
            RocksDBKeyedStateBackend<Integer> backend,
            long checkpointId,
            FsCheckpointStreamFactory streamFactory,
            CheckpointOptions options)
            throws Exception {
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                backend.snapshot(checkpointId, checkpointId, streamFactory, options);
        snapshot.run();
        return snapshot.get().getJobManagerOwnedSnapshot();
    }

    /**
     * Serializes the given handle into checkpoint metadata stored in {@code exclusiveDir} under
     * {@link AbstractFsCheckpointStorageAccess#METADATA_FILE_NAME} and reloads it, mirroring what
     * the JobManager does. Serialization is told the checkpoint's exclusive directory (as
     * production does via {@code FsCheckpointMetadataOutputStream}), and deserialization uses
     * {@code exclusiveDir} as the external pointer, so any {@link RelativeFileStateHandle} that
     * actually belongs to {@code exclusiveDir} is resolved relative to it while foreign relative
     * handles are written (and read back) as absolute.
     */
    private static IncrementalRemoteKeyedStateHandle metadataRoundTrip(
            IncrementalRemoteKeyedStateHandle handle, File exclusiveDir, long checkpointId)
            throws Exception {
        OperatorSubtaskState subtaskState =
                OperatorSubtaskState.builder().setManagedKeyedState(handle).build();
        OperatorState operatorState = new OperatorState(null, null, new OperatorID(), 1, 2);
        operatorState.putState(0, subtaskState);
        CheckpointMetadata metadata =
                new CheckpointMetadata(
                        checkpointId,
                        Collections.singletonList(operatorState),
                        Collections.emptyList());

        File metadataFile =
                new File(exclusiveDir, AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME);
        try (DataOutputStream dos =
                new DataOutputStream(new FileOutputStream(metadataFile, false))) {
            Checkpoints.storeCheckpointMetadata(metadata, dos, fromLocalFile(exclusiveDir));
        }

        CheckpointMetadata reloaded;
        try (DataInputStream dis = new DataInputStream(new FileInputStream(metadataFile))) {
            reloaded =
                    Checkpoints.loadCheckpointMetadata(
                            dis,
                            RocksDBClaimSavepointThenCheckpointRestoreTest.class.getClassLoader(),
                            exclusiveDir.getAbsolutePath());
        }

        OperatorState reloadedOperatorState = reloaded.getOperatorStates().iterator().next();
        OperatorSubtaskState reloadedSubtaskState =
                reloadedOperatorState.getStates().iterator().next();
        return (IncrementalRemoteKeyedStateHandle)
                reloadedSubtaskState.getManagedKeyedState().iterator().next();
    }
}
