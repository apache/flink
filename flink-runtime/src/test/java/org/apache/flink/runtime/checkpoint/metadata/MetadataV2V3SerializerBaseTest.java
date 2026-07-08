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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the exclusive-directory-aware serialization of {@link RelativeFileStateHandle}s in
 * {@link MetadataV2V3SerializerBase}.
 *
 * <p>A {@link RelativeFileStateHandle} only stores the file name relative to the exclusive
 * directory it was written into. On recovery the absolute path is rebuilt as {@code new
 * Path(<exclusiveDir>, relativePath)}. This is only correct as long as the file physically lives in
 * that exclusive directory. The serializer therefore keeps a relative handle relative only when it
 * belongs to the exclusive directory being written, and otherwise persists its absolute path.
 *
 * <p>Throughout this test an <i>own</i> handle is one whose file lives in the directory the
 * metadata is written to; a <i>foreign</i> handle points anywhere else.
 */
class MetadataV2V3SerializerBaseTest {

    @TempDir private java.nio.file.Path tmp;

    /**
     * A shared-state file that lives <b>outside</b> the checkpoint's exclusive directory (e.g. an
     * SST file of a NATIVE savepoint reused by the first incremental checkpoint after a CLAIM-mode
     * restore) must survive the metadata round-trip with its correct absolute path, not be resolved
     * against the checkpoint's exclusive directory.
     */
    @Test
    void testForeignRelativeSharedStateRoundTripsAsAbsolute() throws Exception {
        final File checkpointDir = TempDirUtils.newFolder(tmp, "chk-3");
        final File savepointDir = TempDirUtils.newFolder(tmp, "savepoint");
        writeEmptyMetadataFile(checkpointDir);

        final Path exclusiveDirPath = Path.fromLocalFile(checkpointDir);
        final Path savepointDirPath = Path.fromLocalFile(savepointDir);

        final String relativeFileName = UUID.randomUUID().toString();
        final Path foreignAbsolutePath = new Path(savepointDirPath, relativeFileName);
        final long stateSize = 4242L;
        final RelativeFileStateHandle foreignHandle =
                new RelativeFileStateHandle(foreignAbsolutePath, relativeFileName, stateSize);

        final IncrementalRemoteKeyedStateHandle reloaded =
                roundTrip(
                        incrementalHandleWithSharedState(foreignHandle, "000001.sst"),
                        exclusiveDirPath,
                        checkpointDir.getAbsolutePath());

        final StreamStateHandle reloadedShared = reloaded.getSharedState().get(0).getHandle();
        assertThat(reloadedShared)
                .as("a shared handle pointing outside the exclusive dir must come back absolute")
                .isInstanceOf(FileStateHandle.class)
                .isNotInstanceOf(RelativeFileStateHandle.class);
        final FileStateHandle reloadedFile = (FileStateHandle) reloadedShared;
        assertThat(reloadedFile.getFilePath()).isEqualTo(foreignAbsolutePath);
        assertThat(reloadedFile.getStateSize()).isEqualTo(stateSize);
    }

    /**
     * A handle that actually belongs to the exclusive directory being written (a savepoint's own
     * SST) must stay relative so that the directory can still be relocated: re-reading the metadata
     * from a different location resolves the handle against that new location.
     */
    @Test
    void testOwnRelativeStateRoundTripsAsRelativeAndIsRelocatable() throws Exception {
        final File savepointDir = TempDirUtils.newFolder(tmp, "savepoint");
        writeEmptyMetadataFile(savepointDir);

        final Path savepointDirPath = Path.fromLocalFile(savepointDir);
        final String relativeFileName = UUID.randomUUID().toString();
        final Path ownAbsolutePath = new Path(savepointDirPath, relativeFileName);
        final long stateSize = 777L;
        final RelativeFileStateHandle ownHandle =
                new RelativeFileStateHandle(ownAbsolutePath, relativeFileName, stateSize);

        final byte[] metadataBytes =
                serialize(
                        incrementalHandleWithSharedState(ownHandle, "000002.sst"),
                        savepointDirPath);

        // Re-read from the original location: must stay relative and resolve to the original path.
        final IncrementalRemoteKeyedStateHandle reloaded =
                deserialize(metadataBytes, savepointDir.getAbsolutePath());

        final StreamStateHandle reloadedShared = reloaded.getSharedState().get(0).getHandle();
        assertThat(reloadedShared)
                .as("a handle belonging to the exclusive dir must round-trip as relative")
                .isInstanceOf(RelativeFileStateHandle.class);
        assertThat(((RelativeFileStateHandle) reloadedShared).getRelativePath())
                .isEqualTo(relativeFileName);
        assertThat(((FileStateHandle) reloadedShared).getFilePath()).isEqualTo(ownAbsolutePath);

        // Relocate the savepoint: re-read the very same bytes from a *different* directory. Because
        // the handle stayed relative, it must now resolve against the new location.
        final File movedDir = TempDirUtils.newFolder(tmp, "savepoint-moved");
        writeEmptyMetadataFile(movedDir);
        final Path movedDirPath = Path.fromLocalFile(movedDir);

        final IncrementalRemoteKeyedStateHandle relocated =
                deserialize(metadataBytes, movedDir.getAbsolutePath());
        final StreamStateHandle relocatedShared = relocated.getSharedState().get(0).getHandle();
        assertThat(relocatedShared).isInstanceOf(RelativeFileStateHandle.class);
        assertThat(((FileStateHandle) relocatedShared).getFilePath())
                .as(
                        "a relocated savepoint must resolve its relative handles against the new directory")
                .isEqualTo(new Path(movedDirPath, relativeFileName));
    }

    /**
     * A checkpoint may reference its own relative handles and foreign ones (reused savepoint SSTs)
     * at the same time. Each must be encoded on its own merits: own handles stay relative, foreign
     * handles become absolute.
     */
    @Test
    void testMixedOwnAndForeignRelativeSharedState() throws Exception {
        final File checkpointDir = TempDirUtils.newFolder(tmp, "chk-4");
        final File savepointDir = TempDirUtils.newFolder(tmp, "savepoint");
        writeEmptyMetadataFile(checkpointDir);

        final Path exclusiveDirPath = Path.fromLocalFile(checkpointDir);
        final Path savepointDirPath = Path.fromLocalFile(savepointDir);

        final String ownFileName = UUID.randomUUID().toString();
        final String foreignFileName = UUID.randomUUID().toString();
        final RelativeFileStateHandle ownHandle =
                new RelativeFileStateHandle(
                        new Path(exclusiveDirPath, ownFileName), ownFileName, 11L);
        final RelativeFileStateHandle foreignHandle =
                new RelativeFileStateHandle(
                        new Path(savepointDirPath, foreignFileName), foreignFileName, 22L);

        final IncrementalRemoteKeyedStateHandle handle =
                incrementalHandleWithSharedState(
                        Arrays.asList(
                                HandleAndLocalPath.of(ownHandle, "000001.sst"),
                                HandleAndLocalPath.of(foreignHandle, "000002.sst")));

        final IncrementalRemoteKeyedStateHandle reloaded =
                roundTrip(handle, exclusiveDirPath, checkpointDir.getAbsolutePath());

        final StreamStateHandle reloadedOwn = reloaded.getSharedState().get(0).getHandle();
        assertThat(reloadedOwn).isInstanceOf(RelativeFileStateHandle.class);
        assertThat(((FileStateHandle) reloadedOwn).getFilePath())
                .isEqualTo(new Path(exclusiveDirPath, ownFileName));

        final StreamStateHandle reloadedForeign = reloaded.getSharedState().get(1).getHandle();
        assertThat(reloadedForeign)
                .isInstanceOf(FileStateHandle.class)
                .isNotInstanceOf(RelativeFileStateHandle.class);
        assertThat(((FileStateHandle) reloadedForeign).getFilePath())
                .isEqualTo(new Path(savepointDirPath, foreignFileName));
    }

    /**
     * Without a known exclusive directory (a {@code null} {@code exclusiveDirPath}, which is what
     * the state processor API requests via {@code
     * Checkpoints#storeCheckpointMetadataWithoutExclusiveDir}), the legacy encoding must be
     * preserved: every relative handle stays relative and is resolved against whatever directory
     * the metadata is later read from.
     */
    @Test
    void testNullExclusiveDirKeepsLegacyRelativeEncoding() throws Exception {
        final File checkpointDir = TempDirUtils.newFolder(tmp, "chk-5");
        final File savepointDir = TempDirUtils.newFolder(tmp, "savepoint");
        writeEmptyMetadataFile(checkpointDir);

        final String fileName = UUID.randomUUID().toString();
        final RelativeFileStateHandle foreignHandle =
                new RelativeFileStateHandle(
                        new Path(Path.fromLocalFile(savepointDir), fileName), fileName, 33L);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            // context-free convenience variant: must behave like exclusiveDirPath == null
            MetadataV3Serializer.INSTANCE.serialize(
                    metadataFor(incrementalHandleWithSharedState(foreignHandle, "000001.sst")),
                    dos);
        }

        final IncrementalRemoteKeyedStateHandle reloaded =
                deserialize(baos.toByteArray(), checkpointDir.getAbsolutePath());
        final StreamStateHandle reloadedShared = reloaded.getSharedState().get(0).getHandle();
        assertThat(reloadedShared).isInstanceOf(RelativeFileStateHandle.class);
        assertThat(((FileStateHandle) reloadedShared).getFilePath())
                .as("legacy encoding resolves the handle against the directory it is read from")
                .isEqualTo(new Path(Path.fromLocalFile(checkpointDir), fileName));
    }

    /**
     * The own/foreign decision must also work for object-store URIs (no filesystem access is
     * involved in taking it): a foreign relative handle on s3/abfss is persisted with its full
     * absolute URI.
     */
    @Test
    void testForeignRelativeHandleOnObjectStoreIsWrittenAbsolute() throws Exception {
        for (String scheme : new String[] {"s3", "abfss"}) {
            final Path savepointDir = new Path(scheme + "://bucket/flink/savepoints/savepoint-1");
            final Path exclusiveDir = new Path(scheme + "://bucket/flink/checkpoints/chk-2");
            final String fileName = UUID.randomUUID().toString();
            final RelativeFileStateHandle foreignHandle =
                    new RelativeFileStateHandle(new Path(savepointDir, fileName), fileName, 44L);

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (DataOutputStream dos = new DataOutputStream(baos)) {
                MetadataV2V3SerializerBase.serializeStreamStateHandle(
                        foreignHandle,
                        dos,
                        new MetadataV2V3SerializerBase.SerializationContext(exclusiveDir));
            }

            // a null deserialization context suffices exactly because the handle must have been
            // written absolute; a (wrongly) relative handle would fail to deserialize here
            final StreamStateHandle reloaded =
                    MetadataV2V3SerializerBase.deserializeStreamStateHandle(
                            new DataInputStream(new ByteArrayInputStream(baos.toByteArray())),
                            null);
            assertThat(reloaded)
                    .as(scheme + " foreign handle must be written absolute")
                    .isInstanceOf(FileStateHandle.class)
                    .isNotInstanceOf(RelativeFileStateHandle.class);
            assertThat(((FileStateHandle) reloaded).getFilePath())
                    .isEqualTo(new Path(savepointDir, fileName));
        }
    }

    /**
     * Counterpart of {@link #testForeignRelativeHandleOnObjectStoreIsWrittenAbsolute}: a handle
     * that belongs to the object-store exclusive directory keeps the relative encoding, bit for bit
     * the same as the legacy (context-free) encoding.
     */
    @Test
    void testOwnRelativeHandleOnObjectStoreStaysRelative() throws Exception {
        for (String scheme : new String[] {"s3", "abfss"}) {
            final Path exclusiveDir = new Path(scheme + "://bucket/flink/checkpoints/chk-2");
            final String fileName = UUID.randomUUID().toString();
            final RelativeFileStateHandle ownHandle =
                    new RelativeFileStateHandle(new Path(exclusiveDir, fileName), fileName, 55L);

            final ByteArrayOutputStream withContext = new ByteArrayOutputStream();
            try (DataOutputStream dos = new DataOutputStream(withContext)) {
                MetadataV2V3SerializerBase.serializeStreamStateHandle(
                        ownHandle,
                        dos,
                        new MetadataV2V3SerializerBase.SerializationContext(exclusiveDir));
            }
            final ByteArrayOutputStream legacy = new ByteArrayOutputStream();
            try (DataOutputStream dos = new DataOutputStream(legacy)) {
                MetadataV2V3SerializerBase.serializeStreamStateHandle(ownHandle, dos, null);
            }

            assertThat(withContext.toByteArray())
                    .as(scheme + " own handle must keep the (relative) legacy encoding")
                    .isEqualTo(legacy.toByteArray());
        }
    }

    /**
     * The own/foreign decision must also apply to handles nested inside a {@link
     * ChangelogStateBackendHandle}: its materialized handles are serialized through a recursive
     * call that has to keep passing the serialization context along.
     */
    @Test
    void testForeignRelativeStateInsideChangelogHandleRoundTripsAsAbsolute() throws Exception {
        final File checkpointDir = TempDirUtils.newFolder(tmp, "chk-6");
        final File savepointDir = TempDirUtils.newFolder(tmp, "savepoint");
        writeEmptyMetadataFile(checkpointDir);

        final Path exclusiveDirPath = Path.fromLocalFile(checkpointDir);
        final String relativeFileName = UUID.randomUUID().toString();
        final Path foreignAbsolutePath =
                new Path(Path.fromLocalFile(savepointDir), relativeFileName);
        final RelativeFileStateHandle foreignHandle =
                new RelativeFileStateHandle(foreignAbsolutePath, relativeFileName, 4242L);

        // Reachable only through serializeKeyedStateHandle's materialized-handles recursion.
        final ChangelogStateBackendHandle changelogHandle =
                new ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl(
                        Collections.singletonList(
                                incrementalHandleWithSharedState(foreignHandle, "000001.sst")),
                        Collections.emptyList(),
                        KeyGroupRange.of(0, 0),
                        1L,
                        1L,
                        0L);

        final KeyedStateHandle reloaded =
                deserializeManagedKeyedState(
                        serialize(changelogHandle, exclusiveDirPath),
                        checkpointDir.getAbsolutePath());

        assertThat(reloaded).isInstanceOf(ChangelogStateBackendHandle.class);
        final StreamStateHandle reloadedShared =
                ((IncrementalRemoteKeyedStateHandle)
                                ((ChangelogStateBackendHandle) reloaded)
                                        .getMaterializedStateHandles()
                                        .get(0))
                        .getSharedState()
                        .get(0)
                        .getHandle();
        assertThat(reloadedShared)
                .as("a foreign shared handle nested in a changelog handle must come back absolute")
                .isInstanceOf(FileStateHandle.class)
                .isNotInstanceOf(RelativeFileStateHandle.class);
        assertThat(((FileStateHandle) reloadedShared).getFilePath()).isEqualTo(foreignAbsolutePath);
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private static IncrementalRemoteKeyedStateHandle incrementalHandleWithSharedState(
            StreamStateHandle sharedHandle, String localPath) {
        return incrementalHandleWithSharedState(
                Collections.singletonList(HandleAndLocalPath.of(sharedHandle, localPath)));
    }

    private static IncrementalRemoteKeyedStateHandle incrementalHandleWithSharedState(
            List<HandleAndLocalPath> sharedState) {
        return new IncrementalRemoteKeyedStateHandle(
                UUID.nameUUIDFromBytes("backend".getBytes(StandardCharsets.UTF_8)),
                KeyGroupRange.of(0, 0),
                1L,
                sharedState,
                Collections.emptyList(),
                new ByteStreamStateHandle("meta", new byte[] {1, 2, 3}));
    }

    private static IncrementalRemoteKeyedStateHandle roundTrip(
            IncrementalRemoteKeyedStateHandle handle, Path exclusiveDirPath, String externalPointer)
            throws Exception {
        return deserialize(serialize(handle, exclusiveDirPath), externalPointer);
    }

    private static CheckpointMetadata metadataFor(KeyedStateHandle handle) {
        final OperatorSubtaskState subtaskState =
                OperatorSubtaskState.builder().setManagedKeyedState(handle).build();
        final OperatorState operatorState = new OperatorState(null, null, new OperatorID(), 1, 1);
        operatorState.putState(0, subtaskState);
        return new CheckpointMetadata(
                1L, Collections.singletonList(operatorState), Collections.emptyList());
    }

    private static byte[] serialize(KeyedStateHandle handle, Path exclusiveDirPath)
            throws Exception {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            MetadataV3Serializer.INSTANCE.serialize(metadataFor(handle), dos, exclusiveDirPath);
        }
        return baos.toByteArray();
    }

    private static IncrementalRemoteKeyedStateHandle deserialize(
            byte[] bytes, String externalPointer) throws Exception {
        return (IncrementalRemoteKeyedStateHandle)
                deserializeManagedKeyedState(bytes, externalPointer);
    }

    private static KeyedStateHandle deserializeManagedKeyedState(
            byte[] bytes, String externalPointer) throws Exception {
        final CheckpointMetadata reloaded;
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            reloaded =
                    MetadataV3Serializer.INSTANCE.deserialize(
                            dis,
                            MetadataV2V3SerializerBaseTest.class.getClassLoader(),
                            externalPointer);
        }
        final OperatorState operatorState = reloaded.getOperatorStates().iterator().next();
        final OperatorSubtaskState subtaskState = operatorState.getStates().iterator().next();
        return subtaskState.getManagedKeyedState().iterator().next();
    }

    /**
     * Creates an empty {@code _metadata} file so the directory resolves via {@code
     * AbstractFsCheckpointStorageAccess#resolveCheckpointPointer}, which deserialization uses to
     * rebuild relative handles. This keeps a wrongly-relative handle failing on the test's own
     * assertion instead of on an IOException about a missing _metadata file.
     */
    private static void writeEmptyMetadataFile(File dir) throws Exception {
        final Path metadataFile =
                new Path(
                        Path.fromLocalFile(dir),
                        AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME);
        FileSystem.getLocalFileSystem()
                .create(metadataFile, FileSystem.WriteMode.OVERWRITE)
                .close();
    }
}
