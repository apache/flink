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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryChangelogStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.BiFunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle.UNKNOWN_CHECKPOINTED_SIZE;
import static org.apache.flink.runtime.state.changelog.StateChange.META_KEY_GROUP;

/**
 * Base (De)serializer for checkpoint metadata format version 2 and 3.
 *
 * <p>The difference between versions 2 and 3 is minor. Version 3 includes operator coordinator
 * state for each operator, and drops some minor unused fields.
 *
 * <p>Basic checkpoint metadata layout:
 *
 * <pre>
 *  +--------------+---------------+-----------------+
 *  | checkpointID | master states | operator states |
 *  +--------------+---------------+-----------------+
 *
 *  Master state:
 *  +--------------+---------------------+---------+------+---------------+
 *  | magic number | num remaining bytes | version | name | payload bytes |
 *  +--------------+---------------------+---------+------+---------------+
 * </pre>
 */
@Internal
public abstract class MetadataV2V3SerializerBase {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataV2V3SerializerBase.class);

    /** Random magic number for consistency checks. */
    private static final int MASTER_STATE_MAGIC_NUMBER = 0xc96b1696;

    private static final byte NULL_HANDLE = 0;
    private static final byte BYTE_STREAM_STATE_HANDLE = 1;
    private static final byte FILE_STREAM_STATE_HANDLE = 2;
    private static final byte KEY_GROUPS_HANDLE = 3;
    private static final byte PARTITIONABLE_OPERATOR_STATE_HANDLE = 4;
    private static final byte INCREMENTAL_KEY_GROUPS_HANDLE = 5;
    private static final byte RELATIVE_STREAM_STATE_HANDLE = 6;
    private static final byte SAVEPOINT_KEY_GROUPS_HANDLE = 7;
    private static final byte CHANGELOG_HANDLE = 8;
    private static final byte CHANGELOG_BYTE_INCREMENT_HANDLE = 9;
    private static final byte CHANGELOG_FILE_INCREMENT_HANDLE = 10;
    // INCREMENTAL_KEY_GROUPS_HANDLE_V2 is introduced to add field of checkpointedSize and
    // stateHandleId.
    private static final byte INCREMENTAL_KEY_GROUPS_HANDLE_V2 = 11;
    // KEY_GROUPS_HANDLE_V2 is introduced to add new field of stateHandleId.
    private static final byte KEY_GROUPS_HANDLE_V2 = 12;
    // CHANGELOG_FILE_INCREMENT_HANDLE_V2 is introduced to add new field of storageIdentifier.
    private static final byte CHANGELOG_FILE_INCREMENT_HANDLE_V2 = 13;
    // CHANGELOG_HANDLE_V2 is introduced to add new field of checkpointId.
    private static final byte CHANGELOG_HANDLE_V2 = 14;

    // ------------------------------------------------------------------------
    //  (De)serialization entry points
    // ------------------------------------------------------------------------

    protected void serializeMetadata(CheckpointMetadata checkpointMetadata, DataOutputStream dos)
            throws IOException {
        // first: checkpoint ID
        dos.writeLong(checkpointMetadata.getCheckpointId());

        // second: master state
        final Collection<MasterState> masterStates = checkpointMetadata.getMasterStates();
        dos.writeInt(masterStates.size());
        for (MasterState ms : masterStates) {
            serializeMasterState(ms, dos);
        }

        // third: operator states
        Collection<OperatorState> operatorStates = checkpointMetadata.getOperatorStates();
        dos.writeInt(operatorStates.size());

        for (OperatorState operatorState : operatorStates) {
            serializeOperatorState(operatorState, dos);
        }
    }

    protected CheckpointMetadata deserializeMetadata(
            DataInputStream dis, @Nullable String externalPointer) throws IOException {

        final DeserializationContext context =
                externalPointer == null ? null : new DeserializationContext(externalPointer);

        // first: checkpoint ID
        final long checkpointId = dis.readLong();
        if (checkpointId < 0) {
            throw new IOException("invalid checkpoint ID: " + checkpointId);
        }

        // second: master state
        final List<MasterState> masterStates;
        final int numMasterStates = dis.readInt();

        if (numMasterStates == 0) {
            masterStates = Collections.emptyList();
        } else if (numMasterStates > 0) {
            masterStates = new ArrayList<>(numMasterStates);
            for (int i = 0; i < numMasterStates; i++) {
                masterStates.add(deserializeMasterState(dis));
            }
        } else {
            throw new IOException("invalid number of master states: " + numMasterStates);
        }

        // third: operator states
        final int numTaskStates = dis.readInt();
        final List<OperatorState> operatorStates = new ArrayList<>(numTaskStates);

        for (int i = 0; i < numTaskStates; i++) {
            operatorStates.add(deserializeOperatorState(dis, context));
        }

        return new CheckpointMetadata(checkpointId, operatorStates, masterStates);
    }

    // ------------------------------------------------------------------------
    //  master state (de)serialization methods
    // ------------------------------------------------------------------------

    protected void serializeMasterState(MasterState state, DataOutputStream dos)
            throws IOException {
        // magic number for error detection
        dos.writeInt(MASTER_STATE_MAGIC_NUMBER);

        // for safety, we serialize first into an array and then write the array and its
        // length into the checkpoint
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos);

        out.writeInt(state.version());
        out.writeUTF(state.name());

        final byte[] bytes = state.bytes();
        out.writeInt(bytes.length);
        out.write(bytes, 0, bytes.length);

        out.close();
        byte[] data = baos.toByteArray();

        dos.writeInt(data.length);
        dos.write(data, 0, data.length);
    }

    protected MasterState deserializeMasterState(DataInputStream dis) throws IOException {
        final int magicNumber = dis.readInt();
        if (magicNumber != MASTER_STATE_MAGIC_NUMBER) {
            throw new IOException("incorrect magic number in master styte byte sequence");
        }

        final int numBytes = dis.readInt();
        if (numBytes <= 0) {
            throw new IOException("found zero or negative length for master state bytes");
        }

        final byte[] data = new byte[numBytes];
        dis.readFully(data);

        final DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));

        final int version = in.readInt();
        final String name = in.readUTF();

        final byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);

        // check that the data is not corrupt
        if (in.read() != -1) {
            throw new IOException("found trailing bytes in master state");
        }

        return new MasterState(name, bytes, version);
    }

    // ------------------------------------------------------------------------
    //  operator state (de)serialization methods
    // ------------------------------------------------------------------------

    protected abstract void serializeOperatorState(
            OperatorState operatorState, DataOutputStream dos) throws IOException;

    protected abstract OperatorState deserializeOperatorState(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException;

    protected void serializeSubtaskState(OperatorSubtaskState subtaskState, DataOutputStream dos)
            throws IOException {
        serializeSingleton(
                subtaskState.getManagedOperatorState(), dos, this::serializeOperatorStateHandle);
        serializeSingleton(
                subtaskState.getRawOperatorState(), dos, this::serializeOperatorStateHandle);
        serializeKeyedStateCol(subtaskState.getManagedKeyedState(), dos);
        serializeKeyedStateCol(subtaskState.getRawKeyedState(), dos);
    }

    private void serializeKeyedStateCol(
            StateObjectCollection<KeyedStateHandle> managedKeyedState, DataOutputStream dos)
            throws IOException {
        serializeKeyedStateHandle(extractSingleton(managedKeyedState), dos);
    }

    protected OperatorSubtaskState deserializeSubtaskState(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {

        final OperatorSubtaskState.Builder state = OperatorSubtaskState.builder();

        final boolean hasManagedOperatorState = dis.readInt() != 0;
        if (hasManagedOperatorState) {
            state.setManagedOperatorState(deserializeOperatorStateHandle(dis, context));
        }

        final boolean hasRawOperatorState = dis.readInt() != 0;
        if (hasRawOperatorState) {
            state.setRawOperatorState(deserializeOperatorStateHandle(dis, context));
        }

        final KeyedStateHandle managedKeyedState = deserializeKeyedStateHandle(dis, context);
        if (managedKeyedState != null) {
            state.setManagedKeyedState(managedKeyedState);
        }
        final KeyedStateHandle rawKeyedState = deserializeKeyedStateHandle(dis, context);
        if (rawKeyedState != null) {
            state.setRawKeyedState(rawKeyedState);
        }

        state.setInputChannelState(deserializeInputChannelStateHandle(dis, context));
        state.setResultSubpartitionState(deserializeResultSubpartitionStateHandle(dis, context));

        return state.build();
    }

    // ------------------------------------------------------------------------
    //  keyed state
    // ------------------------------------------------------------------------

    @VisibleForTesting
    static void serializeKeyedStateHandle(KeyedStateHandle stateHandle, DataOutputStream dos)
            throws IOException {
        if (stateHandle == null) {
            dos.writeByte(NULL_HANDLE);
        } else if (stateHandle instanceof KeyGroupsStateHandle) {
            KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) stateHandle;

            if (stateHandle instanceof KeyGroupsSavepointStateHandle) {
                dos.writeByte(SAVEPOINT_KEY_GROUPS_HANDLE);
            } else {
                dos.writeByte(KEY_GROUPS_HANDLE_V2);
            }
            dos.writeInt(keyGroupsStateHandle.getKeyGroupRange().getStartKeyGroup());
            dos.writeInt(keyGroupsStateHandle.getKeyGroupRange().getNumberOfKeyGroups());
            for (int keyGroup : keyGroupsStateHandle.getKeyGroupRange()) {
                dos.writeLong(keyGroupsStateHandle.getOffsetForKeyGroup(keyGroup));
            }
            serializeStreamStateHandle(keyGroupsStateHandle.getDelegateStateHandle(), dos);

            // savepoint state handle would not need to persist state handle id out.
            if (!(stateHandle instanceof KeyGroupsSavepointStateHandle)) {
                writeStateHandleId(stateHandle, dos);
            }
        } else if (stateHandle instanceof IncrementalRemoteKeyedStateHandle) {
            IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
                    (IncrementalRemoteKeyedStateHandle) stateHandle;

            dos.writeByte(INCREMENTAL_KEY_GROUPS_HANDLE_V2);

            dos.writeLong(incrementalKeyedStateHandle.getCheckpointId());
            dos.writeUTF(String.valueOf(incrementalKeyedStateHandle.getBackendIdentifier()));
            dos.writeInt(incrementalKeyedStateHandle.getKeyGroupRange().getStartKeyGroup());
            dos.writeInt(incrementalKeyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups());
            dos.writeLong(incrementalKeyedStateHandle.getCheckpointedSize());

            serializeStreamStateHandle(incrementalKeyedStateHandle.getMetaDataStateHandle(), dos);

            serializeHandleAndLocalPathList(incrementalKeyedStateHandle.getSharedState(), dos);
            serializeHandleAndLocalPathList(incrementalKeyedStateHandle.getPrivateState(), dos);

            writeStateHandleId(incrementalKeyedStateHandle, dos);
        } else if (stateHandle instanceof ChangelogStateBackendHandle) {
            ChangelogStateBackendHandle handle = (ChangelogStateBackendHandle) stateHandle;

            dos.writeByte(CHANGELOG_HANDLE_V2);
            dos.writeInt(handle.getKeyGroupRange().getStartKeyGroup());
            dos.writeInt(handle.getKeyGroupRange().getNumberOfKeyGroups());

            dos.writeLong(handle.getCheckpointedSize());

            dos.writeInt(handle.getMaterializedStateHandles().size());
            for (KeyedStateHandle keyedStateHandle : handle.getMaterializedStateHandles()) {
                serializeKeyedStateHandle(keyedStateHandle, dos);
            }

            dos.writeInt(handle.getNonMaterializedStateHandles().size());
            for (KeyedStateHandle k : handle.getNonMaterializedStateHandles()) {
                serializeKeyedStateHandle(k, dos);
            }

            dos.writeLong(handle.getMaterializationID());
            dos.writeLong(handle.getCheckpointId());
            writeStateHandleId(handle, dos);

        } else if (stateHandle instanceof InMemoryChangelogStateHandle) {
            InMemoryChangelogStateHandle handle = (InMemoryChangelogStateHandle) stateHandle;
            dos.writeByte(CHANGELOG_BYTE_INCREMENT_HANDLE);
            dos.writeInt(handle.getKeyGroupRange().getStartKeyGroup());
            dos.writeInt(handle.getKeyGroupRange().getNumberOfKeyGroups());
            dos.writeLong(handle.getFrom());
            dos.writeLong(handle.getTo());
            dos.writeInt(handle.getChanges().size());
            for (StateChange change : handle.getChanges()) {
                dos.writeInt(change.getKeyGroup());
                dos.writeInt(change.getChange().length);
                dos.write(change.getChange());
            }
            writeStateHandleId(handle, dos);
        } else if (stateHandle instanceof ChangelogStateHandleStreamImpl) {
            ChangelogStateHandleStreamImpl handle = (ChangelogStateHandleStreamImpl) stateHandle;
            dos.writeByte(CHANGELOG_FILE_INCREMENT_HANDLE_V2);
            dos.writeInt(handle.getKeyGroupRange().getStartKeyGroup());
            dos.writeInt(handle.getKeyGroupRange().getNumberOfKeyGroups());
            dos.writeInt(handle.getHandlesAndOffsets().size());
            for (Tuple2<StreamStateHandle, Long> streamHandleAndOffset :
                    handle.getHandlesAndOffsets()) {
                dos.writeLong(streamHandleAndOffset.f1);
                serializeStreamStateHandle(streamHandleAndOffset.f0, dos);
            }
            dos.writeLong(handle.getStateSize());
            dos.writeLong(handle.getCheckpointedSize());
            writeStateHandleId(handle, dos);
            dos.writeUTF(handle.getStorageIdentifier());
        } else {
            throw new IllegalStateException(
                    "Unknown KeyedStateHandle type: " + stateHandle.getClass());
        }
    }

    private static void writeStateHandleId(KeyedStateHandle keyedStateHandle, DataOutputStream dos)
            throws IOException {
        dos.writeUTF(keyedStateHandle.getStateHandleId().getKeyString());
    }

    @VisibleForTesting
    @Nullable
    static KeyedStateHandle deserializeKeyedStateHandle(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {

        final int type = dis.readByte();
        if (NULL_HANDLE == type) {

            return null;
        } else if (KEY_GROUPS_HANDLE == type
                || KEY_GROUPS_HANDLE_V2 == type
                || SAVEPOINT_KEY_GROUPS_HANDLE == type) {
            int startKeyGroup = dis.readInt();
            int numKeyGroups = dis.readInt();
            KeyGroupRange keyGroupRange =
                    KeyGroupRange.of(startKeyGroup, startKeyGroup + numKeyGroups - 1);
            long[] offsets = new long[numKeyGroups];
            for (int i = 0; i < numKeyGroups; ++i) {
                offsets[i] = dis.readLong();
            }
            KeyGroupRangeOffsets keyGroupRangeOffsets =
                    new KeyGroupRangeOffsets(keyGroupRange, offsets);
            StreamStateHandle stateHandle = deserializeStreamStateHandle(dis, context);
            if (SAVEPOINT_KEY_GROUPS_HANDLE == type) {
                return new KeyGroupsSavepointStateHandle(keyGroupRangeOffsets, stateHandle);
            } else {
                StateHandleID stateHandleID =
                        KEY_GROUPS_HANDLE_V2 == type
                                ? new StateHandleID(dis.readUTF())
                                : StateHandleID.randomStateHandleId();
                return KeyGroupsStateHandle.restore(
                        keyGroupRangeOffsets, stateHandle, stateHandleID);
            }
        } else if (INCREMENTAL_KEY_GROUPS_HANDLE == type
                || INCREMENTAL_KEY_GROUPS_HANDLE_V2 == type) {
            return deserializeIncrementalStateHandle(dis, context, type);
        } else if (CHANGELOG_HANDLE == type || CHANGELOG_HANDLE_V2 == type) {

            int startKeyGroup = dis.readInt();
            int numKeyGroups = dis.readInt();
            KeyGroupRange keyGroupRange =
                    KeyGroupRange.of(startKeyGroup, startKeyGroup + numKeyGroups - 1);
            long checkpointedSize = dis.readLong();

            int baseSize = dis.readInt();
            List<KeyedStateHandle> base = new ArrayList<>(baseSize);
            for (int i = 0; i < baseSize; i++) {
                KeyedStateHandle handle = deserializeKeyedStateHandle(dis, context);
                if (handle != null) {
                    base.add(handle);
                } else {
                    LOG.warn(
                            "Unexpected null keyed state handle of materialized part when deserializing changelog state-backend handle");
                }
            }
            int deltaSize = dis.readInt();
            List<ChangelogStateHandle> delta = new ArrayList<>(deltaSize);
            for (int i = 0; i < deltaSize; i++) {
                delta.add((ChangelogStateHandle) deserializeKeyedStateHandle(dis, context));
            }

            long materializationID = dis.readLong();
            long checkpointId = CHANGELOG_HANDLE_V2 == type ? dis.readLong() : materializationID;
            StateHandleID stateHandleId = new StateHandleID(dis.readUTF());
            return ChangelogStateBackendHandleImpl.restore(
                    base,
                    delta,
                    keyGroupRange,
                    checkpointId,
                    materializationID,
                    checkpointedSize,
                    stateHandleId);

        } else if (CHANGELOG_BYTE_INCREMENT_HANDLE == type) {
            int start = dis.readInt();
            int numKeyGroups = dis.readInt();
            KeyGroupRange keyGroupRange = KeyGroupRange.of(start, start + numKeyGroups - 1);
            long from = dis.readLong();
            long to = dis.readLong();
            int size = dis.readInt();
            List<StateChange> changes = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int keyGroup = dis.readInt();
                int bytesSize = dis.readInt();
                byte[] bytes = new byte[bytesSize];
                IOUtils.readFully(dis, bytes, 0, bytesSize);
                StateChange stateChange =
                        keyGroup == META_KEY_GROUP
                                ? StateChange.ofMetadataChange(bytes)
                                : StateChange.ofDataChange(keyGroup, bytes);
                changes.add(stateChange);
            }
            StateHandleID stateHandleId = new StateHandleID(dis.readUTF());
            return InMemoryChangelogStateHandle.restore(
                    changes,
                    SequenceNumber.of(from),
                    SequenceNumber.of(to),
                    keyGroupRange,
                    stateHandleId);

        } else if (CHANGELOG_FILE_INCREMENT_HANDLE == type
                || CHANGELOG_FILE_INCREMENT_HANDLE_V2 == type) {
            int start = dis.readInt();
            int numKeyGroups = dis.readInt();
            KeyGroupRange keyGroupRange = KeyGroupRange.of(start, start + numKeyGroups - 1);
            int numHandles = dis.readInt();
            List<Tuple2<StreamStateHandle, Long>> streamHandleAndOffset =
                    new ArrayList<>(numHandles);
            for (int i = 0; i < numHandles; i++) {
                long o = dis.readLong();
                StreamStateHandle h = deserializeStreamStateHandle(dis, context);
                streamHandleAndOffset.add(Tuple2.of(h, o));
            }
            long size = dis.readLong();
            long checkpointedSize = dis.readLong();
            StateHandleID stateHandleId = new StateHandleID(dis.readUTF());
            String storageIdentifier =
                    CHANGELOG_FILE_INCREMENT_HANDLE_V2 == type ? dis.readUTF() : "filesystem";
            return ChangelogStateHandleStreamImpl.restore(
                    streamHandleAndOffset,
                    keyGroupRange,
                    size,
                    checkpointedSize,
                    storageIdentifier,
                    stateHandleId);
        } else {
            throw new IllegalStateException("Reading invalid KeyedStateHandle, type: " + type);
        }
    }

    private static IncrementalRemoteKeyedStateHandle deserializeIncrementalStateHandle(
            DataInputStream dis, @Nullable DeserializationContext context, int stateHandleType)
            throws IOException {
        boolean isV2Format = INCREMENTAL_KEY_GROUPS_HANDLE_V2 == stateHandleType;
        long checkpointId = dis.readLong();
        String backendId = dis.readUTF();
        int startKeyGroup = dis.readInt();
        int numKeyGroups = dis.readInt();
        long checkpointedSize = isV2Format ? dis.readLong() : UNKNOWN_CHECKPOINTED_SIZE;

        KeyGroupRange keyGroupRange =
                KeyGroupRange.of(startKeyGroup, startKeyGroup + numKeyGroups - 1);

        StreamStateHandle metaDataStateHandle = deserializeStreamStateHandle(dis, context);
        List<HandleAndLocalPath> sharedStates = deserializeHandleAndLocalPathList(dis, context);
        List<HandleAndLocalPath> privateStates = deserializeHandleAndLocalPathList(dis, context);

        UUID uuid;

        try {
            uuid = UUID.fromString(backendId);
        } catch (Exception ex) {
            // compatibility with old format pre FLINK-6964:
            uuid = UUID.nameUUIDFromBytes(backendId.getBytes(StandardCharsets.UTF_8));
        }

        StateHandleID stateHandleId =
                isV2Format ? new StateHandleID(dis.readUTF()) : StateHandleID.randomStateHandleId();
        return IncrementalRemoteKeyedStateHandle.restore(
                uuid,
                keyGroupRange,
                checkpointId,
                sharedStates,
                privateStates,
                metaDataStateHandle,
                checkpointedSize,
                stateHandleId);
    }

    void serializeOperatorStateHandle(OperatorStateHandle stateHandle, DataOutputStream dos)
            throws IOException {
        if (stateHandle != null) {
            dos.writeByte(PARTITIONABLE_OPERATOR_STATE_HANDLE);
            Map<String, OperatorStateHandle.StateMetaInfo> partitionOffsetsMap =
                    stateHandle.getStateNameToPartitionOffsets();
            dos.writeInt(partitionOffsetsMap.size());
            for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> entry :
                    partitionOffsetsMap.entrySet()) {
                dos.writeUTF(entry.getKey());

                OperatorStateHandle.StateMetaInfo stateMetaInfo = entry.getValue();

                int mode = stateMetaInfo.getDistributionMode().ordinal();
                dos.writeByte(mode);

                long[] offsets = stateMetaInfo.getOffsets();
                dos.writeInt(offsets.length);
                for (long offset : offsets) {
                    dos.writeLong(offset);
                }
            }
            serializeStreamStateHandle(stateHandle.getDelegateStateHandle(), dos);
        } else {
            dos.writeByte(NULL_HANDLE);
        }
    }

    OperatorStateHandle deserializeOperatorStateHandle(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {

        final int type = dis.readByte();
        if (NULL_HANDLE == type) {
            return null;
        } else if (PARTITIONABLE_OPERATOR_STATE_HANDLE == type) {
            int mapSize = dis.readInt();
            Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap =
                    CollectionUtil.newHashMapWithExpectedSize(mapSize);
            for (int i = 0; i < mapSize; ++i) {
                String key = dis.readUTF();

                int modeOrdinal = dis.readByte();
                OperatorStateHandle.Mode mode = OperatorStateHandle.Mode.values()[modeOrdinal];

                long[] offsets = new long[dis.readInt()];
                for (int j = 0; j < offsets.length; ++j) {
                    offsets[j] = dis.readLong();
                }

                OperatorStateHandle.StateMetaInfo metaInfo =
                        new OperatorStateHandle.StateMetaInfo(offsets, mode);
                offsetsMap.put(key, metaInfo);
            }
            StreamStateHandle stateHandle = deserializeStreamStateHandle(dis, context);
            return new OperatorStreamStateHandle(offsetsMap, stateHandle);
        } else {
            throw new IllegalStateException("Reading invalid OperatorStateHandle, type: " + type);
        }
    }

    // ------------------------------------------------------------------------
    //  channel state (unaligned checkpoints)
    // ------------------------------------------------------------------------

    protected StateObjectCollection<ResultSubpartitionStateHandle>
            deserializeResultSubpartitionStateHandle(
                    DataInputStream dis, @Nullable DeserializationContext context)
                    throws IOException {
        return StateObjectCollection.empty();
    }

    protected StateObjectCollection<InputChannelStateHandle> deserializeInputChannelStateHandle(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {
        return StateObjectCollection.empty();
    }

    protected void serializeResultSubpartitionStateHandle(
            ResultSubpartitionStateHandle resultSubpartitionStateHandle, DataOutputStream dos)
            throws IOException {}

    protected void serializeInputChannelStateHandle(
            InputChannelStateHandle inputChannelStateHandle, DataOutputStream dos)
            throws IOException {}

    // ------------------------------------------------------------------------
    //  low-level state handles
    // ------------------------------------------------------------------------

    static void serializeStreamStateHandle(StreamStateHandle stateHandle, DataOutputStream dos)
            throws IOException {
        if (stateHandle == null) {
            dos.writeByte(NULL_HANDLE);

        } else if (stateHandle instanceof RelativeFileStateHandle) {
            dos.writeByte(RELATIVE_STREAM_STATE_HANDLE);
            RelativeFileStateHandle relativeFileStateHandle = (RelativeFileStateHandle) stateHandle;
            dos.writeUTF(relativeFileStateHandle.getRelativePath());
            dos.writeLong(relativeFileStateHandle.getStateSize());
        } else if (stateHandle instanceof FileStateHandle) {
            dos.writeByte(FILE_STREAM_STATE_HANDLE);
            FileStateHandle fileStateHandle = (FileStateHandle) stateHandle;
            dos.writeLong(stateHandle.getStateSize());
            dos.writeUTF(fileStateHandle.getFilePath().toString());

        } else if (stateHandle instanceof ByteStreamStateHandle) {
            dos.writeByte(BYTE_STREAM_STATE_HANDLE);
            ByteStreamStateHandle byteStreamStateHandle = (ByteStreamStateHandle) stateHandle;
            dos.writeUTF(byteStreamStateHandle.getHandleName());
            byte[] internalData = byteStreamStateHandle.getData();
            dos.writeInt(internalData.length);
            dos.write(byteStreamStateHandle.getData());
        } else if (stateHandle instanceof KeyGroupsStateHandle) {
            KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) stateHandle;
            dos.writeByte(KEY_GROUPS_HANDLE);
            dos.writeInt(keyGroupsStateHandle.getKeyGroupRange().getStartKeyGroup());
            dos.writeInt(keyGroupsStateHandle.getKeyGroupRange().getNumberOfKeyGroups());
            for (int keyGroup : keyGroupsStateHandle.getKeyGroupRange()) {
                dos.writeLong(keyGroupsStateHandle.getOffsetForKeyGroup(keyGroup));
            }
            serializeStreamStateHandle(keyGroupsStateHandle.getDelegateStateHandle(), dos);
        } else {
            throw new IOException(
                    "Unknown implementation of StreamStateHandle: " + stateHandle.getClass());
        }
    }

    @Nullable
    static StreamStateHandle deserializeStreamStateHandle(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {

        final int type = dis.read();
        if (NULL_HANDLE == type) {
            return null;
        } else if (FILE_STREAM_STATE_HANDLE == type) {
            long size = dis.readLong();
            String pathString = dis.readUTF();
            return new FileStateHandle(new Path(pathString), size);
        } else if (BYTE_STREAM_STATE_HANDLE == type) {
            String handleName = dis.readUTF();
            int numBytes = dis.readInt();
            byte[] data = new byte[numBytes];
            dis.readFully(data);
            return new ByteStreamStateHandle(handleName, data);
        } else if (RELATIVE_STREAM_STATE_HANDLE == type) {
            if (context == null) {
                throw new IOException(
                        "Cannot deserialize a RelativeFileStateHandle without a context to make it relative to.");
            }
            String relativePath = dis.readUTF();
            long size = dis.readLong();
            Path statePath = new Path(context.getExclusiveDirPath(), relativePath);
            return new RelativeFileStateHandle(statePath, relativePath, size);
        } else if (KEY_GROUPS_HANDLE == type) {

            int startKeyGroup = dis.readInt();
            int numKeyGroups = dis.readInt();
            KeyGroupRange keyGroupRange =
                    KeyGroupRange.of(startKeyGroup, startKeyGroup + numKeyGroups - 1);
            long[] offsets = new long[numKeyGroups];
            for (int i = 0; i < numKeyGroups; ++i) {
                offsets[i] = dis.readLong();
            }
            KeyGroupRangeOffsets keyGroupRangeOffsets =
                    new KeyGroupRangeOffsets(keyGroupRange, offsets);
            StreamStateHandle stateHandle = deserializeStreamStateHandle(dis, context);
            return new KeyGroupsStateHandle(keyGroupRangeOffsets, stateHandle);
        } else {
            throw new IOException("Unknown implementation of StreamStateHandle, code: " + type);
        }
    }

    @Nullable
    static ByteStreamStateHandle deserializeAndCheckByteStreamStateHandle(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {

        final StreamStateHandle handle = deserializeStreamStateHandle(dis, context);
        if (handle == null || handle instanceof ByteStreamStateHandle) {
            return (ByteStreamStateHandle) handle;
        } else {
            throw new IOException(
                    "Expected a ByteStreamStateHandle but found a " + handle.getClass().getName());
        }
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    @Nullable
    private static <T> T extractSingleton(Collection<T> collection) {
        if (collection == null || collection.isEmpty()) {
            return null;
        }

        if (collection.size() == 1) {
            return collection.iterator().next();
        } else {
            throw new IllegalStateException(
                    "Expected singleton collection, but found size: " + collection.size());
        }
    }

    private static <T extends StateObject> void serializeSingleton(
            StateObjectCollection<T> stateObjectCollection,
            DataOutputStream dos,
            BiConsumerWithException<T, DataOutputStream, IOException> cons)
            throws IOException {
        final T state = extractSingleton(stateObjectCollection);
        if (state != null) {
            dos.writeInt(1);
            cons.accept(state, dos);
        } else {
            dos.writeInt(0);
        }
    }

    static <T extends StateObject> StateObjectCollection<T> deserializeCollection(
            DataInputStream dis,
            DeserializationContext context,
            BiFunctionWithException<DataInputStream, DeserializationContext, T, IOException> s)
            throws IOException {

        int size = dis.readInt();
        List<T> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(s.apply(dis, context));
        }
        return new StateObjectCollection<>(result);
    }

    private static void serializeHandleAndLocalPathList(
            List<HandleAndLocalPath> list, DataOutputStream dos) throws IOException {

        dos.writeInt(list.size());
        for (HandleAndLocalPath handleAndLocalPath : list) {
            dos.writeUTF(handleAndLocalPath.getLocalPath());
            serializeStreamStateHandle(handleAndLocalPath.getHandle(), dos);
        }
    }

    private static List<HandleAndLocalPath> deserializeHandleAndLocalPathList(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {

        final int size = dis.readInt();
        List<HandleAndLocalPath> result = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            String localPath = dis.readUTF();
            StreamStateHandle stateHandle = deserializeStreamStateHandle(dis, context);
            result.add(HandleAndLocalPath.of(stateHandle, localPath));
        }

        return result;
    }

    // ------------------------------------------------------------------------
    //  internal helper classes
    // ------------------------------------------------------------------------

    /**
     * A context that keeps information needed during serialization. This context is passed along by
     * the methods. In some sense, this replaces the member fields of the class, because the
     * serializer is supposed to be "singleton stateless", and because there are multiple instances
     * involved (metadata serializer, channel state serializer).
     *
     * <p>The alternative to passing this context along would be to change the serializers to work
     * as actual instances so that they can keep the state. We might still want to do that, but at
     * the time of implementing this, it seems the less invasive change to use this context, and it
     * also works with static methods and with different serializers instances that do not know of
     * each other.
     *
     * <p>This context is currently hardwired to the FileSystem-based State Backends. At the moment,
     * this works because those are the only ones producing relative file paths handles, which are
     * in turn the only ones needing this context. In the future, we should refactor this, though,
     * and make the DeserializationContext a property of the used checkpoint storage. That makes
     */
    protected static final class DeserializationContext {

        private final String externalPointer;

        private Path cachedExclusiveDirPath;

        DeserializationContext(String externalPointer) {
            this.externalPointer = externalPointer;
        }

        Path getExclusiveDirPath() throws IOException {
            if (cachedExclusiveDirPath == null) {
                cachedExclusiveDirPath = createExclusiveDirPath(externalPointer);
            }
            return cachedExclusiveDirPath;
        }

        private static Path createExclusiveDirPath(String externalPointer) throws IOException {
            try {
                return AbstractFsCheckpointStorageAccess.resolveCheckpointPointer(externalPointer)
                        .getExclusiveCheckpointDir();
            } catch (IOException e) {
                throw new IOException("Could not parse external pointer as state base path", e);
            }
        }
    }
}
