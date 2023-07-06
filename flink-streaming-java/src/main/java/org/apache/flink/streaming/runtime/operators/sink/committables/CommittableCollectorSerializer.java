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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.CollectionUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The serializer for the {@link CommittableCollector}. Compatible to 1.14- StreamingCommitterState.
 */
@Internal
public final class CommittableCollectorSerializer<CommT>
        implements SimpleVersionedSerializer<CommittableCollector<CommT>> {

    private static final int MAGIC_NUMBER = 0xb91f252c;

    private final SimpleVersionedSerializer<CommT> committableSerializer;
    private final int subtaskId;
    private final int numberOfSubtasks;

    public CommittableCollectorSerializer(
            SimpleVersionedSerializer<CommT> committableSerializer,
            int subtaskId,
            int numberOfSubtasks) {
        this.committableSerializer = checkNotNull(committableSerializer);
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
    }

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(CommittableCollector<CommT> committableCollector) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV2(committableCollector, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public CommittableCollector<CommT> deserialize(int version, byte[] serialized)
            throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        if (version == 1) {
            return deserializeV1(in);
        }
        if (version == 2) {
            validateMagicNumber(in);
            return deserializeV2(in);
        }
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }

    private CommittableCollector<CommT> deserializeV1(DataInputView in) throws IOException {
        return CommittableCollector.ofLegacy(
                SinkV1CommittableDeserializer.readVersionAndDeserializeList(
                        committableSerializer, in));
    }

    private void serializeV2(
            CommittableCollector<CommT> committableCollector, DataOutputView dataOutputView)
            throws IOException {

        SimpleVersionedSerialization.writeVersionAndSerializeList(
                new CheckpointSimpleVersionedSerializer(),
                new ArrayList<>(committableCollector.getCheckpointCommittables()),
                dataOutputView);
    }

    private CommittableCollector<CommT> deserializeV2(DataInputDeserializer in) throws IOException {
        List<CheckpointCommittableManagerImpl<CommT>> checkpoints =
                SimpleVersionedSerialization.readVersionAndDeserializeList(
                        new CheckpointSimpleVersionedSerializer(), in);
        return new CommittableCollector<>(
                checkpoints.stream()
                        .collect(
                                Collectors.toMap(
                                        CheckpointCommittableManagerImpl::getCheckpointId, e -> e)),
                subtaskId,
                numberOfSubtasks);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        final int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }

    private class CheckpointSimpleVersionedSerializer
            implements SimpleVersionedSerializer<CheckpointCommittableManagerImpl<CommT>> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(CheckpointCommittableManagerImpl<CommT> checkpoint)
                throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            out.writeLong(checkpoint.getCheckpointId());
            SimpleVersionedSerialization.writeVersionAndSerializeList(
                    new SubtaskSimpleVersionedSerializer(),
                    new ArrayList<>(checkpoint.getSubtaskCommittableManagers()),
                    out);
            return out.getCopyOfBuffer();
        }

        @Override
        public CheckpointCommittableManagerImpl<CommT> deserialize(int version, byte[] serialized)
                throws IOException {

            DataInputDeserializer in = new DataInputDeserializer(serialized);
            long checkpointId = in.readLong();

            List<SubtaskCommittableManager<CommT>> subtaskCommittableManagers =
                    SimpleVersionedSerialization.readVersionAndDeserializeList(
                            new SubtaskSimpleVersionedSerializer(checkpointId), in);

            Map<Integer, SubtaskCommittableManager<CommT>> subtasksCommittableManagers =
                    CollectionUtil.newHashMapWithExpectedSize(subtaskCommittableManagers.size());

            for (SubtaskCommittableManager<CommT> subtaskCommittableManager :
                    subtaskCommittableManagers) {

                // check if we already have manager for current
                // subtaskCommittableManager.getSubtaskId() if yes,
                // then merge them.
                SubtaskCommittableManager<CommT> mergedManager =
                        subtasksCommittableManagers.computeIfPresent(
                                subtaskId,
                                (key, manager) -> manager.merge(subtaskCommittableManager));

                // This is new subtaskId, lets add the mapping.
                if (mergedManager == null) {
                    subtasksCommittableManagers.put(
                            subtaskCommittableManager.getSubtaskId(), subtaskCommittableManager);
                }
            }

            return new CheckpointCommittableManagerImpl<>(
                    subtasksCommittableManagers, subtaskId, numberOfSubtasks, checkpointId);
        }
    }

    private class SubtaskSimpleVersionedSerializer
            implements SimpleVersionedSerializer<SubtaskCommittableManager<CommT>> {

        @Nullable private final Long checkpointId;

        /**
         * This ctor must be used to create a deserializer where the checkpointId is used to set the
         * checkpointId of the deserialized SubtaskCommittableManager.
         *
         * @param checkpointId used to recover the SubtaskCommittableManager
         */
        public SubtaskSimpleVersionedSerializer(long checkpointId) {
            this.checkpointId = checkpointId;
        }

        /**
         * When using this ctor, you cannot use the serializer for deserialization because it misses
         * the checkpointId. For deserialization please use {@link
         * #SubtaskSimpleVersionedSerializer(long)}.
         */
        public SubtaskSimpleVersionedSerializer() {
            this.checkpointId = null;
        }

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(SubtaskCommittableManager<CommT> subtask) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            SimpleVersionedSerialization.writeVersionAndSerializeList(
                    new RequestSimpleVersionedSerializer(),
                    new ArrayList<>(subtask.getRequests()),
                    out);
            out.writeInt(subtask.getNumCommittables());
            out.writeInt(subtask.getNumDrained());
            out.writeInt(subtask.getNumFailed());
            return out.getCopyOfBuffer();
        }

        @Override
        public SubtaskCommittableManager<CommT> deserialize(int version, byte[] serialized)
                throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            List<CommitRequestImpl<CommT>> requests =
                    SimpleVersionedSerialization.readVersionAndDeserializeList(
                            new RequestSimpleVersionedSerializer(), in);
            return new SubtaskCommittableManager<>(
                    requests,
                    in.readInt(),
                    in.readInt(),
                    in.readInt(),
                    subtaskId,
                    checkNotNull(
                            checkpointId,
                            "CheckpointId must be set to align the SubtaskCommittableManager with holding CheckpointCommittableManager."));
        }

        private class RequestSimpleVersionedSerializer
                implements SimpleVersionedSerializer<CommitRequestImpl<CommT>> {
            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(CommitRequestImpl<CommT> request) throws IOException {
                DataOutputSerializer out = new DataOutputSerializer(256);
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        committableSerializer, request.getCommittable(), out);
                out.writeInt(request.getNumberOfRetries());
                out.writeInt(request.getState().ordinal());
                return out.getCopyOfBuffer();
            }

            @Override
            public CommitRequestImpl<CommT> deserialize(int version, byte[] serialized)
                    throws IOException {
                DataInputDeserializer in = new DataInputDeserializer(serialized);
                CommT committable =
                        SimpleVersionedSerialization.readVersionAndDeSerialize(
                                committableSerializer, in);
                return new CommitRequestImpl<>(
                        committable, in.readInt(), CommitRequestState.values()[in.readInt()]);
            }
        }
    }
}
