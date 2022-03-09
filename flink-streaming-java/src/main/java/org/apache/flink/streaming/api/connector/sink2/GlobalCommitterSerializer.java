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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollectorSerializer;
import org.apache.flink.streaming.runtime.operators.sink.committables.SinkV1CommittableDeserializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@Internal
class GlobalCommitterSerializer<CommT, GlobalCommT>
        implements SimpleVersionedSerializer<GlobalCommittableWrapper<CommT, GlobalCommT>> {

    private static final int MAGIC_NUMBER = 0xb91f252b;

    private final CommittableCollectorSerializer<CommT> committableCollectorSerializer;
    @Nullable private final SimpleVersionedSerializer<GlobalCommT> globalCommittableSerializer;
    private final int subtaskId;
    private final int numberOfSubtasks;

    GlobalCommitterSerializer(
            CommittableCollectorSerializer<CommT> committableCollectorSerializer,
            @Nullable SimpleVersionedSerializer<GlobalCommT> globalCommittableSerializer,
            int subtaskId,
            int numberOfSubtasks) {
        this.committableCollectorSerializer = checkNotNull(committableCollectorSerializer);
        this.globalCommittableSerializer = globalCommittableSerializer;
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
    }

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(GlobalCommittableWrapper<CommT, GlobalCommT> obj) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        if (globalCommittableSerializer != null) {
            out.writeBoolean(true);
            final Collection<GlobalCommT> globalCommittables = obj.getGlobalCommittables();
            SimpleVersionedSerialization.writeVersionAndSerializeList(
                    globalCommittableSerializer, new ArrayList<>(globalCommittables), out);
        } else {
            out.writeBoolean(false);
        }
        SimpleVersionedSerialization.writeVersionAndSerialize(
                committableCollectorSerializer, obj.getCommittableCollector(), out);
        return out.getCopyOfBuffer();
    }

    @Override
    public GlobalCommittableWrapper<CommT, GlobalCommT> deserialize(int version, byte[] serialized)
            throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        if (version == 1) {
            if (globalCommittableSerializer == null) {
                throw new IllegalStateException(
                        "Tried to deserialize Sink V1 state without a GlobalCommittable serializer.");
            }
            return deserializeV1(in);
        }
        if (version == 2) {
            validateMagicNumber(in);
            return deserializeV2(in);
        }
        throw new IllegalStateException("Unrecognized version or corrupt state: " + version);
    }

    private GlobalCommittableWrapper<CommT, GlobalCommT> deserializeV1(DataInputView in)
            throws IOException {
        final List<GlobalCommT> globalCommittables =
                SinkV1CommittableDeserializer.readVersionAndDeserializeList(
                        globalCommittableSerializer, in);
        return new GlobalCommittableWrapper<>(
                new CommittableCollector<>(subtaskId, numberOfSubtasks), globalCommittables);
    }

    private GlobalCommittableWrapper<CommT, GlobalCommT> deserializeV2(DataInputView in)
            throws IOException {
        final boolean withGlobalCommittableSerializer = in.readBoolean();
        List<GlobalCommT> globalCommittables;
        if (globalCommittableSerializer == null) {
            checkState(
                    !withGlobalCommittableSerializer,
                    "Trying to recover state from a GlobalCommittable serializer without specifying one.");
            globalCommittables = Collections.emptyList();
        } else {
            globalCommittables =
                    SimpleVersionedSerialization.readVersionAndDeserializeList(
                            globalCommittableSerializer, in);
        }
        return new GlobalCommittableWrapper<>(
                SimpleVersionedSerialization.readVersionAndDeSerialize(
                        committableCollectorSerializer, in),
                globalCommittables);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        final int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IllegalStateException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}
