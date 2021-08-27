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

package org.apache.flink.connector.file.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Versioned serializer for {@link FileSinkCommittable}. */
@Internal
public class FileSinkCommittableSerializer
        implements SimpleVersionedSerializer<FileSinkCommittable> {

    private static final int MAGIC_NUMBER = 0x1e765c80;

    private final SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
            pendingFileSerializer;

    private final SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable>
            inProgressFileSerializer;

    public FileSinkCommittableSerializer(
            SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
                    pendingFileSerializer,
            SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable>
                    inProgressFileSerializer) {
        this.pendingFileSerializer = checkNotNull(pendingFileSerializer);
        this.inProgressFileSerializer = checkNotNull(inProgressFileSerializer);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(FileSinkCommittable committable) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV1(committable, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public FileSinkCommittable deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        switch (version) {
            case 1:
                validateMagicNumber(in);
                return deserializeV1(in);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private void serializeV1(FileSinkCommittable committable, DataOutputView dataOutputView)
            throws IOException {

        if (committable.hasPendingFile()) {
            dataOutputView.writeBoolean(true);
            SimpleVersionedSerialization.writeVersionAndSerialize(
                    pendingFileSerializer, committable.getPendingFile(), dataOutputView);
        } else {
            dataOutputView.writeBoolean(false);
        }

        if (committable.hasInProgressFileToCleanup()) {
            dataOutputView.writeBoolean(true);
            SimpleVersionedSerialization.writeVersionAndSerialize(
                    inProgressFileSerializer,
                    committable.getInProgressFileToCleanup(),
                    dataOutputView);
        } else {
            dataOutputView.writeBoolean(false);
        }
    }

    private FileSinkCommittable deserializeV1(DataInputView dataInputView) throws IOException {
        InProgressFileWriter.PendingFileRecoverable pendingFile = null;
        if (dataInputView.readBoolean()) {
            pendingFile =
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            pendingFileSerializer, dataInputView);
        }

        InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup = null;
        if (dataInputView.readBoolean()) {
            inProgressFileToCleanup =
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            inProgressFileSerializer, dataInputView);
        }

        return new FileSinkCommittable(pendingFile, inProgressFileToCleanup);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}
