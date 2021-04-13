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

package org.apache.flink.fs.gs.writer;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import com.google.cloud.storage.BlobId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** The serializer for the recoverable writer state. */
class GSRecoverableWriterStateSerializer
        implements SimpleVersionedSerializer<GSRecoverableWriterState> {

    /** Current version of serializer. */
    private static final int SERIALIZER_VERSION = 0;

    /** The one and only instance of the serializer. */
    public static final GSRecoverableWriterStateSerializer INSTANCE =
            new GSRecoverableWriterStateSerializer();

    private GSRecoverableWriterStateSerializer() {}

    @Override
    public int getVersion() {
        return SERIALIZER_VERSION;
    }

    @Override
    public byte[] serialize(GSRecoverableWriterState state) throws IOException {
        Preconditions.checkNotNull(state);

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {

                // finalBlobId
                dataOutputStream.writeUTF(state.finalBlobId.getBucket());
                dataOutputStream.writeUTF(state.finalBlobId.getName());

                // bytesWritten
                dataOutputStream.writeLong(state.bytesWritten);

                // closed
                dataOutputStream.writeBoolean(state.closed);

                // componentObjectIds
                dataOutputStream.writeInt(state.componentObjectIds.size());
                for (UUID componentObjectId : state.componentObjectIds) {
                    dataOutputStream.writeLong(componentObjectId.getMostSignificantBits());
                    dataOutputStream.writeLong(componentObjectId.getLeastSignificantBits());
                }
            }

            outputStream.flush();
            return outputStream.toByteArray();
        }
    }

    @Override
    public GSRecoverableWriterState deserialize(int version, byte[] serialized) throws IOException {
        Preconditions.checkArgument(version >= 0);
        Preconditions.checkNotNull(serialized);

        // ensure this serializer can deserialize data with this version
        if (version > SERIALIZER_VERSION) {
            throw new IOException(
                    String.format(
                            "Serialized data with version %d cannot be read by serializer with version %d",
                            version, SERIALIZER_VERSION));
        }

        BlobId finalBlobId;
        long bytesWritten;
        boolean closed;
        List<UUID> componentObjectIds = new ArrayList<>();

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized)) {

            try (DataInputStream dataInputStream = new DataInputStream(inputStream)) {

                // finalBlobId
                String finalBucketName = dataInputStream.readUTF();
                String finalObjectName = dataInputStream.readUTF();
                finalBlobId = BlobId.of(finalBucketName, finalObjectName);

                // bytesWritten
                bytesWritten = dataInputStream.readLong();

                // closed
                closed = dataInputStream.readBoolean();

                // componentObjectIds
                int count = dataInputStream.readInt();
                for (int i = 0; i < count; i++) {
                    long msbValue = dataInputStream.readLong();
                    long lsbValue = dataInputStream.readLong();
                    UUID componentObjectId = new UUID(msbValue, lsbValue);
                    componentObjectIds.add(componentObjectId);
                }
            }
        }

        return new GSRecoverableWriterState(finalBlobId, bytesWritten, closed, componentObjectIds);
    }
}
