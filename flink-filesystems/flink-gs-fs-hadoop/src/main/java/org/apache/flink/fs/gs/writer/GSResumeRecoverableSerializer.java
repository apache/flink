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
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

/** The serializer for the recoverable writer state. */
class GSResumeRecoverableSerializer implements SimpleVersionedSerializer<GSResumeRecoverable> {

    /** Current version of serializer. */
    private static final int SERIALIZER_VERSION = 0;

    /** The one and only instance of the serializer. */
    public static final GSResumeRecoverableSerializer INSTANCE =
            new GSResumeRecoverableSerializer();

    private GSResumeRecoverableSerializer() {}

    @Override
    public int getVersion() {
        return SERIALIZER_VERSION;
    }

    @Override
    public byte[] serialize(GSResumeRecoverable recoverable) throws IOException {
        Preconditions.checkNotNull(recoverable);

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {

                // finalBlobIdentifier
                dataOutputStream.writeUTF(recoverable.finalBlobIdentifier.bucketName);
                dataOutputStream.writeUTF(recoverable.finalBlobIdentifier.objectName);

                // position
                dataOutputStream.writeLong(recoverable.position);

                // closed
                dataOutputStream.writeBoolean(recoverable.closed);

                // componentObjectIds
                dataOutputStream.writeInt(recoverable.componentObjectIds.length);
                for (UUID componentObjectId : recoverable.componentObjectIds) {
                    dataOutputStream.writeLong(componentObjectId.getMostSignificantBits());
                    dataOutputStream.writeLong(componentObjectId.getLeastSignificantBits());
                }
            }

            outputStream.flush();
            return outputStream.toByteArray();
        }
    }

    @Override
    public GSResumeRecoverable deserialize(int version, byte[] serialized) throws IOException {
        Preconditions.checkArgument(version >= 0);
        Preconditions.checkNotNull(serialized);

        // ensure this serializer can deserialize data with this version
        if (version > SERIALIZER_VERSION) {
            throw new IOException(
                    String.format(
                            "Serialized data with version %d cannot be read by serializer with version %d",
                            version, SERIALIZER_VERSION));
        }

        GSBlobIdentifier finalBlobIdentifier;
        long position;
        boolean closed;
        ArrayList<UUID> componentObjectIds = new ArrayList<>();

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized)) {

            try (DataInputStream dataInputStream = new DataInputStream(inputStream)) {

                // finalBlobId
                String finalBucketName = dataInputStream.readUTF();
                String finalObjectName = dataInputStream.readUTF();
                finalBlobIdentifier = new GSBlobIdentifier(finalBucketName, finalObjectName);

                // position
                position = dataInputStream.readLong();

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

        return new GSResumeRecoverable(finalBlobIdentifier, position, closed, componentObjectIds);
    }
}
