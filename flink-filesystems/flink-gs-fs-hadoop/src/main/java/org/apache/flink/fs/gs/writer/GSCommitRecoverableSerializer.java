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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

/** The serializer for the recoverable writer state. */
class GSCommitRecoverableSerializer implements SimpleVersionedSerializer<GSCommitRecoverable> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(GSCommitRecoverableSerializer.class);

    /** Current version of serializer. */
    private static final int SERIALIZER_VERSION = 1;

    /** The one and only instance of the serializer. */
    public static final GSCommitRecoverableSerializer INSTANCE =
            new GSCommitRecoverableSerializer();

    private GSCommitRecoverableSerializer() {}

    /**
     * The serializer version. Note that, if this changes, then the version of {@link
     * GSResumeRecoverableSerializer} must also change, because it uses this class to serialize
     * itself, in part.
     *
     * @return The serializer version.
     */
    @Override
    public int getVersion() {
        return SERIALIZER_VERSION;
    }

    /**
     * Writes a commit recoverable to a data output stream.
     *
     * @param recoverable The commit recoverable
     * @param dataOutputStream The data output stream
     * @throws IOException On underlyilng failure
     */
    static void serializeCommitRecoverable(
            GSCommitRecoverable recoverable, DataOutputStream dataOutputStream) throws IOException {

        // finalBlobIdentifier
        dataOutputStream.writeUTF(recoverable.finalBlobIdentifier.bucketName);
        dataOutputStream.writeUTF(recoverable.finalBlobIdentifier.objectName);

        // componentObjectIds
        dataOutputStream.writeInt(recoverable.componentObjectIds.size());
        for (UUID componentObjectId : recoverable.componentObjectIds) {
            dataOutputStream.writeLong(componentObjectId.getMostSignificantBits());
            dataOutputStream.writeLong(componentObjectId.getLeastSignificantBits());
        }
    }

    @Override
    public byte[] serialize(GSCommitRecoverable recoverable) throws IOException {

        LOGGER.trace("Serializing recoverable {}", recoverable);
        Preconditions.checkNotNull(recoverable);

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                serializeCommitRecoverable(recoverable, dataOutputStream);
            }

            outputStream.flush();
            return outputStream.toByteArray();
        }
    }

    /**
     * Deserializes a commit recoverable from the input stream.
     *
     * @param dataInputStream The input stream
     * @return The commit recoverable
     * @throws IOException On underlying failure
     */
    static GSCommitRecoverable deserializeCommitRecoverable(DataInputStream dataInputStream)
            throws IOException {

        // finalBlobId
        String finalBucketName = dataInputStream.readUTF();
        String finalObjectName = dataInputStream.readUTF();
        GSBlobIdentifier finalBlobIdentifier =
                new GSBlobIdentifier(finalBucketName, finalObjectName);

        // componentObjectIds
        ArrayList<UUID> componentObjectIds = new ArrayList<>();
        int count = dataInputStream.readInt();
        for (int i = 0; i < count; i++) {
            long msbValue = dataInputStream.readLong();
            long lsbValue = dataInputStream.readLong();
            UUID componentObjectId = new UUID(msbValue, lsbValue);
            componentObjectIds.add(componentObjectId);
        }

        GSCommitRecoverable recoverable =
                new GSCommitRecoverable(finalBlobIdentifier, componentObjectIds);
        LOGGER.trace("Deserialized commit recoverable {}", recoverable);
        return recoverable;
    }

    @Override
    public GSCommitRecoverable deserialize(int version, byte[] serialized) throws IOException {
        Preconditions.checkArgument(version > 0);
        Preconditions.checkNotNull(serialized);

        // ensure this serializer can deserialize data with this version
        if (version > SERIALIZER_VERSION) {
            throw new IOException(
                    String.format(
                            "Serialized data with version %d cannot be read by serializer with version %d",
                            version, SERIALIZER_VERSION));
        }

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized)) {
            try (DataInputStream dataInputStream = new DataInputStream(inputStream)) {
                return deserializeCommitRecoverable(dataInputStream);
            }
        }
    }
}
