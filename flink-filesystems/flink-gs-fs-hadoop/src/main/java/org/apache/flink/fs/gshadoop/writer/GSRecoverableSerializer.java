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

package org.apache.flink.fs.gshadoop.writer;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import com.google.cloud.storage.BlobId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Serializer for {@link org.apache.flink.fs.gshadoop.writer.GSRecoverable}.
 *
 * <p>Note that this deliberately breaks the Flink project guidance to never use Java serialization;
 * the {@link com.google.cloud.RestorableState} instance that is used to persist states of the
 * {@link com.google.cloud.WriteChannel} is an opaque object that supports Java serialization, so
 * there seems to be no other way to serde it.
 */
class GSRecoverableSerializer implements SimpleVersionedSerializer<GSRecoverable> {

    private static final int SERIALIZER_VERSION = 0;

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    static final GSRecoverableSerializer INSTANCE = new GSRecoverableSerializer();

    /**
     * Helper to serialize a blob id, using the package name and object name directly.
     *
     * @param blobId The blob id
     * @return The serialized bytes
     */
    private static byte[] serializeBlobId(BlobId blobId) {
        byte[] bucketBytes = blobId.getBucket().getBytes(CHARSET);
        byte[] nameBytes = blobId.getName().getBytes(CHARSET);

        int bufferLength = 2 * Integer.BYTES + bucketBytes.length + nameBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(bufferLength);
        buffer.putInt(bucketBytes.length);
        buffer.put(bucketBytes);
        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);

        return buffer.array();
    }

    /**
     * Helper to deserializer a blob id.
     *
     * @param bytes The serialized bytes
     * @return The blob id
     */
    private static BlobId deserializeBlobId(byte[] bytes, int initialOffset, int length) {
        int offset = initialOffset;
        int bucketLength = ByteBuffer.wrap(bytes, offset, Integer.BYTES).getInt();
        offset += Integer.BYTES;
        String bucketName = CHARSET.decode(ByteBuffer.wrap(bytes, offset, bucketLength)).toString();
        offset += bucketLength;
        int objectNameLength = ByteBuffer.wrap(bytes, offset, Integer.BYTES).getInt();
        offset += Integer.BYTES;
        String objectName =
                CHARSET.decode(ByteBuffer.wrap(bytes, offset, objectNameLength)).toString();
        return BlobId.of(bucketName, objectName);
    }

    /**
     * Helper to serialize a GSWriterStateAdapter. This adapter wraps an opaque serializable object
     * from Google Storage, so we have to use Java serialization.
     *
     * @param writerState The writer state to serialize
     * @return Serialized byte array
     * @throws IOException On serialization failure
     */
    private static byte[] serializeWriterState(GSRecoverableWriterHelper.WriterState writerState)
            throws IOException {
        Preconditions.checkNotNull(writerState);

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
                objectOutputStream.writeObject(writerState);
                objectOutputStream.flush();
                return outputStream.toByteArray();
            }
        }
    }

    /**
     * Helper to deserialize a GSWriterStateAdapter. This adapter wraps an opaque serializable
     * object from Google Storage, so we have to use Java serialization.
     *
     * @param bytes The serialized bytes
     * @param offset The start point in the serialized bytes
     * @param length The length of the serialized data
     * @return The serializable object
     * @throws IOException If deserialization fails
     */
    private static GSRecoverableWriterHelper.WriterState deserializeWriterState(
            byte[] bytes, int offset, int length) throws IOException {
        Preconditions.checkNotNull(bytes);
        Preconditions.checkArgument(offset >= 0, "offset must be non-negative: %s", offset);
        Preconditions.checkArgument(length >= 0, "length must be non-negative: %s", length);

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes, offset, length)) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
                try {
                    return (GSRecoverableWriterHelper.WriterState) objectInputStream.readObject();
                } catch (ClassNotFoundException ex) {
                    throw new IOException(ex);
                }
            }
        }
    }

    @Override
    public int getVersion() {
        return SERIALIZER_VERSION;
    }

    @Override
    public byte[] serialize(GSRecoverable recoverable) throws IOException {
        Preconditions.checkNotNull(recoverable);

        // serialize these objects from the Google API
        byte[] tempBlobIdBytes = serializeBlobId(recoverable.plan.tempBlobId);
        byte[] finalBlobIdBytes = serializeBlobId(recoverable.plan.finalBlobId);
        byte[] writeStateBytes = serializeWriterState(recoverable.writerState);

        // total length is sum of lengths of:
        // 1) serialized temp blob id, length(int) + serialized length
        // 2) serialized final blob id, length(int) + serialized length
        // 3) serialized restorable state, length(int) + serialized length
        // 4) position (long)
        int serializedLength =
                Integer.BYTES * 3
                        + tempBlobIdBytes.length
                        + finalBlobIdBytes.length
                        + writeStateBytes.length
                        + Long.BYTES;

        ByteBuffer buffer = ByteBuffer.allocate(serializedLength);

        // tempBlobId
        buffer.putInt(tempBlobIdBytes.length);
        buffer.put(tempBlobIdBytes);

        // finalBlobId
        buffer.putInt(finalBlobIdBytes.length);
        buffer.put(finalBlobIdBytes);

        // writeState
        buffer.putInt(writeStateBytes.length);
        buffer.put(writeStateBytes);

        // position
        buffer.putLong(recoverable.position);

        return buffer.array();
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public GSRecoverable deserialize(int version, byte[] bytes) throws IOException {
        Preconditions.checkNotNull(bytes);
        // only support version 0 for now
        Preconditions.checkArgument(version == 0, "serialized version must be 0");

        // the current offset in the serialized byte array
        int offset = 0;

        // tempBlobId
        int tempBlobIdBytesLength = ByteBuffer.wrap(bytes, offset, Integer.BYTES).getInt();
        offset += Integer.BYTES;
        BlobId tempBlobId = (BlobId) deserializeBlobId(bytes, offset, tempBlobIdBytesLength);
        offset += tempBlobIdBytesLength;

        // finalBlobId
        int finalBlobIdBytesLength = ByteBuffer.wrap(bytes, offset, Integer.BYTES).getInt();
        offset += Integer.BYTES;
        BlobId finalBlobId = (BlobId) deserializeBlobId(bytes, offset, finalBlobIdBytesLength);
        offset += finalBlobIdBytesLength;

        // writeState
        int writerStateLength = ByteBuffer.wrap(bytes, offset, Integer.BYTES).getInt();
        offset += Integer.BYTES;
        GSRecoverableWriterHelper.WriterState writerState =
                deserializeWriterState(bytes, offset, writerStateLength);
        offset += writerStateLength;

        // position
        long position = ByteBuffer.wrap(bytes, offset, Long.BYTES).getLong();

        GSRecoverablePlan plan = new GSRecoverablePlan(tempBlobId, finalBlobId);
        return new GSRecoverable(plan, writerState, position);
    }
}
