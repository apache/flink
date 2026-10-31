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

package org.apache.flink.fs.s3native.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.fs.s3native.writer.NativeS3Recoverable.PartETag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@link NativeS3Recoverable} state used during checkpointing and recovery.
 *
 * <p><b>Schema Evolution Strategy:</b> This serializer uses an explicit versioning scheme via
 * {@link SimpleVersionedSerializer}. The approach is intentionally simple (version-based binary
 * format) rather than using schema evolution frameworks like Avro or Protobuf because:
 *
 * <ul>
 *   <li>The recoverable state is relatively simple and stable
 *   <li>Changes are infrequent and typically additive
 *   <li>The format is internal to the filesystem implementation
 *   <li>Minimizes external dependencies
 * </ul>
 *
 * <p><b>Version Compatibility Rules:</b>
 *
 * <ul>
 *   <li>New fields should be added at the end of the serialized form
 *   <li>Use optional markers (like the boolean flag for incomplete parts) for nullable fields
 *   <li>When bumping the version, add handling for previous versions in {@link #deserialize}
 *   <li>Never remove or reorder existing fields in the current version
 * </ul>
 *
 * <p><b>Current Format (Version 1):</b>
 *
 * <pre>
 * - objectName: UTF string
 * - uploadId: UTF string
 * - numBytesInParts: long
 * - partsCount: int
 * - parts[]: { partNumber: int, eTag: UTF string } repeated partsCount times
 * - hasIncompletePart: boolean
 * - if hasIncompletePart:
 *   - incompleteObjectName: UTF string
 *   - incompleteObjectLength: long
 * </pre>
 */
class NativeS3RecoverableSerializer implements SimpleVersionedSerializer<NativeS3Recoverable> {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3RecoverableSerializer.class);

    public static final NativeS3RecoverableSerializer INSTANCE =
            new NativeS3RecoverableSerializer();

    private static final int CURRENT_VERSION = 1;

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    /**
     * Magic number written at the beginning of the legacy ({@code flink-s3-fs-hadoop}) serialized
     * form, as found in {@code S3RecoverableSerializer} of flink-s3-fs-base.
     */
    @VisibleForTesting static final int LEGACY_MAGIC_NUMBER = 0x98761432;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(NativeS3Recoverable recoverable) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(outputStream);
        out.writeUTF(recoverable.getObjectName());
        out.writeUTF(recoverable.uploadId());
        out.writeLong(recoverable.numBytesInParts());
        List<PartETag> parts = recoverable.parts();
        out.writeInt(parts.size());
        for (PartETag part : parts) {
            out.writeInt(part.getPartNumber());
            out.writeUTF(part.getETag());
        }
        String incompleteObject = recoverable.incompleteObjectName();
        if (incompleteObject == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(incompleteObject);
            out.writeLong(recoverable.incompleteObjectLength());
        }
        out.flush();
        return outputStream.toByteArray();
    }

    @Override
    public NativeS3Recoverable deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        if (isLegacyFormat(serialized)) {
            LOG.debug(
                    "Detected S3 multipart upload state written by flink-s3-fs-hadoop "
                            + "(legacy format); restoring it with the native S3 filesystem.");
            return deserializeLegacy(serialized);
        }
        ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized);
        DataInputStream in = new DataInputStream(inputStream);
        String objectName = in.readUTF();
        String uploadId = in.readUTF();
        long numBytesInParts = in.readLong();
        int numParts = in.readInt();
        List<PartETag> parts = new ArrayList<>(numParts);
        for (int i = 0; i < numParts; i++) {
            int partNumber = in.readInt();
            String eTag = in.readUTF();
            parts.add(new PartETag(partNumber, eTag));
        }
        boolean hasIncompletePart = in.readBoolean();
        String incompleteObjectName = null;
        long incompleteObjectLength = -1;
        if (hasIncompletePart) {
            incompleteObjectName = in.readUTF();
            incompleteObjectLength = in.readLong();
        }
        return new NativeS3Recoverable(
                objectName,
                uploadId,
                parts,
                numBytesInParts,
                incompleteObjectName,
                incompleteObjectLength);
    }

    /**
     * Returns whether the state was written by flink-s3-fs-base's {@code S3RecoverableSerializer}.
     */
    private static boolean isLegacyFormat(byte[] serialized) {
        if (serialized.length < Integer.BYTES) {
            return false;
        }
        return ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN).getInt()
                == LEGACY_MAGIC_NUMBER;
    }

    /**
     * Decodes the legacy little-endian format written by {@code S3RecoverableSerializer} in
     * flink-s3-fs-base, wrapping any decode error of corrupt state in an {@link IOException}.
     */
    private static NativeS3Recoverable deserializeLegacy(byte[] serialized) throws IOException {
        try {
            return decodeLegacy(serialized);
        } catch (BufferUnderflowException
                | NegativeArraySizeException
                | IllegalArgumentException e) {
            throw new IOException(
                    "Corrupt legacy S3 recoverable state: truncated or invalid data "
                            + "(written by flink-s3-fs-hadoop).",
                    e);
        }
    }

    private static NativeS3Recoverable decodeLegacy(byte[] serialized) throws IOException {
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        // re-check kept so this decoder stays a faithful copy of the authoritative
        // S3RecoverableSerializer.deserializeV1 in flink-s3-fs-base
        if (bb.getInt() != LEGACY_MAGIC_NUMBER) {
            throw new IOException("Corrupt data: Unexpected magic number.");
        }

        final byte[] keyBytes = new byte[readBoundedLength(bb)];
        bb.get(keyBytes);

        final byte[] uploadIdBytes = new byte[readBoundedLength(bb)];
        bb.get(uploadIdBytes);

        final int numParts = readBoundedPartCount(bb);
        final List<PartETag> parts = new ArrayList<>(numParts);
        for (int i = 0; i < numParts; i++) {
            final int partNum = bb.getInt();
            final byte[] buffer = new byte[readBoundedLength(bb)];
            bb.get(buffer);
            parts.add(new PartETag(partNum, new String(buffer, CHARSET)));
        }

        final long numBytes = bb.getLong();

        final String lastPart;
        final int lastObjectArraySize = readBoundedLength(bb);
        if (lastObjectArraySize == 0) {
            lastPart = null;
        } else {
            final byte[] lastPartBytes = new byte[lastObjectArraySize];
            bb.get(lastPartBytes);
            lastPart = new String(lastPartBytes, CHARSET);
        }

        final long lastPartLength = bb.getLong();

        if (bb.hasRemaining()) {
            throw new IOException(
                    "Corrupt legacy S3 recoverable state: unexpected trailing bytes.");
        }

        return new NativeS3Recoverable(
                new String(keyBytes, CHARSET),
                new String(uploadIdBytes, CHARSET),
                parts,
                numBytes,
                lastPart,
                lastPartLength);
    }

    /** Reads a field length, rejecting negative values and values beyond the remaining buffer. */
    private static int readBoundedLength(ByteBuffer bb) throws IOException {
        final int length = bb.getInt();
        if (length < 0 || length > bb.remaining()) {
            throw new IOException(
                    "Corrupt legacy S3 recoverable state: invalid field length " + length);
        }
        return length;
    }

    /** Reads the part count, bounded so that a corrupt value cannot trigger a huge allocation. */
    private static int readBoundedPartCount(ByteBuffer bb) throws IOException {
        final int numParts = bb.getInt();
        // every part needs at least part number + eTag length (2 ints); remaining also holds the
        // trailing fields, so this bound is conservative but proportional to the buffer size
        if (numParts < 0 || (long) numParts * 2 * Integer.BYTES > bb.remaining()) {
            throw new IOException(
                    "Corrupt legacy S3 recoverable state: invalid part count " + numParts);
        }
        return numParts;
    }
}
