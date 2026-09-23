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

package org.apache.flink.fs.s3.common.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer implementation for a {@link S3Recoverable}.
 *
 * <p>Version 2 extends version 1 by persisting the per-part checksums (see {@link S3PartChecksum})
 * so that a CompleteMultipartUpload after recovery can repeat the checksum each part was uploaded
 * with. Version-1 state deserializes into checksum-less parts, which is correct: it was written by
 * releases that never generated part checksums.
 */
@Internal
final class S3RecoverableSerializer implements SimpleVersionedSerializer<S3Recoverable> {

    static final S3RecoverableSerializer INSTANCE = new S3RecoverableSerializer();

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final int MAGIC_NUMBER = 0x98761432;

    /** Do not instantiate, use reusable {@link #INSTANCE} instead. */
    private S3RecoverableSerializer() {}

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(S3Recoverable obj) throws IOException {
        final List<CompletedPart> partList = obj.parts();
        final CompletedPart[] parts = partList.toArray(new CompletedPart[0]);

        final byte[] keyBytes = obj.getObjectName().getBytes(CHARSET);
        final byte[] uploadIdBytes = obj.uploadId().getBytes(CHARSET);

        final byte[][] etags = new byte[parts.length][];
        final byte[][] checksumBlocks = new byte[parts.length][];
        int partEtagBytes = 0;
        for (int i = 0; i < parts.length; i++) {
            etags[i] = parts[i].eTag().getBytes(CHARSET);
            checksumBlocks[i] = encodeChecksums(parts[i]);
            partEtagBytes += etags[i].length + 2 * Integer.BYTES + checksumBlocks[i].length;
        }

        final String lastObjectKey = obj.incompleteObjectName();
        final byte[] lastPartBytes = lastObjectKey == null ? null : lastObjectKey.getBytes(CHARSET);

        final byte[] targetBytes =
                new byte
                        [Integer.BYTES
                                + // magic number
                                Integer.BYTES
                                + keyBytes.length
                                + Integer.BYTES
                                + uploadIdBytes.length
                                + Integer.BYTES
                                + partEtagBytes
                                + Long.BYTES
                                + Integer.BYTES
                                + (lastPartBytes == null ? 0 : lastPartBytes.length)
                                + Long.BYTES];

        ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(MAGIC_NUMBER);

        bb.putInt(keyBytes.length);
        bb.put(keyBytes);

        bb.putInt(uploadIdBytes.length);
        bb.put(uploadIdBytes);

        bb.putInt(etags.length);
        for (int i = 0; i < parts.length; i++) {
            CompletedPart pe = parts[i];
            bb.putInt(pe.partNumber());
            bb.putInt(etags[i].length);
            bb.put(etags[i]);
            bb.put(checksumBlocks[i]);
        }

        bb.putLong(obj.numBytesInParts());

        if (lastPartBytes == null) {
            bb.putInt(0);
        } else {
            bb.putInt(lastPartBytes.length);
            bb.put(lastPartBytes);
        }

        bb.putLong(obj.incompleteObjectLength());

        return targetBytes;
    }

    /**
     * Encodes the checksums of a part as: checksum count (byte, 0 for none), then per checksum its
     * wire tag (byte) and the length-prefixed UTF-8 value exactly as the SDK returned it.
     */
    private static byte[] encodeChecksums(CompletedPart part) {
        final List<S3PartChecksum> presentChecksums = new ArrayList<>();
        final List<byte[]> values = new ArrayList<>();
        int size = Byte.BYTES;
        for (S3PartChecksum checksum : S3PartChecksum.values()) {
            final String value = checksum.valueOf(part);
            if (value != null) {
                final byte[] valueBytes = value.getBytes(CHARSET);
                presentChecksums.add(checksum);
                values.add(valueBytes);
                size += Byte.BYTES + Integer.BYTES + valueBytes.length;
            }
        }

        final ByteBuffer bb = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) presentChecksums.size());
        for (int i = 0; i < presentChecksums.size(); i++) {
            bb.put(presentChecksums.get(i).getWireTag());
            bb.putInt(values.get(i).length);
            bb.put(values.get(i));
        }
        return bb.array();
    }

    @Override
    public S3Recoverable deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
                return deserializeV1(serialized);
            case 2:
                return deserializeV2(serialized);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private static S3Recoverable deserializeV1(byte[] serialized) throws IOException {
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        if (bb.getInt() != MAGIC_NUMBER) {
            throw new IOException("Corrupt data: Unexpected magic number.");
        }

        final byte[] keyBytes = new byte[bb.getInt()];
        bb.get(keyBytes);

        final byte[] uploadIdBytes = new byte[bb.getInt()];
        bb.get(uploadIdBytes);

        final int numParts = bb.getInt();
        final ArrayList<CompletedPart> parts = new ArrayList<>(numParts);
        for (int i = 0; i < numParts; i++) {
            final int partNum = bb.getInt();
            final byte[] buffer = new byte[bb.getInt()];
            bb.get(buffer);
            parts.add(
                    CompletedPart.builder()
                            .partNumber(partNum)
                            .eTag(new String(buffer, CHARSET))
                            .build());
        }

        return deserializeTrailer(bb, keyBytes, uploadIdBytes, parts);
    }

    private static S3Recoverable deserializeV2(byte[] serialized) throws IOException {
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        if (bb.getInt() != MAGIC_NUMBER) {
            throw new IOException("Corrupt data: Unexpected magic number.");
        }

        final byte[] keyBytes = new byte[bb.getInt()];
        bb.get(keyBytes);

        final byte[] uploadIdBytes = new byte[bb.getInt()];
        bb.get(uploadIdBytes);

        final int numParts = bb.getInt();
        final ArrayList<CompletedPart> parts = new ArrayList<>(numParts);
        for (int i = 0; i < numParts; i++) {
            final int partNum = bb.getInt();
            final byte[] buffer = new byte[bb.getInt()];
            bb.get(buffer);
            final CompletedPart.Builder partBuilder =
                    CompletedPart.builder().partNumber(partNum).eTag(new String(buffer, CHARSET));
            final int numChecksums = bb.get();
            for (int c = 0; c < numChecksums; c++) {
                final S3PartChecksum checksum = S3PartChecksum.fromWireTag(bb.get());
                final byte[] valueBuffer = new byte[bb.getInt()];
                bb.get(valueBuffer);
                checksum.applyTo(partBuilder, new String(valueBuffer, CHARSET));
            }
            parts.add(partBuilder.build());
        }

        return deserializeTrailer(bb, keyBytes, uploadIdBytes, parts);
    }

    /** Reads the fields following the part list; identical in versions 1 and 2. */
    private static S3Recoverable deserializeTrailer(
            ByteBuffer bb, byte[] keyBytes, byte[] uploadIdBytes, List<CompletedPart> parts) {
        final long numBytes = bb.getLong();

        final String lastPart;
        final int lastObjectArraySize = bb.getInt();
        if (lastObjectArraySize == 0) {
            lastPart = null;
        } else {
            byte[] lastPartBytes = new byte[lastObjectArraySize];
            bb.get(lastPartBytes);
            lastPart = new String(lastPartBytes, CHARSET);
        }

        final long lastPartLength = bb.getLong();

        return new S3Recoverable(
                new String(keyBytes, CHARSET),
                new String(uploadIdBytes, CHARSET),
                parts,
                numBytes,
                lastPart,
                lastPartLength);
    }
}
