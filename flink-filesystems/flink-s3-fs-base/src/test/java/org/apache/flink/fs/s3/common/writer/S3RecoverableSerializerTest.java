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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link S3RecoverableSerializer}. */
class S3RecoverableSerializerTest {

    private final S3RecoverableSerializer serializer = S3RecoverableSerializer.INSTANCE;

    private static final String TEST_OBJECT_NAME = "TEST-OBJECT";

    private static final String TEST_UPLOAD_ID = "TEST-UPLOAD-ID";

    private static final String INCOMPLETE_OBJECT_NAME = "TEST-INCOMPLETE-PART";

    private static final String ETAG_PREFIX = "TEST-ETAG-";

    private static final String CRC32C_PREFIX = "TEST-CRC32C-";

    private static final String SHA256_PREFIX = "TEST-SHA256-";

    /** Version-2 wire tags of the checksums used by the fixtures; pinned independently. */
    private static final byte CRC32C_WIRE_TAG = 2;

    private static final byte SHA256_WIRE_TAG = 5;

    @Test
    void serializerVersionIsTwo() {
        assertThat(serializer.getVersion()).isEqualTo(2);
    }

    @Test
    void serializeEmptyS3Recoverable() throws IOException {
        S3Recoverable originalEmptyRecoverable = createTestS3Recoverable(false);

        byte[] serializedRecoverable = serializer.serialize(originalEmptyRecoverable);
        S3Recoverable copiedEmptyRecoverable =
                serializer.deserialize(serializer.getVersion(), serializedRecoverable);

        assertThatIsEqualTo(originalEmptyRecoverable, copiedEmptyRecoverable);
    }

    @Test
    void serializeS3RecoverableWithoutIncompleteObject() throws IOException {
        S3Recoverable originalNoIncompletePartRecoverable = createTestS3Recoverable(false, 1, 5, 9);

        byte[] serializedRecoverable = serializer.serialize(originalNoIncompletePartRecoverable);
        S3Recoverable copiedNoIncompletePartRecoverable =
                serializer.deserialize(serializer.getVersion(), serializedRecoverable);

        assertThatIsEqualTo(originalNoIncompletePartRecoverable, copiedNoIncompletePartRecoverable);
    }

    @Test
    void serializeS3RecoverableOnlyWithIncompleteObject() throws IOException {
        S3Recoverable originalOnlyIncompletePartRecoverable = createTestS3Recoverable(true);

        byte[] serializedRecoverable = serializer.serialize(originalOnlyIncompletePartRecoverable);
        S3Recoverable copiedOnlyIncompletePartRecoverable =
                serializer.deserialize(serializer.getVersion(), serializedRecoverable);

        assertThatIsEqualTo(
                originalOnlyIncompletePartRecoverable, copiedOnlyIncompletePartRecoverable);
    }

    @Test
    void serializeS3RecoverableWithCompleteAndIncompleteParts() throws IOException {
        S3Recoverable originalFullRecoverable = createTestS3Recoverable(true, 1, 5, 9);

        byte[] serializedRecoverable = serializer.serialize(originalFullRecoverable);
        S3Recoverable copiedFullRecoverable =
                serializer.deserialize(serializer.getVersion(), serializedRecoverable);

        assertThatIsEqualTo(originalFullRecoverable, copiedFullRecoverable);
    }

    @Test
    void serializeS3RecoverableWithPartChecksums() throws IOException {
        S3Recoverable originalChecksummedRecoverable =
                createChecksummedTestS3Recoverable(true, 1, 5, 9);

        byte[] serializedRecoverable = serializer.serialize(originalChecksummedRecoverable);
        S3Recoverable copiedChecksummedRecoverable =
                serializer.deserialize(serializer.getVersion(), serializedRecoverable);

        assertThatIsEqualTo(originalChecksummedRecoverable, copiedChecksummedRecoverable);
    }

    // ------------------------------------------------------------------------
    //  Wire-format fixture tests. The serialized bytes are persisted in
    //  checkpoints and savepoints, so state written by previous releases must
    //  keep deserializing. These tests pin the exact byte layouts independently
    //  of the serializer implementation; if they fail, the layout changed and a
    //  new serializer version is required instead.
    // ------------------------------------------------------------------------

    @Test
    void wireFormatV1StillDeserializes() throws IOException {
        S3Recoverable expected = createTestS3Recoverable(true, 1, 5, 9);
        byte[] v1Bytes =
                buildV1WireBytes(
                        TEST_OBJECT_NAME,
                        TEST_UPLOAD_ID,
                        new int[] {1, 5, 9},
                        12345L,
                        INCOMPLETE_OBJECT_NAME,
                        54321L);

        assertThatIsEqualTo(serializer.deserialize(1, v1Bytes), expected);
    }

    @Test
    void wireFormatV1StillDeserializesWithoutIncompleteObject() throws IOException {
        S3Recoverable expected = createTestS3Recoverable(false, 1, 5, 9);
        byte[] v1Bytes =
                buildV1WireBytes(
                        TEST_OBJECT_NAME, TEST_UPLOAD_ID, new int[] {1, 5, 9}, 12345L, null, -1L);

        assertThatIsEqualTo(serializer.deserialize(1, v1Bytes), expected);
    }

    @Test
    void wireFormatV2IsStableWithoutChecksums() throws IOException {
        S3Recoverable recoverable = createTestS3Recoverable(true, 1, 5, 9);
        byte[] expectedBytes =
                buildV2WireBytes(
                        TEST_OBJECT_NAME,
                        TEST_UPLOAD_ID,
                        new int[] {1, 5, 9},
                        false,
                        12345L,
                        INCOMPLETE_OBJECT_NAME,
                        54321L);

        assertThat(serializer.serialize(recoverable)).isEqualTo(expectedBytes);
        assertThatIsEqualTo(serializer.deserialize(2, expectedBytes), recoverable);
    }

    @Test
    void wireFormatV2IsStableWithChecksums() throws IOException {
        S3Recoverable recoverable = createChecksummedTestS3Recoverable(false, 1, 5, 9);
        byte[] expectedBytes =
                buildV2WireBytes(
                        TEST_OBJECT_NAME,
                        TEST_UPLOAD_ID,
                        new int[] {1, 5, 9},
                        true,
                        12345L,
                        null,
                        -1L);

        assertThat(serializer.serialize(recoverable)).isEqualTo(expectedBytes);
        assertThatIsEqualTo(serializer.deserialize(2, expectedBytes), recoverable);
    }

    @Test
    void unknownVersionIsRejected() throws IOException {
        byte[] serialized = serializer.serialize(createTestS3Recoverable(false, 1));

        assertThatThrownBy(() -> serializer.deserialize(3, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unrecognized version");
    }

    @Test
    void unknownChecksumTagIsRejected() {
        byte[] corruptBytes = buildV2WireBytesWithSingleChecksum((byte) 99, "SOME-VALUE");

        assertThatThrownBy(() -> serializer.deserialize(2, corruptBytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown part checksum tag");
    }

    private static void assertThatIsEqualTo(
            S3Recoverable actualRecoverable, S3Recoverable expectedRecoverable) {
        assertThat(actualRecoverable.getObjectName())
                .isEqualTo(expectedRecoverable.getObjectName());
        assertThat(actualRecoverable.uploadId()).isEqualTo(expectedRecoverable.uploadId());
        assertThat(actualRecoverable.numBytesInParts())
                .isEqualTo(expectedRecoverable.numBytesInParts());
        assertThat(actualRecoverable.incompleteObjectName())
                .isEqualTo(expectedRecoverable.incompleteObjectName());
        assertThat(actualRecoverable.incompleteObjectLength())
                .isEqualTo(expectedRecoverable.incompleteObjectLength());
        // full CompletedPart equality, covering the checksum fields
        assertThat(actualRecoverable.parts().toArray())
                .isEqualTo(expectedRecoverable.parts().toArray());
    }

    // --------------------------------- Test Utils ---------------------------------

    private static S3Recoverable createTestS3Recoverable(
            boolean withIncompletePart, int... partNumbers) {
        return createTestS3Recoverable(false, withIncompletePart, partNumbers);
    }

    private static S3Recoverable createChecksummedTestS3Recoverable(
            boolean withIncompletePart, int... partNumbers) {
        return createTestS3Recoverable(true, withIncompletePart, partNumbers);
    }

    private static S3Recoverable createTestS3Recoverable(
            boolean withChecksums, boolean withIncompletePart, int... partNumbers) {
        List<CompletedPart> parts = new ArrayList<>();
        for (int i : partNumbers) {
            parts.add(createCompletedPart(i, withChecksums));
        }

        if (withIncompletePart) {
            return new S3Recoverable(
                    TEST_OBJECT_NAME,
                    TEST_UPLOAD_ID,
                    parts,
                    12345L,
                    INCOMPLETE_OBJECT_NAME,
                    54321L);
        } else {
            return new S3Recoverable(TEST_OBJECT_NAME, TEST_UPLOAD_ID, parts, 12345L);
        }
    }

    private static CompletedPart createCompletedPart(int partNumber, boolean withChecksums) {
        CompletedPart.Builder builder =
                CompletedPart.builder().partNumber(partNumber).eTag(ETAG_PREFIX + partNumber);
        if (withChecksums) {
            builder.checksumCRC32C(CRC32C_PREFIX + partNumber)
                    .checksumSHA256(SHA256_PREFIX + partNumber);
        }
        return builder.build();
    }

    /**
     * Hand-builds the version-1 wire layout: little-endian; magic number (int); key length (int) +
     * UTF-8 bytes; upload id length (int) + bytes; part count (int); per part: part number (int) +
     * etag length (int) + bytes; bytes in parts (long); incomplete object name length (int, 0 for
     * none) + bytes; incomplete object length (long).
     */
    private static byte[] buildV1WireBytes(
            String objectName,
            String uploadId,
            int[] partNumbers,
            long numBytesInParts,
            @Nullable String incompleteObjectName,
            long incompleteObjectLength) {
        final byte[] keyBytes = objectName.getBytes(StandardCharsets.UTF_8);
        final byte[] uploadIdBytes = uploadId.getBytes(StandardCharsets.UTF_8);
        final byte[][] etags = new byte[partNumbers.length][];
        int partBytes = 0;
        for (int i = 0; i < partNumbers.length; i++) {
            etags[i] = (ETAG_PREFIX + partNumbers[i]).getBytes(StandardCharsets.UTF_8);
            partBytes += 2 * Integer.BYTES + etags[i].length;
        }
        final byte[] incompleteBytes =
                incompleteObjectName == null
                        ? new byte[0]
                        : incompleteObjectName.getBytes(StandardCharsets.UTF_8);

        final ByteBuffer bb =
                ByteBuffer.allocate(
                                4 * Integer.BYTES
                                        + keyBytes.length
                                        + uploadIdBytes.length
                                        + partBytes
                                        + 2 * Long.BYTES
                                        + Integer.BYTES
                                        + incompleteBytes.length)
                        .order(ByteOrder.LITTLE_ENDIAN);

        bb.putInt(0x98761432);
        bb.putInt(keyBytes.length);
        bb.put(keyBytes);
        bb.putInt(uploadIdBytes.length);
        bb.put(uploadIdBytes);
        bb.putInt(partNumbers.length);
        for (int i = 0; i < partNumbers.length; i++) {
            bb.putInt(partNumbers[i]);
            bb.putInt(etags[i].length);
            bb.put(etags[i]);
        }
        bb.putLong(numBytesInParts);
        bb.putInt(incompleteBytes.length);
        bb.put(incompleteBytes);
        bb.putLong(incompleteObjectLength);

        return bb.array();
    }

    /**
     * Hand-builds the version-2 wire layout: like version 1, but each part is followed by its
     * checksum count (byte) and per checksum the wire tag (byte) + value length (int) + UTF-8
     * bytes. Parts built with {@code withChecksums} carry a CRC32C (tag 2) and a SHA256 (tag 5)
     * checksum, in wire-tag order.
     */
    private static byte[] buildV2WireBytes(
            String objectName,
            String uploadId,
            int[] partNumbers,
            boolean withChecksums,
            long numBytesInParts,
            @Nullable String incompleteObjectName,
            long incompleteObjectLength) {
        final byte[] keyBytes = objectName.getBytes(StandardCharsets.UTF_8);
        final byte[] uploadIdBytes = uploadId.getBytes(StandardCharsets.UTF_8);
        final byte[][] etags = new byte[partNumbers.length][];
        final byte[][] crc32cs = new byte[partNumbers.length][];
        final byte[][] sha256s = new byte[partNumbers.length][];
        int partBytes = 0;
        for (int i = 0; i < partNumbers.length; i++) {
            etags[i] = (ETAG_PREFIX + partNumbers[i]).getBytes(StandardCharsets.UTF_8);
            partBytes += 2 * Integer.BYTES + etags[i].length + Byte.BYTES;
            if (withChecksums) {
                crc32cs[i] = (CRC32C_PREFIX + partNumbers[i]).getBytes(StandardCharsets.UTF_8);
                sha256s[i] = (SHA256_PREFIX + partNumbers[i]).getBytes(StandardCharsets.UTF_8);
                partBytes +=
                        2 * (Byte.BYTES + Integer.BYTES) + crc32cs[i].length + sha256s[i].length;
            }
        }
        final byte[] incompleteBytes =
                incompleteObjectName == null
                        ? new byte[0]
                        : incompleteObjectName.getBytes(StandardCharsets.UTF_8);

        final ByteBuffer bb =
                ByteBuffer.allocate(
                                4 * Integer.BYTES
                                        + keyBytes.length
                                        + uploadIdBytes.length
                                        + partBytes
                                        + 2 * Long.BYTES
                                        + Integer.BYTES
                                        + incompleteBytes.length)
                        .order(ByteOrder.LITTLE_ENDIAN);

        bb.putInt(0x98761432);
        bb.putInt(keyBytes.length);
        bb.put(keyBytes);
        bb.putInt(uploadIdBytes.length);
        bb.put(uploadIdBytes);
        bb.putInt(partNumbers.length);
        for (int i = 0; i < partNumbers.length; i++) {
            bb.putInt(partNumbers[i]);
            bb.putInt(etags[i].length);
            bb.put(etags[i]);
            if (withChecksums) {
                bb.put((byte) 2);
                bb.put(CRC32C_WIRE_TAG);
                bb.putInt(crc32cs[i].length);
                bb.put(crc32cs[i]);
                bb.put(SHA256_WIRE_TAG);
                bb.putInt(sha256s[i].length);
                bb.put(sha256s[i]);
            } else {
                bb.put((byte) 0);
            }
        }
        bb.putLong(numBytesInParts);
        bb.putInt(incompleteBytes.length);
        bb.put(incompleteBytes);
        bb.putLong(incompleteObjectLength);

        return bb.array();
    }

    /** Builds version-2 bytes for a single one-part recoverable carrying one checksum entry. */
    private static byte[] buildV2WireBytesWithSingleChecksum(byte wireTag, String checksumValue) {
        final byte[] keyBytes = TEST_OBJECT_NAME.getBytes(StandardCharsets.UTF_8);
        final byte[] uploadIdBytes = TEST_UPLOAD_ID.getBytes(StandardCharsets.UTF_8);
        final byte[] etagBytes = (ETAG_PREFIX + 1).getBytes(StandardCharsets.UTF_8);
        final byte[] checksumBytes = checksumValue.getBytes(StandardCharsets.UTF_8);

        final ByteBuffer bb =
                ByteBuffer.allocate(
                                8 * Integer.BYTES
                                        + keyBytes.length
                                        + uploadIdBytes.length
                                        + etagBytes.length
                                        + 2 * Byte.BYTES
                                        + checksumBytes.length
                                        + 2 * Long.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN);

        bb.putInt(0x98761432);
        bb.putInt(keyBytes.length);
        bb.put(keyBytes);
        bb.putInt(uploadIdBytes.length);
        bb.put(uploadIdBytes);
        bb.putInt(1);
        bb.putInt(1);
        bb.putInt(etagBytes.length);
        bb.put(etagBytes);
        bb.put((byte) 1);
        bb.put(wireTag);
        bb.putInt(checksumBytes.length);
        bb.put(checksumBytes);
        bb.putLong(12345L);
        bb.putInt(0);
        bb.putLong(-1L);

        return bb.array();
    }
}
