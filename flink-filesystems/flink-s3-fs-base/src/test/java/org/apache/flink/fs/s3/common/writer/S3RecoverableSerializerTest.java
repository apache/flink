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

/** Tests for the {@link S3RecoverableSerializer}. */
class S3RecoverableSerializerTest {

    private final S3RecoverableSerializer serializer = S3RecoverableSerializer.INSTANCE;

    private static final String TEST_OBJECT_NAME = "TEST-OBJECT";

    private static final String TEST_UPLOAD_ID = "TEST-UPLOAD-ID";

    private static final String INCOMPLETE_OBJECT_NAME = "TEST-INCOMPLETE-PART";

    private static final String ETAG_PREFIX = "TEST-ETAG-";

    @Test
    void serializeEmptyS3Recoverable() throws IOException {
        S3Recoverable originalEmptyRecoverable = createTestS3Recoverable(false);

        byte[] serializedRecoverable = serializer.serialize(originalEmptyRecoverable);
        S3Recoverable copiedEmptyRecoverable = serializer.deserialize(1, serializedRecoverable);

        assertThatIsEqualTo(originalEmptyRecoverable, copiedEmptyRecoverable);
    }

    @Test
    void serializeS3RecoverableWithoutIncompleteObject() throws IOException {
        S3Recoverable originalNoIncompletePartRecoverable = createTestS3Recoverable(false, 1, 5, 9);

        byte[] serializedRecoverable = serializer.serialize(originalNoIncompletePartRecoverable);
        S3Recoverable copiedNoIncompletePartRecoverable =
                serializer.deserialize(1, serializedRecoverable);

        assertThatIsEqualTo(originalNoIncompletePartRecoverable, copiedNoIncompletePartRecoverable);
    }

    @Test
    void serializeS3RecoverableOnlyWithIncompleteObject() throws IOException {
        S3Recoverable originalOnlyIncompletePartRecoverable = createTestS3Recoverable(true);

        byte[] serializedRecoverable = serializer.serialize(originalOnlyIncompletePartRecoverable);
        S3Recoverable copiedOnlyIncompletePartRecoverable =
                serializer.deserialize(1, serializedRecoverable);

        assertThatIsEqualTo(
                originalOnlyIncompletePartRecoverable, copiedOnlyIncompletePartRecoverable);
    }

    @Test
    void serializeS3RecoverableWithCompleteAndIncompleteParts() throws IOException {
        S3Recoverable originalFullRecoverable = createTestS3Recoverable(true, 1, 5, 9);

        byte[] serializedRecoverable = serializer.serialize(originalFullRecoverable);
        S3Recoverable copiedFullRecoverable = serializer.deserialize(1, serializedRecoverable);

        assertThatIsEqualTo(originalFullRecoverable, copiedFullRecoverable);
    }

    // ------------------------------------------------------------------------
    //  Wire-format fixture tests. Version-1 bytes are persisted in checkpoints
    //  and savepoints, so state written by previous releases must keep
    //  deserializing. These tests pin the exact byte layout independently of
    //  the serializer implementation; if they fail, the layout changed and a
    //  new serializer version is required instead.
    // ------------------------------------------------------------------------

    @Test
    void wireFormatIsStableWithCompleteAndIncompleteParts() throws IOException {
        S3Recoverable recoverable = createTestS3Recoverable(true, 1, 5, 9);
        byte[] expectedBytes =
                buildV1WireBytes(
                        TEST_OBJECT_NAME,
                        TEST_UPLOAD_ID,
                        new int[] {1, 5, 9},
                        12345L,
                        INCOMPLETE_OBJECT_NAME,
                        54321L);

        assertThat(serializer.serialize(recoverable)).isEqualTo(expectedBytes);
        assertThatIsEqualTo(serializer.deserialize(1, expectedBytes), recoverable);
    }

    @Test
    void wireFormatIsStableWithoutIncompleteObject() throws IOException {
        S3Recoverable recoverable = createTestS3Recoverable(false, 1, 5, 9);
        byte[] expectedBytes =
                buildV1WireBytes(
                        TEST_OBJECT_NAME, TEST_UPLOAD_ID, new int[] {1, 5, 9}, 12345L, null, -1L);

        assertThat(serializer.serialize(recoverable)).isEqualTo(expectedBytes);
        assertThatIsEqualTo(serializer.deserialize(1, expectedBytes), recoverable);
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
        assertThat(actualRecoverable.parts().stream().map(CompletedPart::eTag).toArray())
                .isEqualTo(expectedRecoverable.parts().stream().map(CompletedPart::eTag).toArray());
    }

    // --------------------------------- Test Utils ---------------------------------

    private static S3Recoverable createTestS3Recoverable(
            boolean withIncompletePart, int... partNumbers) {
        List<CompletedPart> parts = new ArrayList<>();
        for (int i : partNumbers) {
            parts.add(createCompletedPart(i));
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

    private static CompletedPart createCompletedPart(int partNumber) {
        return CompletedPart.builder()
                .partNumber(partNumber)
                .eTag(ETAG_PREFIX + partNumber)
                .build();
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
}
