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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NativeS3RecoverableSerializer}. */
class NativeS3RecoverableSerializerTest {

    @Test
    void testSerializeAndDeserializeWithParts() throws IOException {
        NativeS3RecoverableSerializer serializer = NativeS3RecoverableSerializer.INSTANCE;

        List<NativeS3Recoverable.PartETag> parts = new ArrayList<>();
        parts.add(new NativeS3Recoverable.PartETag(1, "etag1"));
        parts.add(new NativeS3Recoverable.PartETag(2, "etag2"));
        parts.add(new NativeS3Recoverable.PartETag(3, "etag3"));

        NativeS3Recoverable original =
                new NativeS3Recoverable("test-object-key", "test-upload-id", parts, 12345L);

        byte[] serialized = serializer.serialize(original);
        assertThat(serialized).isNotNull();
        assertThat(serialized.length).isGreaterThan(0);

        NativeS3Recoverable deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserialized.getObjectName()).isEqualTo(original.getObjectName());
        assertThat(deserialized.uploadId()).isEqualTo(original.uploadId());
        assertThat(deserialized.numBytesInParts()).isEqualTo(original.numBytesInParts());
        assertThat(deserialized.parts()).hasSize(original.parts().size());
        assertThat(deserialized.incompleteObjectName()).isNull();

        for (int i = 0; i < parts.size(); i++) {
            assertThat(deserialized.parts().get(i).getPartNumber())
                    .isEqualTo(original.parts().get(i).getPartNumber());
            assertThat(deserialized.parts().get(i).getETag())
                    .isEqualTo(original.parts().get(i).getETag());
        }
    }

    @Test
    void testSerializeAndDeserializeWithIncompleteObject() throws IOException {
        NativeS3RecoverableSerializer serializer = NativeS3RecoverableSerializer.INSTANCE;

        List<NativeS3Recoverable.PartETag> parts = new ArrayList<>();
        parts.add(new NativeS3Recoverable.PartETag(1, "etag1"));

        NativeS3Recoverable original =
                new NativeS3Recoverable(
                        "test-object-key",
                        "test-upload-id",
                        parts,
                        5242880L,
                        "incomplete-object-key",
                        1024L);

        byte[] serialized = serializer.serialize(original);
        NativeS3Recoverable deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserialized.getObjectName()).isEqualTo(original.getObjectName());
        assertThat(deserialized.uploadId()).isEqualTo(original.uploadId());
        assertThat(deserialized.numBytesInParts()).isEqualTo(original.numBytesInParts());
        assertThat(deserialized.incompleteObjectName()).isEqualTo(original.incompleteObjectName());
    }

    @Test
    void testSerializeAndDeserializeEmptyParts() throws IOException {
        NativeS3RecoverableSerializer serializer = NativeS3RecoverableSerializer.INSTANCE;

        NativeS3Recoverable original =
                new NativeS3Recoverable("test-object-key", "test-upload-id", new ArrayList<>(), 0L);

        byte[] serialized = serializer.serialize(original);
        NativeS3Recoverable deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserialized.getObjectName()).isEqualTo(original.getObjectName());
        assertThat(deserialized.uploadId()).isEqualTo(original.uploadId());
        assertThat(deserialized.numBytesInParts()).isEqualTo(0L);
        assertThat(deserialized.parts()).isEmpty();
    }

    @Test
    void testVersionIsConsistent() {
        NativeS3RecoverableSerializer serializer = NativeS3RecoverableSerializer.INSTANCE;
        assertThat(serializer.getVersion()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void testUnsupportedVersionThrows() throws IOException {
        NativeS3RecoverableSerializer serializer = NativeS3RecoverableSerializer.INSTANCE;
        NativeS3Recoverable original =
                new NativeS3Recoverable("key", "upload-id", new ArrayList<>(), 0L);
        byte[] serialized = serializer.serialize(original);

        assertThatThrownBy(() -> serializer.deserialize(2, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported version");
    }

    @Test
    void testDeserializeLegacyFormatWithIncompletePart() throws IOException {
        NativeS3Recoverable deserialized =
                NativeS3RecoverableSerializer.INSTANCE.deserialize(
                        NativeS3RecoverableSerializer.INSTANCE.getVersion(),
                        serializeLegacy(
                                "test-object-key",
                                "test-upload-id",
                                new int[] {1, 2},
                                new String[] {"etag1", "etag2"},
                                12345L,
                                "incomplete-object-key",
                                1024L));

        assertThat(deserialized.getObjectName()).isEqualTo("test-object-key");
        assertThat(deserialized.uploadId()).isEqualTo("test-upload-id");
        assertThat(deserialized.numBytesInParts()).isEqualTo(12345L);
        assertThat(deserialized.parts()).hasSize(2);
        assertThat(deserialized.parts().get(0).getPartNumber()).isEqualTo(1);
        assertThat(deserialized.parts().get(0).getETag()).isEqualTo("etag1");
        assertThat(deserialized.parts().get(1).getPartNumber()).isEqualTo(2);
        assertThat(deserialized.parts().get(1).getETag()).isEqualTo("etag2");
        assertThat(deserialized.incompleteObjectName()).isEqualTo("incomplete-object-key");
        assertThat(deserialized.incompleteObjectLength()).isEqualTo(1024L);
    }

    @Test
    void testDeserializeLegacyFormatWithoutIncompletePart() throws IOException {
        NativeS3Recoverable deserialized =
                NativeS3RecoverableSerializer.INSTANCE.deserialize(
                        NativeS3RecoverableSerializer.INSTANCE.getVersion(),
                        serializeLegacy(
                                "test-object-key",
                                "test-upload-id",
                                new int[] {1},
                                new String[] {"etag1"},
                                54321L,
                                null,
                                -1L));

        assertThat(deserialized.getObjectName()).isEqualTo("test-object-key");
        assertThat(deserialized.uploadId()).isEqualTo("test-upload-id");
        assertThat(deserialized.numBytesInParts()).isEqualTo(54321L);
        assertThat(deserialized.parts()).hasSize(1);
        assertThat(deserialized.incompleteObjectName()).isNull();
        assertThat(deserialized.incompleteObjectLength()).isEqualTo(-1L);
    }

    @Test
    void testLegacyFormatWithoutParts() throws IOException {
        NativeS3Recoverable deserialized =
                NativeS3RecoverableSerializer.INSTANCE.deserialize(
                        NativeS3RecoverableSerializer.INSTANCE.getVersion(),
                        serializeLegacy(
                                "key", "upload-id", new int[0], new String[0], 0L, null, -1L));

        assertThat(deserialized.getObjectName()).isEqualTo("key");
        assertThat(deserialized.parts()).isEmpty();
    }

    @Test
    void testCurrentFormatNotMistakenForLegacyFormat() throws IOException {
        NativeS3RecoverableSerializer serializer = NativeS3RecoverableSerializer.INSTANCE;

        List<NativeS3Recoverable.PartETag> parts = new ArrayList<>();
        parts.add(new NativeS3Recoverable.PartETag(1, "etag1"));
        NativeS3Recoverable original =
                new NativeS3Recoverable("test-object-key", "test-upload-id", parts, 12345L);

        byte[] serialized = serializer.serialize(original);
        NativeS3Recoverable deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserialized.getObjectName()).isEqualTo(original.getObjectName());
        assertThat(deserialized.uploadId()).isEqualTo(original.uploadId());
        assertThat(deserialized.numBytesInParts()).isEqualTo(original.numBytesInParts());
        assertThat(deserialized.parts()).hasSize(1);
    }

    /**
     * Golden test against bytes captured once from the real {@code
     * S3RecoverableSerializer#serialize(S3Recoverable)} in flink-s3-fs-base (object name {@code
     * dir/test-key-ä日🔑}, upload id {@code upload-ä-id}, 2 parts, incomplete part with a multi-byte
     * name), pinning the legacy wire format independently of the test-side re-encoder below.
     */
    @Test
    void testDeserializeGoldenLegacyBytes() throws IOException {
        NativeS3Recoverable deserialized =
                NativeS3RecoverableSerializer.INSTANCE.deserialize(
                        NativeS3RecoverableSerializer.INSTANCE.getVersion(),
                        hexToBytes(
                                "3214769816000000"
                                        + "6469722f74657374"
                                        + "2d6b65792dc3a4e6"
                                        + "97a5f09f94910c00"
                                        + "000075706c6f6164"
                                        + "2dc3a42d69640200"
                                        + "0000010000000800"
                                        + "0000657461672d6f"
                                        + "6e65020000000800"
                                        + "0000657461672d74"
                                        + "776f393000000000"
                                        + "000011000000696e"
                                        + "636f6d706c657465"
                                        + "2d6b65792dc3a400"
                                        + "04000000000000"));

        assertThat(deserialized.getObjectName()).isEqualTo("dir/test-key-\u00e4\u65e5\uD83D\uDD11");
        assertThat(deserialized.uploadId()).isEqualTo("upload-\u00e4-id");
        assertThat(deserialized.numBytesInParts()).isEqualTo(12345L);
        assertThat(deserialized.parts()).hasSize(2);
        assertThat(deserialized.parts().get(0).getPartNumber()).isEqualTo(1);
        assertThat(deserialized.parts().get(0).getETag()).isEqualTo("etag-one");
        assertThat(deserialized.parts().get(1).getPartNumber()).isEqualTo(2);
        assertThat(deserialized.parts().get(1).getETag()).isEqualTo("etag-two");
        assertThat(deserialized.incompleteObjectName()).isEqualTo("incomplete-key-\u00e4");
        assertThat(deserialized.incompleteObjectLength()).isEqualTo(1024L);
    }

    @Test
    void testDeserializeLegacyWithMultiByteNames() throws IOException {
        NativeS3Recoverable deserialized =
                NativeS3RecoverableSerializer.INSTANCE.deserialize(
                        NativeS3RecoverableSerializer.INSTANCE.getVersion(),
                        serializeLegacy(
                                "dir/ключ-\u00e4日\uD83D\uDD11",
                                "upload-id-\u00e9",
                                new int[] {7},
                                new String[] {"etag-ä"},
                                99L,
                                "trailing-\u00fc",
                                7L));

        assertThat(deserialized.getObjectName()).isEqualTo("dir/ключ-\u00e4日\uD83D\uDD11");
        assertThat(deserialized.uploadId()).isEqualTo("upload-id-\u00e9");
        assertThat(deserialized.parts().get(0).getETag()).isEqualTo("etag-ä");
        assertThat(deserialized.incompleteObjectName()).isEqualTo("trailing-\u00fc");
    }

    @Test
    void testDeserializeLegacyTruncatedThrowsIOException() {
        byte[] legacy =
                hexToBytes(
                        "3214769816000000"
                                + "6469722f74657374"
                                + "2d6b65792dc3a4e6"
                                + "97a5f09f94910c00"
                                + "000075706c6f6164"
                                + "2dc3a42d69640200");
        byte[] truncated = new byte[legacy.length - 5];
        System.arraycopy(legacy, 0, truncated, 0, truncated.length);

        assertThatThrownBy(
                        () ->
                                NativeS3RecoverableSerializer.INSTANCE.deserialize(
                                        NativeS3RecoverableSerializer.INSTANCE.getVersion(),
                                        truncated))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Corrupt legacy");
    }

    @Test
    void testDeserializeLegacyWithTrailingBytesThrowsIOException() {
        byte[] legacy = serializeLegacyQuietly("key", "upload-id", 0L);
        byte[] withTrailing = new byte[legacy.length + 1];
        System.arraycopy(legacy, 0, withTrailing, 0, legacy.length);

        assertThatThrownBy(
                        () ->
                                NativeS3RecoverableSerializer.INSTANCE.deserialize(
                                        NativeS3RecoverableSerializer.INSTANCE.getVersion(),
                                        withTrailing))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("trailing bytes");
    }

    @Test
    void testDeserializeLegacyWithNegativeLengthThrowsIOException() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertThatThrownBy(
                        () -> {
                            putIntLE(out, NativeS3RecoverableSerializer.LEGACY_MAGIC_NUMBER);
                            putIntLE(out, -1);
                            NativeS3RecoverableSerializer.INSTANCE.deserialize(
                                    NativeS3RecoverableSerializer.INSTANCE.getVersion(),
                                    out.toByteArray());
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid field length");
    }

    @Test
    void testDeserializeLegacyWithHugeLengthThrowsIOException() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertThatThrownBy(
                        () -> {
                            putIntLE(out, NativeS3RecoverableSerializer.LEGACY_MAGIC_NUMBER);
                            putIntLE(out, Integer.MAX_VALUE);
                            NativeS3RecoverableSerializer.INSTANCE.deserialize(
                                    NativeS3RecoverableSerializer.INSTANCE.getVersion(),
                                    out.toByteArray());
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid field length");
    }

    @Test
    void testDeserializeLegacyWithHugePartCountThrowsIOException() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertThatThrownBy(
                        () -> {
                            putIntLE(out, NativeS3RecoverableSerializer.LEGACY_MAGIC_NUMBER);
                            putIntLE(out, 0); // object name length
                            putIntLE(out, 0); // upload id length
                            putIntLE(out, Integer.MAX_VALUE); // part count
                            NativeS3RecoverableSerializer.INSTANCE.deserialize(
                                    NativeS3RecoverableSerializer.INSTANCE.getVersion(),
                                    out.toByteArray());
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid part count");
    }

    private static byte[] serializeLegacyQuietly(
            String objectName, String uploadId, long numBytesInParts) {
        try {
            return serializeLegacy(
                    objectName, uploadId, new int[0], new String[0], numBytesInParts, null, -1L);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static byte[] hexToBytes(String hex) {
        String normalized = hex.replace(" ", "");
        byte[] bytes = new byte[normalized.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            int hi = Character.digit(normalized.charAt(2 * i), 16);
            int lo = Character.digit(normalized.charAt(2 * i + 1), 16);
            bytes[i] = (byte) ((hi << 4) + lo);
        }
        return bytes;
    }

    /**
     * Reproduces the binary format written by {@code S3RecoverableSerializer} (version 1) of
     * flink-s3-fs-base, used by flink-s3-fs-hadoop / flink-s3-fs-presto: little-endian, leading
     * magic number, then length-prefixed UTF-8 strings.
     */
    private static byte[] serializeLegacy(
            String objectName,
            String uploadId,
            int[] partNumbers,
            String[] eTags,
            long numBytesInParts,
            String incompleteObjectName,
            long incompleteObjectLength)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        putIntLE(out, NativeS3RecoverableSerializer.LEGACY_MAGIC_NUMBER);
        putLegacyString(out, objectName);
        putLegacyString(out, uploadId);

        putIntLE(out, partNumbers.length);
        for (int i = 0; i < partNumbers.length; i++) {
            putIntLE(out, partNumbers[i]);
            putLegacyString(out, eTags[i]);
        }

        putLongLE(out, numBytesInParts);

        if (incompleteObjectName == null) {
            putIntLE(out, 0);
        } else {
            putLegacyString(out, incompleteObjectName);
        }
        putLongLE(out, incompleteObjectLength);

        return out.toByteArray();
    }

    private static void putLegacyString(ByteArrayOutputStream out, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        putIntLE(out, bytes.length);
        out.write(bytes);
    }

    private static void putIntLE(ByteArrayOutputStream out, int value) throws IOException {
        out.write(
                ByteBuffer.allocate(Integer.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(value)
                        .array());
    }

    private static void putLongLE(ByteArrayOutputStream out, long value) throws IOException {
        out.write(
                ByteBuffer.allocate(Long.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putLong(value)
                        .array());
    }
}
