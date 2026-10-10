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

package org.apache.flink.fs.cse.aes.gcm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AesGcmEncryptionMetadata}. */
class AesGcmEncryptionMetadataTest {

    private static final String KEY_ID = "my-key-id";
    private static final String KEY_VERSION = "v1";
    private static final int IV_BYTE_LENGTH = 12;
    private static final Map<String, String> EMPTY_CONTEXT = Collections.emptyMap();
    private static final AesGcmEncryptionMetadata DEFAULT_META =
            new AesGcmEncryptionMetadata(KEY_ID, KEY_VERSION, IV_BYTE_LENGTH, EMPTY_CONTEXT);

    // -------------------------------------------------------------------------
    // Round-trip
    // -------------------------------------------------------------------------

    @Test
    void toMetadataShouldContainAllEntries() {
        final Map<String, String> context = Map.of("org_id", "org-123");
        final AesGcmEncryptionMetadata meta =
                new AesGcmEncryptionMetadata(KEY_ID, KEY_VERSION, IV_BYTE_LENGTH, context);

        assertThat(meta.toMetadata())
                .containsEntry("x_cse_key_id", KEY_ID)
                .containsEntry("x_cse_key_version", KEY_VERSION)
                .containsEntry("x_cse_iv_bytes_length", Integer.toString(IV_BYTE_LENGTH))
                .containsEntry("x_cse_algorithm", "AES/GCM/NoPadding")
                .containsEntry(
                        "x_cse_gcm_tag_length",
                        Integer.toString(AesGcmEncryptionMetadata.GCM_TAG_LENGTH))
                .containsEntry("x_cse_chunk_size", "-1")
                .containsEntry("x_cse_ctx_org_id", "org-123");
    }

    @Test
    void shouldRoundTripThroughMetadataMap() {
        final Map<String, String> context = Map.of("department", "eng");
        final AesGcmEncryptionMetadata original =
                new AesGcmEncryptionMetadata(KEY_ID, KEY_VERSION, IV_BYTE_LENGTH, context);

        final AesGcmEncryptionMetadata parsed =
                AesGcmEncryptionMetadata.fromMetadata(original.toMetadata());

        assertThat(parsed.keyId()).isEqualTo(KEY_ID);
        assertThat(parsed.getKeyVersion()).isEqualTo(KEY_VERSION);
        assertThat(parsed.getIvByteLength()).isEqualTo(IV_BYTE_LENGTH);
        assertThat(parsed.getEncryptionContext()).isEqualTo(context);
        assertThat(parsed).isEqualTo(original);
    }

    @ParameterizedTest
    @MethodSource("illegalContextKeys")
    void shouldRejectContextKeyThatIsNotAValidMetadataKey(final String illegalKey) {
        // Object metadata keys must be C# identifiers (Azure's rule, the strictest across clouds);
        // dashes, dots and leading digits are rejected at construction so the write never reaches
        // the object store with an illegal key.
        final Map<String, String> context = Map.of(illegalKey, "v");

        assertThatThrownBy(
                        () ->
                                new AesGcmEncryptionMetadata(
                                        KEY_ID, KEY_VERSION, IV_BYTE_LENGTH, context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(illegalKey);
    }

    private static Stream<Arguments> illegalContextKeys() {
        return Stream.of(
                Arguments.of("org-id"),
                Arguments.of("lfcp-id"),
                Arguments.of("dotted.key"),
                Arguments.of("1leading-digit"),
                Arguments.of(""));
    }

    // -------------------------------------------------------------------------
    // fromMetadata error cases
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("missingKeyArgs")
    void shouldThrowOnMissingMetadataKey(final Map<String, String> map, final String missingKey) {
        assertThatThrownBy(() -> AesGcmEncryptionMetadata.fromMetadata(map))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(missingKey);
    }

    private static Stream<Arguments> missingKeyArgs() {
        return Stream.of(
                        "x_cse_key_id",
                        "x_cse_key_version",
                        "x_cse_iv_bytes_length",
                        "x_cse_algorithm",
                        "x_cse_gcm_tag_length",
                        "x_cse_chunk_size")
                .map(
                        key -> {
                            final Map<String, String> map = validMap();
                            map.remove(key);
                            return Arguments.of(map, key);
                        });
    }

    @ParameterizedTest
    @MethodSource("invalidValueArgs")
    void shouldThrowOnInvalidMetadataValue(
            final String key, final String value, final String expectedErrorMessage) {
        final Map<String, String> map = validMap();
        map.put(key, value);

        assertThatThrownBy(() -> AesGcmEncryptionMetadata.fromMetadata(map))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedErrorMessage);
    }

    private static Stream<Arguments> invalidValueArgs() {
        return Stream.of(
                Arguments.of(
                        "x_cse_iv_bytes_length",
                        "not-a-number",
                        "Malformed value for 'x_cse_iv_bytes_length'"),
                Arguments.of("x_cse_iv_bytes_length", "0", "IV byte length must be positive"),
                Arguments.of(
                        "x_cse_algorithm",
                        "AES/CTR/NoPadding",
                        "Unsupported algorithm 'AES/CTR/NoPadding'"),
                Arguments.of(
                        "x_cse_gcm_tag_length", "12", "Unsupported GCM tag length 12, expected 16"),
                Arguments.of(
                        "x_cse_gcm_tag_length",
                        "not-a-number",
                        "Malformed value for 'x_cse_gcm_tag_length'"),
                Arguments.of("x_cse_chunk_size", "1048576", "Chunked encryption is not supported"));
    }

    // -------------------------------------------------------------------------
    // Constructor validation
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("nullConstructorArgs")
    void shouldThrowOnNullConstructorArg(
            final String keyId,
            final String keyVersion,
            final Map<String, String> context,
            final String expectedMessageFragment) {
        assertThatThrownBy(
                        () ->
                                new AesGcmEncryptionMetadata(
                                        keyId, keyVersion, IV_BYTE_LENGTH, context))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining(expectedMessageFragment);
    }

    private static Stream<Arguments> nullConstructorArgs() {
        return Stream.of(
                Arguments.of(null, KEY_VERSION, EMPTY_CONTEXT, "keyId"),
                Arguments.of(KEY_ID, null, EMPTY_CONTEXT, "keyVersion"),
                Arguments.of(KEY_ID, KEY_VERSION, null, "encryptionContext"));
    }

    @Test
    void shouldThrowOnNonPositiveIvLength() {
        assertThatThrownBy(
                        () -> new AesGcmEncryptionMetadata(KEY_ID, KEY_VERSION, 0, EMPTY_CONTEXT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ivByteLength must be positive");
    }

    // -------------------------------------------------------------------------
    // equals / hashCode
    // -------------------------------------------------------------------------

    @Test
    void equalsShouldReturnTrueForSameReference() {
        assertThat(DEFAULT_META).isEqualTo(DEFAULT_META);
    }

    @Test
    void equalsShouldReturnFalseForDifferentType() {
        assertThat(DEFAULT_META).isNotEqualTo("not a metadata object");
    }

    @Test
    void shouldBeEqualWhenAllFieldsMatch() {
        final Map<String, String> context = Map.of("org_id", "org-123");
        final AesGcmEncryptionMetadata a =
                new AesGcmEncryptionMetadata(KEY_ID, KEY_VERSION, IV_BYTE_LENGTH, context);
        final AesGcmEncryptionMetadata b =
                new AesGcmEncryptionMetadata(KEY_ID, KEY_VERSION, IV_BYTE_LENGTH, context);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @ParameterizedTest
    @MethodSource("unequalInstances")
    void shouldNotBeEqualWhenFieldDiffers(final AesGcmEncryptionMetadata other) {
        assertThat(DEFAULT_META).isNotEqualTo(other);
    }

    private static Stream<Arguments> unequalInstances() {
        return Stream.of(
                Arguments.of(
                        new AesGcmEncryptionMetadata(
                                "other-key", KEY_VERSION, IV_BYTE_LENGTH, EMPTY_CONTEXT)),
                Arguments.of(
                        new AesGcmEncryptionMetadata(KEY_ID, "v2", IV_BYTE_LENGTH, EMPTY_CONTEXT)),
                Arguments.of(
                        new AesGcmEncryptionMetadata(
                                KEY_ID, KEY_VERSION, IV_BYTE_LENGTH, Map.of("k", "v"))));
    }

    // -------------------------------------------------------------------------
    // toString
    // -------------------------------------------------------------------------

    @Test
    void toStringShouldContainAllFields() {
        final Map<String, String> context = Map.of("org_id", "org-123");
        final AesGcmEncryptionMetadata meta =
                new AesGcmEncryptionMetadata(KEY_ID, KEY_VERSION, IV_BYTE_LENGTH, context);
        final String str = meta.toString();

        assertThat(str)
                .contains(KEY_ID)
                .contains(KEY_VERSION)
                .contains("ivByteLength=12")
                .contains("org_id")
                .contains("org-123");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Map<String, String> validMap() {
        final Map<String, String> map = new HashMap<>();
        map.put("x_cse_key_id", KEY_ID);
        map.put("x_cse_key_version", KEY_VERSION);
        map.put("x_cse_iv_bytes_length", Integer.toString(IV_BYTE_LENGTH));
        map.put("x_cse_algorithm", AesGcmCipherStreams.ALGORITHM);
        map.put("x_cse_gcm_tag_length", Integer.toString(AesGcmEncryptionMetadata.GCM_TAG_LENGTH));
        map.put("x_cse_chunk_size", "-1");
        return map;
    }
}
