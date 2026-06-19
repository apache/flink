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

package org.apache.flink.fs.s3native;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.fs.s3native.S3EncryptionConfig.EncryptionType.NONE;
import static org.apache.flink.fs.s3native.S3EncryptionConfig.EncryptionType.SSE_KMS;
import static org.apache.flink.fs.s3native.S3EncryptionConfig.EncryptionType.SSE_S3;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3EncryptionConfig}. */
class S3EncryptionConfigTest {

    @Test
    void sseKms_withContext_contextStoredDefensively() {
        Map<String, String> ctx =
                new HashMap<>(Map.of("aws:s3:arn", "arn:aws:s3:::my-bucket/my-file"));
        S3EncryptionConfig c = S3EncryptionConfig.sseKms("key-id", ctx);
        ctx.put("extra", "value");

        assertThat(c.getEncryptionContext())
                .isEqualTo(Map.of("aws:s3:arn", "arn:aws:s3:::my-bucket/my-file"));
        assertThat(c.hasEncryptionContext()).isTrue();
    }

    @Test
    void sseKms_nullKeyId_keyIdIsNull() {
        S3EncryptionConfig c = S3EncryptionConfig.sseKms(null, Collections.emptyMap());

        assertThat(c.getKmsKeyId()).isNull();
    }

    @Test
    void sseKms_returnedContext_isUnmodifiable() {
        S3EncryptionConfig c =
                S3EncryptionConfig.sseKms(
                        "key-id", Map.of("aws:s3:arn", "arn:aws:s3:::my-bucket/my-file"));

        assertThatThrownBy(() -> c.getEncryptionContext().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest
    @MethodSource
    void sseKms_contextAbsent_contextIsEmpty(Map<String, String> context) {
        S3EncryptionConfig c = S3EncryptionConfig.sseKms("key-id", context);

        assertThat(c.getEncryptionContext()).isEmpty();
        assertThat(c.hasEncryptionContext()).isFalse();
    }

    static Stream<Arguments> sseKms_contextAbsent_contextIsEmpty() {
        return Stream.of(Arguments.of((Object) null), Arguments.of(Collections.emptyMap()));
    }

    @ParameterizedTest
    @MethodSource
    void getServerSideEncryption_allTypes_returnsCorrectSseValue(
            String configType, ServerSideEncryption expected) {
        S3EncryptionConfig c =
                S3EncryptionConfig.fromConfig(configType, null, Collections.emptyMap());

        assertThat(c.getServerSideEncryption()).isEqualTo(expected);
    }

    static Stream<Arguments> getServerSideEncryption_allTypes_returnsCorrectSseValue() {
        return Stream.of(
                Arguments.of(null, null),
                Arguments.of("sse-s3", ServerSideEncryption.AES256),
                Arguments.of("sse-kms", ServerSideEncryption.AWS_KMS));
    }

    @ParameterizedTest
    @MethodSource
    void fromConfig_typeVariants_returnExpectedType(
            String input, S3EncryptionConfig.EncryptionType expected) {
        assertThat(
                        S3EncryptionConfig.fromConfig(input, null, Collections.emptyMap())
                                .getEncryptionType())
                .isEqualTo(expected);
    }

    static Stream<Arguments> fromConfig_typeVariants_returnExpectedType() {
        return Stream.of(
                Arguments.of(null, NONE),
                Arguments.of("", NONE),
                Arguments.of("none", NONE),
                Arguments.of("NONE", NONE),
                Arguments.of("   ", NONE),
                Arguments.of("sse-s3", SSE_S3),
                Arguments.of("AES256", SSE_S3),
                Arguments.of("sse-kms", SSE_KMS),
                Arguments.of("aws:kms", SSE_KMS));
    }

    @Test
    void fromConfig_sseKmsWithKeyAndContext_keyAndContextPreserved() {
        S3EncryptionConfig result =
                S3EncryptionConfig.fromConfig(
                        "sse-kms",
                        "my-key",
                        Map.of("aws:s3:arn", "arn:aws:s3:::my-bucket/my-file"));

        assertThat(result.getKmsKeyId()).isEqualTo("my-key");
        assertThat(result.getEncryptionContext())
                .isEqualTo(Map.of("aws:s3:arn", "arn:aws:s3:::my-bucket/my-file"));
    }

    @ParameterizedTest
    @MethodSource
    void fromConfig_sseKmsWithNoKeyId_keyIdIsNull(String kmsKeyId) {
        assertThat(
                        S3EncryptionConfig.fromConfig("sse-kms", kmsKeyId, Collections.emptyMap())
                                .getKmsKeyId())
                .isNull();
    }

    static Stream<Arguments> fromConfig_sseKmsWithNoKeyId_keyIdIsNull() {
        return Stream.of(Arguments.of((Object) null), Arguments.of(""));
    }

    @Test
    void fromConfig_sseKmsDefaultKeyWithContext_contextPreserved() {
        assertThat(
                        S3EncryptionConfig.fromConfig(
                                        "sse-kms",
                                        null,
                                        Map.of("aws:s3:arn", "arn:aws:s3:::my-bucket/my-file"))
                                .hasEncryptionContext())
                .isTrue();
    }

    @Test
    void fromConfig_sseS3WithContext_contextIgnored() {
        S3EncryptionConfig c =
                S3EncryptionConfig.fromConfig(
                        "sse-s3", null, Map.of("aws:s3:arn", "arn:aws:s3:::my-bucket/my-file"));

        assertThat(c.getEncryptionType()).isEqualTo(SSE_S3);
        assertThat(c.getEncryptionContext()).isEmpty();
    }

    @Test
    void fromConfig_sseS3_kmsKeyIdIgnored() {
        S3EncryptionConfig c =
                S3EncryptionConfig.fromConfig("sse-s3", "some-key", Collections.emptyMap());

        assertThat(c.getKmsKeyId()).isNull();
    }

    @Test
    void fromConfig_unknownType_throwsIllegalArgument() {
        assertThatThrownBy(
                        () ->
                                S3EncryptionConfig.fromConfig(
                                        "invalid-type", null, Collections.emptyMap()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid-type");
    }

    @ParameterizedTest
    @MethodSource
    void isEnabled_encryptionType_returnsCorrectState(S3EncryptionConfig config, boolean expected) {
        assertThat(config.isEnabled()).isEqualTo(expected);
    }

    static Stream<Arguments> isEnabled_encryptionType_returnsCorrectState() {
        return Stream.of(
                Arguments.of(
                        S3EncryptionConfig.fromConfig(null, null, Collections.emptyMap()), false),
                Arguments.of(
                        S3EncryptionConfig.fromConfig("sse-s3", null, Collections.emptyMap()),
                        true),
                Arguments.of(
                        S3EncryptionConfig.fromConfig("sse-kms", null, Collections.emptyMap()),
                        true));
    }

    @ParameterizedTest
    @MethodSource
    void serializeEncryptionContext_exactOutput_correctBase64Json(
            Map<String, String> context, String expectedDecoded) {
        S3EncryptionConfig c = S3EncryptionConfig.sseKms(null, context);
        String decoded = new String(Base64.getDecoder().decode(c.serializeEncryptionContext()));

        assertThat(decoded).isEqualTo(expectedDecoded);
    }

    static Stream<Arguments> serializeEncryptionContext_exactOutput_correctBase64Json() {
        return Stream.of(
                Arguments.of(Collections.emptyMap(), "{}"),
                Arguments.of(Map.of("k", "v"), "{\"k\":\"v\"}"));
    }

    @Test
    void serializeEncryptionContext_multipleEntries_allEntriesPresent() {
        S3EncryptionConfig c = S3EncryptionConfig.sseKms(null, Map.of("k1", "v1", "k2", "v2"));
        String decoded = new String(Base64.getDecoder().decode(c.serializeEncryptionContext()));

        assertThat(decoded).contains("\"k1\":\"v1\"", "\"k2\":\"v2\"");
    }

    @ParameterizedTest
    @MethodSource
    void serializeEncryptionContext_jsonSpecialChars_escapedCorrectly(
            String key, String value, String expectedFragment) {
        S3EncryptionConfig c = S3EncryptionConfig.sseKms(null, Map.of(key, value));
        String decoded = new String(Base64.getDecoder().decode(c.serializeEncryptionContext()));

        assertThat(decoded).contains(expectedFragment);
    }

    static Stream<Arguments> serializeEncryptionContext_jsonSpecialChars_escapedCorrectly() {
        return Stream.of(
                Arguments.of("k", "val\"ue", "\"k\":\"val\\\"ue\""),
                Arguments.of("k", "val\\ue", "\"k\":\"val\\\\ue\""),
                Arguments.of("k\"ey", "v", "\"k\\\"ey\":\"v\""),
                Arguments.of("k\\ey", "v", "\"k\\\\ey\":\"v\""),
                Arguments.of("k", "\\\"", "\"k\":\"\\\\\\\"\""));
    }

    @ParameterizedTest
    @MethodSource
    void serialization_roundTrip_preservesAllFields(S3EncryptionConfig config) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        new ObjectOutputStream(bos).writeObject(config);
        S3EncryptionConfig copy =
                (S3EncryptionConfig)
                        new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))
                                .readObject();

        assertThat(copy.getEncryptionType()).isEqualTo(config.getEncryptionType());
        assertThat(copy.getKmsKeyId()).isEqualTo(config.getKmsKeyId());
        assertThat(copy.getEncryptionContext()).isEqualTo(config.getEncryptionContext());
    }

    static Stream<Arguments> serialization_roundTrip_preservesAllFields() {
        return Stream.of(
                Arguments.of(S3EncryptionConfig.sseKms("key-id", Map.of("k", "v"))),
                Arguments.of(S3EncryptionConfig.none()),
                Arguments.of(S3EncryptionConfig.sseS3()),
                Arguments.of(S3EncryptionConfig.sseKms(null, null)));
    }
}
