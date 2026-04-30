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

    @ParameterizedTest
    @MethodSource
    void noArgFactories_encryptionTypeCorrect(
            S3EncryptionConfig config,
            S3EncryptionConfig.EncryptionType expectedType,
            boolean expectedEnabled,
            ServerSideEncryption expectedSse) {
        assertThat(config.getEncryptionType()).isEqualTo(expectedType);
        assertThat(config.isEnabled()).isEqualTo(expectedEnabled);
        assertThat(config.getKmsKeyId()).isNull();
        assertThat(config.getServerSideEncryption()).isEqualTo(expectedSse);
    }

    static Stream<Arguments> noArgFactories_encryptionTypeCorrect() {
        return Stream.of(
                Arguments.of(S3EncryptionConfig.none(), NONE, false, null),
                Arguments.of(S3EncryptionConfig.sseS3(), SSE_S3, true, ServerSideEncryption.AES256),
                Arguments.of(
                        S3EncryptionConfig.sseKms(), SSE_KMS, true, ServerSideEncryption.AWS_KMS));
    }

    @Test
    void sseKms_withKeyId_keyIdStoredAndEnabled() {
        S3EncryptionConfig c = S3EncryptionConfig.sseKms("arn:aws:kms:us-east-1:123:key/abc");

        assertThat(c.getKmsKeyId()).isEqualTo("arn:aws:kms:us-east-1:123:key/abc");
        assertThat(c.isEnabled()).isTrue();
    }

    @Test
    void sseKms_withContext_contextStoredDefensively() {
        Map<String, String> ctx = new HashMap<>(Map.of("dept", "finance"));
        S3EncryptionConfig c = S3EncryptionConfig.sseKms("key-id", ctx);
        ctx.put("extra", "value");

        assertThat(c.getEncryptionContext()).isEqualTo(Map.of("dept", "finance"));
        assertThat(c.hasEncryptionContext()).isTrue();
    }

    @Test
    void sseKms_nullContext_contextIsEmpty() {
        S3EncryptionConfig c = S3EncryptionConfig.sseKms("key-id", null);

        assertThat(c.getEncryptionContext()).isEmpty();
        assertThat(c.hasEncryptionContext()).isFalse();
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
                Arguments.of("sse-s3", SSE_S3),
                Arguments.of("AES256", SSE_S3),
                Arguments.of("sse-kms", SSE_KMS),
                Arguments.of("aws:kms", SSE_KMS));
    }

    @Test
    void fromConfig_sseKmsWithKeyAndContext_keyAndContextPreserved() {
        S3EncryptionConfig result =
                S3EncryptionConfig.fromConfig("sse-kms", "my-key", Map.of("env", "prod"));

        assertThat(result.getKmsKeyId()).isEqualTo("my-key");
        assertThat(result.getEncryptionContext()).isEqualTo(Map.of("env", "prod"));
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
                        S3EncryptionConfig.fromConfig("sse-kms", null, Map.of("env", "prod"))
                                .hasEncryptionContext())
                .isTrue();
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

    @Test
    void toString_noKeyOrContext_containsTypeOnly() {
        S3EncryptionConfig c = S3EncryptionConfig.none();

        assertThat(c.toString()).contains("NONE");
        assertThat(c.toString()).doesNotContain("kmsKeyId");
        assertThat(c.toString()).doesNotContain("encryptionContext");
    }

    @Test
    void toString_withKeyId_includesKeyId() {
        S3EncryptionConfig c = S3EncryptionConfig.sseKms("my-key");

        assertThat(c.toString()).contains("my-key");
    }

    @Test
    void toString_withContext_includesContextKeys() {
        S3EncryptionConfig c = S3EncryptionConfig.sseKms("k", Map.of("dept", "finance"));

        assertThat(c.toString()).contains("dept");
    }

    @Test
    void serialization_roundTrip_preservesAllFields() throws Exception {
        S3EncryptionConfig original = S3EncryptionConfig.sseKms("key-id", Map.of("k", "v"));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        new ObjectOutputStream(bos).writeObject(original);
        S3EncryptionConfig copy =
                (S3EncryptionConfig)
                        new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))
                                .readObject();

        assertThat(copy.getEncryptionType()).isEqualTo(original.getEncryptionType());
        assertThat(copy.getKmsKeyId()).isEqualTo(original.getKmsKeyId());
        assertThat(copy.getEncryptionContext()).isEqualTo(original.getEncryptionContext());
    }
}
