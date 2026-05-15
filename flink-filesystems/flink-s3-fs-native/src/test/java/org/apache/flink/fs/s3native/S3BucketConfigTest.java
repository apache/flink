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

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.IllegalConfigurationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3BucketConfig}. */
class S3BucketConfigTest {

    @Test
    void testBuilderWithAllFields() {
        S3BucketConfig config =
                S3BucketConfig.builder("my-bucket")
                        .region("us-west-2")
                        .endpoint("https://custom.s3.endpoint")
                        .pathStyleAccess(true)
                        .accessKey("AKIAIOSFODNN7EXAMPLE")
                        .secretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                        .sseType("sse-kms")
                        .sseKmsKeyId("arn:aws:kms:us-east-1:123:key/abc")
                        .assumeRoleArn("arn:aws:iam::123:role/S3Role")
                        .assumeRoleExternalId("ext-id-123")
                        .assumeRoleSessionName("my-session")
                        .assumeRoleSessionDurationSeconds(7200)
                        .credentialsProvider("AnonymousCredentialsProvider")
                        .build();

        assertThat(config.getBucketName()).isEqualTo("my-bucket");
        assertThat(config.getRegion()).isEqualTo("us-west-2");
        assertThat(config.getEndpoint()).isEqualTo("https://custom.s3.endpoint");
        assertThat(config.getPathStyleAccess()).isTrue();
        assertThat(config.getAccessKey()).isEqualTo("AKIAIOSFODNN7EXAMPLE");
        assertThat(config.getSecretKey()).isEqualTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        assertThat(config.getSseType()).isEqualTo("sse-kms");
        assertThat(config.getSseKmsKeyId()).isEqualTo("arn:aws:kms:us-east-1:123:key/abc");
        assertThat(config.getAssumeRoleArn()).isEqualTo("arn:aws:iam::123:role/S3Role");
        assertThat(config.getAssumeRoleExternalId()).isEqualTo("ext-id-123");
        assertThat(config.getAssumeRoleSessionName()).isEqualTo("my-session");
        assertThat(config.getAssumeRoleSessionDurationSeconds()).isEqualTo(7200);
        assertThat(config.getCredentialsProvider()).isEqualTo("AnonymousCredentialsProvider");
        assertThat(config.hasAnyOverride()).isTrue();
    }

    @Test
    void testNoOverridesHasAnyOverrideFalse() {
        assertThat(S3BucketConfig.builder("empty-bucket").build().hasAnyOverride()).isFalse();
    }

    /** Each field, when set alone, must trigger {@code hasAnyOverride()}. */
    static Stream<S3BucketConfig> singleFieldConfigs() {
        return Stream.of(
                S3BucketConfig.builder("b").region("us-east-1").build(),
                S3BucketConfig.builder("b").endpoint("http://localhost:9000").build(),
                S3BucketConfig.builder("b").pathStyleAccess(true).build(),
                S3BucketConfig.builder("b").accessKey("KEY").secretKey("SECRET").build(),
                S3BucketConfig.builder("b").sseType("sse-s3").build(),
                S3BucketConfig.builder("b").sseKmsKeyId("key-id").build(),
                S3BucketConfig.builder("b").assumeRoleArn("arn:aws:iam::1:role/R").build(),
                S3BucketConfig.builder("b").assumeRoleExternalId("ext-id").build(),
                S3BucketConfig.builder("b").assumeRoleSessionName("session").build(),
                S3BucketConfig.builder("b").assumeRoleSessionDurationSeconds(900).build(),
                S3BucketConfig.builder("b")
                        .credentialsProvider("AnonymousCredentialsProvider")
                        .build());
    }

    @ParameterizedTest
    @MethodSource("singleFieldConfigs")
    void testEachFieldAloneTriggersHasAnyOverride(S3BucketConfig config) {
        assertThat(config.hasAnyOverride())
                .as("hasAnyOverride() must be true when any single field is set")
                .isTrue();
    }

    @Test
    void testPartialCredentialsAccessKeyOnlyRejected() {
        assertThatThrownBy(
                        () -> S3BucketConfig.builder("b").accessKey("AKIAIOSFODNN7EXAMPLE").build())
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("access-key")
                .hasMessageContaining("secret-key")
                .hasMessageContaining("must be set together");
    }

    @Test
    void testPartialCredentialsSecretKeyOnlyRejected() {
        assertThatThrownBy(
                        () ->
                                S3BucketConfig.builder("b")
                                        .secretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                                        .build())
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("access-key")
                .hasMessageContaining("secret-key")
                .hasMessageContaining("must be set together");
    }

    @Test
    void testToStringRedactsCredentials() {
        S3BucketConfig config =
                S3BucketConfig.builder("secure-bucket")
                        .accessKey("AKIAIOSFODNN7EXAMPLE")
                        .secretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                        .region("us-east-1")
                        .build();

        String str = config.toString();
        assertThat(str).contains("credentials=" + GlobalConfiguration.HIDDEN_CONTENT);
        assertThat(str).doesNotContain("AKIAIOSFODNN7EXAMPLE");
        assertThat(str).doesNotContain("wJalrXUtnFEMI");
    }

    @Test
    void testToStringRedactsKmsKeyIdAndIncludesAllFields() {
        S3BucketConfig config =
                S3BucketConfig.builder("my-bucket")
                        .region("us-west-2")
                        .endpoint("https://s3.example.com")
                        .pathStyleAccess(true)
                        .accessKey("AKIAIOSFODNN7EXAMPLE")
                        .secretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                        .sseType("sse-kms")
                        .sseKmsKeyId("arn:aws:kms:us-east-1:123:key/abc")
                        .assumeRoleArn("arn:aws:iam::123:role/R")
                        .assumeRoleExternalId("ext-id")
                        .assumeRoleSessionName("my-session")
                        .assumeRoleSessionDurationSeconds(3600)
                        .credentialsProvider("AnonymousCredentialsProvider")
                        .build();

        String str = config.toString();
        assertThat(str).contains("region='us-west-2'");
        assertThat(str).contains("endpoint='https://s3.example.com'");
        assertThat(str).contains("pathStyleAccess=true");
        assertThat(str).contains("sseType='sse-kms'");
        assertThat(str).contains("sseKmsKeyId=" + GlobalConfiguration.HIDDEN_CONTENT);
        assertThat(str).doesNotContain("arn:aws:kms:us-east-1:123:key/abc");
        assertThat(str).contains("assumeRoleArn='arn:aws:iam::123:role/R'");
        assertThat(str).contains("assumeRoleExternalId='ext-id'");
        assertThat(str).contains("assumeRoleSessionName='my-session'");
        assertThat(str).contains("assumeRoleSessionDurationSeconds=3600");
        assertThat(str).contains("credentialsProvider='AnonymousCredentialsProvider'");
    }
}
