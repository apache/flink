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

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for bucket-level S3 configuration support. */
public class BucketConfigProviderTest {

    @Test
    public void testParseBucketConfigs() {
        Configuration config = new Configuration();

        config.setString("s3.bucket.checkpoint-bucket.path-style-access", "true");
        config.setString("s3.bucket.checkpoint-bucket.access-key", "checkpointAccessKey");
        config.setString("s3.bucket.checkpoint-bucket.secret-key", "checkpointSecretKey");

        config.setString("s3.bucket.savepoint-bucket.path-style-access", "false");
        config.setString("s3.bucket.savepoint-bucket.endpoint", "https://s3.example.com");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.hasBucketConfig("checkpoint-bucket")).isTrue();
        assertThat(provider.hasBucketConfig("savepoint-bucket")).isTrue();
        assertThat(provider.hasBucketConfig("unknown-bucket")).isFalse();

        S3BucketConfig checkpointBucketConfig = provider.getBucketConfig("checkpoint-bucket");
        assertThat(checkpointBucketConfig).isNotNull();
        assertThat(checkpointBucketConfig.getBucketName()).isEqualTo("checkpoint-bucket");
        assertThat(checkpointBucketConfig.isPathStyleAccess()).isTrue();
        assertThat(checkpointBucketConfig.getAccessKey()).isEqualTo("checkpointAccessKey");
        assertThat(checkpointBucketConfig.getSecretKey()).isEqualTo("checkpointSecretKey");

        S3BucketConfig savepointBucketConfig = provider.getBucketConfig("savepoint-bucket");
        assertThat(savepointBucketConfig).isNotNull();
        assertThat(savepointBucketConfig.getBucketName()).isEqualTo("savepoint-bucket");
        assertThat(savepointBucketConfig.isPathStyleAccess()).isFalse();
        assertThat(savepointBucketConfig.getEndpoint()).isEqualTo("https://s3.example.com");
    }

    @Test
    public void testBucketConfigWithEncryption() {
        Configuration config = new Configuration();

        config.setString("s3.bucket.encrypted-bucket.sse.type", "sse-kms");
        config.setString(
                "s3.bucket.encrypted-bucket.sse.kms-key-id",
                "arn:aws:kms:us-east-1:123456789:key/12345678");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        S3BucketConfig bucketConfig = provider.getBucketConfig("encrypted-bucket");
        assertThat(bucketConfig).isNotNull();
        assertThat(bucketConfig.getSseType()).isEqualTo("sse-kms");
        assertThat(bucketConfig.getSseKmsKeyId())
                .isEqualTo("arn:aws:kms:us-east-1:123456789:key/12345678");
    }

    @Test
    public void testBucketConfigWithAssumeRole() {
        Configuration config = new Configuration();

        config.setString(
                "s3.bucket.cross-account-bucket.assume-role.arn",
                "arn:aws:iam::123456789012:role/S3AccessRole");
        config.setString(
                "s3.bucket.cross-account-bucket.assume-role.external-id", "external-id-value");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        S3BucketConfig bucketConfig = provider.getBucketConfig("cross-account-bucket");
        assertThat(bucketConfig).isNotNull();
        assertThat(bucketConfig.getAssumeRoleArn())
                .isEqualTo("arn:aws:iam::123456789012:role/S3AccessRole");
        assertThat(bucketConfig.getAssumeRoleExternalId()).isEqualTo("external-id-value");
    }

    @Test
    public void testBucketNameWithDots() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my.bucket.name.path-style-access", "true");
        config.setString("s3.bucket.my.bucket.name.endpoint", "https://s3.example.com");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        S3BucketConfig bucketConfig = provider.getBucketConfig("my.bucket.name");
        assertThat(bucketConfig).isNotNull();
        assertThat(bucketConfig.getBucketName()).isEqualTo("my.bucket.name");
        assertThat(bucketConfig.isPathStyleAccess()).isTrue();
        assertThat(bucketConfig.getEndpoint()).isEqualTo("https://s3.example.com");
    }

    @Test
    public void testS3BucketConfigBuilder() {
        S3BucketConfig config =
                S3BucketConfig.builder("test-bucket")
                        .pathStyleAccess(true)
                        .endpoint("https://s3-compatible.example.com")
                        .region("us-west-2")
                        .accessKey("access-key")
                        .secretKey("secret-key")
                        .sseType("sse-s3")
                        .assumeRoleArn("arn:aws:iam::123456789012:role/S3AccessRole")
                        .build();

        assertThat(config.getBucketName()).isEqualTo("test-bucket");
        assertThat(config.isPathStyleAccess()).isTrue();
        assertThat(config.getEndpoint()).isEqualTo("https://s3-compatible.example.com");
        assertThat(config.getRegion()).isEqualTo("us-west-2");
        assertThat(config.getAccessKey()).isEqualTo("access-key");
        assertThat(config.getSecretKey()).isEqualTo("secret-key");
        assertThat(config.getSseType()).isEqualTo("sse-s3");
        assertThat(config.getAssumeRoleArn())
                .isEqualTo("arn:aws:iam::123456789012:role/S3AccessRole");
    }
}
