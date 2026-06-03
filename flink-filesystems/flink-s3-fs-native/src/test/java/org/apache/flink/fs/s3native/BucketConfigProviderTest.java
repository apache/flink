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
import org.apache.flink.configuration.IllegalConfigurationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BucketConfigProvider}. */
class BucketConfigProviderTest {

    /** One test exercises all 11 known properties on a single bucket. */
    @Test
    void testParsesAllKnownPropertiesForSingleBucket() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my-bucket.region", "us-west-2");
        config.setString("s3.bucket.my-bucket.endpoint", "https://s3.us-west-2.amazonaws.com");
        config.setString("s3.bucket.my-bucket.path-style-access", "true");
        config.setString("s3.bucket.my-bucket.access-key", "AKIAIOSFODNN7EXAMPLE");
        config.setString(
                "s3.bucket.my-bucket.secret-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        config.setString("s3.bucket.my-bucket.sse.type", "sse-kms");
        config.setString(
                "s3.bucket.my-bucket.sse.kms.key-id",
                "arn:aws:kms:us-east-1:123456789:key/12345678");
        config.setString(
                "s3.bucket.my-bucket.assume-role.arn",
                "arn:aws:iam::123456789012:role/S3AccessRole");
        config.setString("s3.bucket.my-bucket.assume-role.external-id", "ext-id-abc");
        config.setString("s3.bucket.my-bucket.assume-role.session-name", "flink-job");
        config.setString("s3.bucket.my-bucket.assume-role.session-duration", "7200");
        config.setString(
                "s3.bucket.my-bucket.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.size()).isEqualTo(1);
        S3BucketConfig bucket = provider.getBucketConfig("my-bucket");
        assertThat(bucket).isNotNull();
        assertThat(bucket.getRegion()).isEqualTo("us-west-2");
        assertThat(bucket.getEndpoint()).isEqualTo("https://s3.us-west-2.amazonaws.com");
        assertThat(bucket.getPathStyleAccess()).isTrue();
        assertThat(bucket.getAccessKey()).isEqualTo("AKIAIOSFODNN7EXAMPLE");
        assertThat(bucket.getSecretKey()).isEqualTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        assertThat(bucket.getSseType()).isEqualTo("sse-kms");
        assertThat(bucket.getSseKmsKeyId())
                .isEqualTo("arn:aws:kms:us-east-1:123456789:key/12345678");
        assertThat(bucket.getAssumeRoleArn())
                .isEqualTo("arn:aws:iam::123456789012:role/S3AccessRole");
        assertThat(bucket.getAssumeRoleExternalId()).isEqualTo("ext-id-abc");
        assertThat(bucket.getAssumeRoleSessionName()).isEqualTo("flink-job");
        assertThat(bucket.getAssumeRoleSessionDurationSeconds()).isEqualTo(7200);
        assertThat(bucket.getCredentialsProvider())
                .isEqualTo("software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider");
    }

    @Test
    void testParsesMultipleBuckets() {
        Configuration config = new Configuration();
        config.setString(
                "s3.bucket.checkpoint-bucket.endpoint", "https://s3.us-east-1.amazonaws.com");
        config.setString("s3.bucket.checkpoint-bucket.region", "us-east-1");
        config.setString(
                "s3.bucket.savepoint-bucket.endpoint", "https://s3.eu-west-1.amazonaws.com");
        config.setString("s3.bucket.savepoint-bucket.region", "eu-west-1");
        config.setString("s3.bucket.savepoint-bucket.path-style-access", "false");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.size()).isEqualTo(2);

        S3BucketConfig cpConfig = provider.getBucketConfig("checkpoint-bucket");
        assertThat(cpConfig).isNotNull();
        assertThat(cpConfig.getEndpoint()).isEqualTo("https://s3.us-east-1.amazonaws.com");
        assertThat(cpConfig.getRegion()).isEqualTo("us-east-1");
        assertThat(cpConfig.getPathStyleAccess()).isNull();

        S3BucketConfig spConfig = provider.getBucketConfig("savepoint-bucket");
        assertThat(spConfig).isNotNull();
        assertThat(spConfig.getEndpoint()).isEqualTo("https://s3.eu-west-1.amazonaws.com");
        assertThat(spConfig.getRegion()).isEqualTo("eu-west-1");
        assertThat(spConfig.getPathStyleAccess()).isFalse();
    }

    /** Bucket names containing dots are fully supported via longest-suffix matching. */
    @Test
    void testDottedBucketName() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my.company.data.endpoint", "https://s3-custom.example.com");
        config.setString("s3.bucket.my.company.data.region", "ap-southeast-1");
        config.setString("s3.bucket.my.company.data.sse.type", "sse-s3");
        config.setString("s3.bucket.my.company.data.sse.kms.key-id", "key-123");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.hasBucketConfig("my.company.data")).isTrue();
        S3BucketConfig bucket = provider.getBucketConfig("my.company.data");
        assertThat(bucket.getEndpoint()).isEqualTo("https://s3-custom.example.com");
        assertThat(bucket.getRegion()).isEqualTo("ap-southeast-1");
        assertThat(bucket.getSseType()).isEqualTo("sse-s3");
        assertThat(bucket.getSseKmsKeyId()).isEqualTo("key-123");
    }

    @Test
    void testNonBucketConfigKeysIgnored() {
        Configuration config = new Configuration();
        config.setString("s3.access-key", "GLOBAL_KEY");
        config.setString("s3.secret-key", "GLOBAL_SECRET");
        config.setString("s3.region", "us-east-1");
        config.setString("s3.bucket.my-bucket.region", "eu-west-1");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.size()).isEqualTo(1);
        assertThat(provider.hasBucketConfig("my-bucket")).isTrue();
    }

    /** A key whose bucket segment is empty (e.g. {@code s3.bucket..region}) must be ignored. */
    @Test
    void testEmptyBucketSegmentInKeyIsIgnored() {
        Configuration config = new Configuration();
        config.setString("s3.bucket..region", "us-east-1");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.size()).isEqualTo(0);
    }

    /** A bucket whose keys only match unknown properties produces no registered bucket config. */
    @Test
    void testBucketWithOnlyUnknownPropertiesProducesNoConfig() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my-bucket.unknown-property", "some-value");
        config.setString("s3.bucket.my-bucket.another-unknown", "other-value");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.size()).isEqualTo(0);
        assertThat(provider.getBucketConfig("my-bucket")).isNull();
    }

    @Test
    void testUnknownPropertyMixedWithKnownIsIgnored() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my-bucket.unknown-property", "some-value");
        config.setString("s3.bucket.my-bucket.region", "us-east-1");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        S3BucketConfig bucket = provider.getBucketConfig("my-bucket");
        assertThat(bucket).isNotNull();
        assertThat(bucket.getRegion()).isEqualTo("us-east-1");
    }

    @Test
    void testNoBucketConfigReturnsNull() {
        BucketConfigProvider provider = new BucketConfigProvider(new Configuration());

        assertThat(provider.getBucketConfig("non-existent-bucket")).isNull();
        assertThat(provider.size()).isEqualTo(0);
    }

    @Test
    void testEmptyConfigurationProducesNoEntries() {
        assertThat(new BucketConfigProvider(new Configuration()).size()).isEqualTo(0);
    }

    @Test
    void testPartialCredentialsRejected() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.bad-bucket.access-key", "AKIAIOSFODNN7EXAMPLE");

        assertThatThrownBy(() -> new BucketConfigProvider(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("must be set together");
    }

    @Test
    void testInvalidSessionDurationThrowsException() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my-bucket.assume-role.session-duration", "not-a-number");
        config.setString("s3.bucket.my-bucket.region", "us-east-1");

        assertThatThrownBy(() -> new BucketConfigProvider(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Invalid assume-role.session-duration");
    }

    @Test
    void testInvalidPathStyleAccessThrowsException() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my-bucket.path-style-access", "treu");
        config.setString("s3.bucket.my-bucket.region", "us-east-1");

        assertThatThrownBy(() -> new BucketConfigProvider(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Invalid path-style-access");
    }

    @Test
    void testUnrecognizedBucketPropertyIsIgnoredWithoutThrow() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my-bucket.typo-region", "us-east-1");
        config.setString("s3.bucket.my-bucket.region", "eu-west-1");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        S3BucketConfig bucket = provider.getBucketConfig("my-bucket");
        assertThat(bucket).isNotNull();
        assertThat(bucket.getRegion()).isEqualTo("eu-west-1");
    }

    @Test
    void testPropertyApplicatorsCoverAllKnownProperties() {
        assertThat(BucketConfigProvider.PROPERTY_APPLICATORS.size())
                .as("PROPERTY_APPLICATORS must have an entry for every known property")
                .isEqualTo(BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.size());

        assertThat(BucketConfigProvider.PROPERTY_APPLICATORS.keySet())
                .containsExactlyInAnyOrderElementsOf(
                        BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH);
    }

    @Test
    void testKnownPropertiesSortedByDescendingLength() {
        for (int i = 1; i < BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.size(); i++) {
            assertThat(BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.get(i).length())
                    .as(
                            "Property at index %d ('%s') should not be longer than property at index %d ('%s')",
                            i,
                            BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.get(i),
                            i - 1,
                            BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.get(i - 1))
                    .isLessThanOrEqualTo(
                            BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.get(i - 1).length());
        }
    }
}
