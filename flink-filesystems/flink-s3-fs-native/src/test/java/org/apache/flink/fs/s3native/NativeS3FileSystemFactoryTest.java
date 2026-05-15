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
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NativeS3FileSystemFactory}. */
class NativeS3FileSystemFactoryTest {
    private static Configuration baseConfig() {
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "us-east-1");
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));
        return config;
    }

    private static NativeS3FileSystem createFs(Configuration config) throws Exception {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        factory.configure(config);
        return (NativeS3FileSystem) factory.create(URI.create("s3://test-bucket/"));
    }

    private static NativeS3FileSystem createS3aFs(Configuration config) throws Exception {
        NativeS3AFileSystemFactory factory = new NativeS3AFileSystemFactory();
        factory.configure(config);
        return (NativeS3FileSystem) factory.create(URI.create("s3a://test-bucket/"));
    }

    @Test
    void testSchemeReturnsS3() {
        assertThat(new NativeS3FileSystemFactory().getScheme()).isEqualTo("s3");
    }

    @Test
    void testS3ASchemeReturnsS3A() {
        assertThat(new NativeS3AFileSystemFactory().getScheme()).isEqualTo("s3a");
    }

    @Test
    void testCreateFileSystemWithMinimalConfiguration() throws Exception {
        NativeS3FileSystem fs = createFs(baseConfig());
        assertThat(fs.getUri()).isEqualTo(URI.create("s3://test-bucket/"));
    }

    @Test
    void testCreateFileSystemWithCustomEndpoint() throws Exception {
        // Global: endpoint A; bucket: endpoint B → bucket endpoint is used
        Configuration config = baseConfig();
        config.setString("s3.endpoint", "http://global.s3:9000");
        config.setString("s3.bucket.test-bucket.endpoint", "http://bucket.s3:9000");
        assertThat(createFs(config).getClientProvider().getEndpoint())
                .isEqualTo("http://bucket.s3:9000");
    }

    @Test
    void testS3ACreateFileSystemWithMinimalConfiguration() throws Exception {
        NativeS3FileSystem fs = createS3aFs(baseConfig());
        assertThat(fs.getUri()).isEqualTo(URI.create("s3a://test-bucket/"));
    }

    @Test
    void testS3ACreateFileSystemWithCustomEndpoint() throws Exception {
        // Global: endpoint A; bucket: endpoint B → bucket endpoint is used on s3a scheme
        Configuration config = baseConfig();
        config.setString("s3.endpoint", "http://global.s3:9000");
        config.setString("s3.bucket.test-bucket.endpoint", "http://bucket.s3:9000");
        assertThat(createS3aFs(config).getClientProvider().getEndpoint())
                .isEqualTo("http://bucket.s3:9000");
    }

    // --- Path-style access ---

    @Test
    void testPathStyleAccessDefaultIsFalse() throws Exception {
        assertThat(createFs(baseConfig()).getClientProvider().isPathStyleAccess()).isFalse();
    }

    @Test
    void testPathStyleAccessBucketOverridesGlobal() throws Exception {
        // Global: false; bucket: true → bucket wins
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.PATH_STYLE_ACCESS, false);
        config.setString("s3.bucket.test-bucket.path-style-access", "true");
        assertThat(createFs(config).getClientProvider().isPathStyleAccess()).isTrue();
    }

    // --- Chunked encoding ---

    @Test
    void testChunkedEncodingDefaultIsTrue() throws Exception {
        assertThat(createFs(baseConfig()).getClientProvider().isChunkedEncoding()).isTrue();
    }

    @Test
    void testChunkedEncodingExplicitlyDisabled() throws Exception {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.CHUNKED_ENCODING_ENABLED, false);
        assertThat(createFs(config).getClientProvider().isChunkedEncoding()).isFalse();
    }

    // --- Checksum validation ---

    @Test
    void testChecksumValidationDefaultIsTrue() throws Exception {
        assertThat(createFs(baseConfig()).getClientProvider().isChecksumValidation()).isTrue();
    }

    @Test
    void testChecksumValidationExplicitlyDisabled() throws Exception {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.CHECKSUM_VALIDATION_ENABLED, false);
        assertThat(createFs(config).getClientProvider().isChecksumValidation()).isFalse();
    }

    // --- Max connections ---

    @Test
    void testMaxConnectionsDefault() throws Exception {
        assertThat(createFs(baseConfig()).getClientProvider().getMaxConnections()).isEqualTo(50);
    }

    @Test
    void testMaxConnectionsExplicitlyConfigured() throws Exception {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.MAX_CONNECTIONS, 100);
        assertThat(createFs(config).getClientProvider().getMaxConnections()).isEqualTo(100);
    }

    @Test
    void testInvalidMaxConnectionsThrowsException() {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.MAX_CONNECTIONS, 0);
        assertThatThrownBy(() -> createFs(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.connection.max")
                .hasMessageContaining("must be a positive integer");
    }

    // --- Max retries ---

    @Test
    void testMaxRetriesDefault() throws Exception {
        assertThat(createFs(baseConfig()).getClientProvider().getMaxRetries()).isEqualTo(3);
    }

    @Test
    void testMaxRetriesExplicitlyConfigured() throws Exception {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.MAX_RETRIES, 5);
        assertThat(createFs(config).getClientProvider().getMaxRetries()).isEqualTo(5);
    }

    // --- Timeouts ---

    @Test
    void testCustomTimeoutConfiguration() throws Exception {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.CONNECTION_TIMEOUT, Duration.ofSeconds(30));
        config.set(NativeS3FileSystemFactory.SOCKET_TIMEOUT, Duration.ofSeconds(45));
        config.set(NativeS3FileSystemFactory.CONNECTION_MAX_IDLE_TIME, Duration.ofMinutes(2));
        config.set(NativeS3FileSystemFactory.FS_CLOSE_TIMEOUT, Duration.ofSeconds(90));
        config.set(NativeS3FileSystemFactory.CLIENT_CLOSE_TIMEOUT, Duration.ofSeconds(15));

        NativeS3FileSystem fs = createFs(config);
        assertThat(fs.getFsCloseTimeout()).isEqualTo(Duration.ofSeconds(90));

        S3ClientProvider clientProvider = fs.getClientProvider();
        assertThat(clientProvider.getConnectionTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(clientProvider.getSocketTimeout()).isEqualTo(Duration.ofSeconds(45));
        assertThat(clientProvider.getConnectionMaxIdleTime()).isEqualTo(Duration.ofMinutes(2));
        assertThat(clientProvider.getClientCloseTimeout()).isEqualTo(Duration.ofSeconds(15));
    }

    @Test
    void testTimeoutConfigurationWithStringDuration() throws Exception {
        Configuration config = baseConfig();
        config.setString("s3.connection.timeout", "30 s");
        config.setString("s3.socket.timeout", "2 min");
        config.setString("s3.close.timeout", "1 min");

        NativeS3FileSystem fs = createFs(config);
        assertThat(fs.getFsCloseTimeout()).isEqualTo(Duration.ofMinutes(1));

        S3ClientProvider clientProvider = fs.getClientProvider();
        assertThat(clientProvider.getConnectionTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(clientProvider.getSocketTimeout()).isEqualTo(Duration.ofMinutes(2));
    }

    @Test
    void testInvalidTimeoutConfigurationThrowsException() {
        Configuration config = baseConfig();
        config.setString("s3.connection.timeout", "not-a-duration");
        assertThatThrownBy(() -> createFs(config)).isInstanceOf(IllegalArgumentException.class);
    }

    // --- Part upload size ---

    @Test
    void testPartSizeTooSmallThrowsException() {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.PART_UPLOAD_MIN_SIZE, 1024L);
        assertThatThrownBy(() -> createFs(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "s3.upload.min.part.size must be at least 5MB (5242880 bytes), but was 1024 bytes");
    }

    @Test
    void testPartSizeTooLargeThrowsException() {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.PART_UPLOAD_MIN_SIZE, 6L * 1024 * 1024 * 1024);
        assertThatThrownBy(() -> createFs(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "s3.upload.min.part.size must not exceed 5GB (5368709120 bytes), but was 6442450944 bytes");
    }

    // --- Max concurrent uploads ---

    @Test
    void testInvalidMaxConcurrentUploadsThrowsException() {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.MAX_CONCURRENT_UPLOADS, 0);
        assertThatThrownBy(() -> createFs(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("s3.upload.max.concurrent.uploads must be positive, but was 0");
    }

    // --- Entropy injection ---

    @Test
    void testInvalidEntropyKeyThrowsException() {
        Configuration config = baseConfig();
        config.setString("s3.entropy.key", "__INVALID#KEY__");
        assertThatThrownBy(() -> createFs(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Invalid character");
    }

    @Test
    void testInvalidEntropyLengthThrowsException() {
        Configuration config = baseConfig();
        config.setString("s3.entropy.key", "__ENTROPY__");
        config.set(NativeS3FileSystemFactory.ENTROPY_INJECT_LENGTH_OPTION, 0);
        assertThatThrownBy(() -> createFs(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("must be > 0");
    }

    @Test
    void testEntropyInjectionWithValidConfiguration() throws Exception {
        Configuration config = baseConfig();
        config.setString("s3.entropy.key", "__ENTROPY__");
        config.set(NativeS3FileSystemFactory.ENTROPY_INJECT_LENGTH_OPTION, 4);
        assertThat(createFs(config)).isNotNull();
    }

    // --- Bulk copy ---

    @Test
    void testBulkCopyMaxConcurrentClampedToMaxConnections() throws Exception {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.BULK_COPY_ENABLED, true);
        config.set(NativeS3FileSystemFactory.BULK_COPY_MAX_CONCURRENT, 32);
        config.set(NativeS3FileSystemFactory.MAX_CONNECTIONS, 10);

        NativeS3FileSystem fs = createFs(config);
        assertThat(fs.getBulkCopyHelper()).isNotNull();
        assertThat(fs.getBulkCopyHelper().getMaxConcurrentCopies()).isEqualTo(10);
    }

    @Test
    void testBulkCopyMaxConcurrentPreservedWithinMaxConnections() throws Exception {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.BULK_COPY_ENABLED, true);
        config.set(NativeS3FileSystemFactory.BULK_COPY_MAX_CONCURRENT, 10);
        config.set(NativeS3FileSystemFactory.MAX_CONNECTIONS, 50);

        NativeS3FileSystem fs = createFs(config);
        assertThat(fs.getBulkCopyHelper()).isNotNull();
        assertThat(fs.getBulkCopyHelper().getMaxConcurrentCopies()).isEqualTo(10);
    }

    @Test
    void testInvalidBulkCopyMaxConcurrentThrowsException() {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.BULK_COPY_MAX_CONCURRENT, 0);
        assertThatThrownBy(() -> createFs(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.bulk-copy.max-concurrent")
                .hasMessageContaining("must be a positive integer");
    }

    // --- Region ---

    @Test
    void testRegionBucketOverridesGlobal() throws Exception {
        // Global: us-east-1; bucket: eu-west-1 → bucket wins
        Configuration config = baseConfig();
        config.setString("s3.region", "us-east-1");
        config.setString("s3.bucket.test-bucket.region", "eu-west-1");
        assertThat(createFs(config).getClientProvider().getRegion()).isEqualTo("eu-west-1");
    }

    @Test
    void testRegionAutodiscoveryWithoutExplicitConfig() throws Exception {
        Configuration config = baseConfig();
        config.removeConfig(NativeS3FileSystemFactory.REGION);
        try {
            assertThat(createFs(config)).isNotNull();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("AWS region could not be determined");
            assertThat(e.getMessage()).contains("s3.region");
            assertThat(e.getMessage()).contains("AWS_REGION");
        }
    }

    @Test
    void testRegionAutodiscoveryWithCustomEndpoint() throws Exception {
        Configuration config = baseConfig();
        config.setString("s3.endpoint", "http://localhost:9000");
        config.removeConfig(NativeS3FileSystemFactory.REGION);
        try {
            assertThat(createFs(config)).isNotNull();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("AWS region could not be determined");
            assertThat(e.getMessage()).contains("AWS_REGION");
            assertThat(e.getMessage()).contains("~/.aws/config");
        }
    }

    @Test
    void testEmptyRegionFallsBackToAutodiscovery() throws Exception {
        Configuration config = baseConfig();
        config.setString("s3.region", "");
        try {
            assertThat(createFs(config)).isNotNull();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("AWS region could not be determined");
        }
    }

    // --- s3a scheme ---

    @Test
    void testS3AWithSSEBucketOverridesGlobal() throws Exception {
        // Global: none; bucket: sse-s3 → bucket wins on the s3a scheme
        Configuration config = baseConfig();
        config.setString("s3.sse.type", "none");
        config.setString("s3.bucket.test-bucket.sse.type", "sse-s3");
        assertThat(
                        createS3aFs(config)
                                .getClientProvider()
                                .getEncryptionConfig()
                                .getEncryptionType())
                .isEqualTo(S3EncryptionConfig.EncryptionType.SSE_S3);
    }

    @Test
    void testS3AInheritsS3Configuration() throws Exception {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.PATH_STYLE_ACCESS, true);
        config.set(NativeS3FileSystemFactory.CHUNKED_ENCODING_ENABLED, false);
        config.set(NativeS3FileSystemFactory.CHECKSUM_VALIDATION_ENABLED, false);

        NativeS3FileSystem fs = createS3aFs(config);
        assertThat(fs.getClientProvider().isPathStyleAccess()).isTrue();
        assertThat(fs.getClientProvider().isChunkedEncoding()).isFalse();
        assertThat(fs.getClientProvider().isChecksumValidation()).isFalse();
    }

    // ---- Bucket-level configuration tests ----

    /**
     * Validates that misconfigured per-bucket credentials surface as a configuration error at
     * {@code configure()} time, not as an opaque AWS SDK error at first request. Override
     * resolution itself (which wins between bucket and global) is exhaustively covered by {@code
     * BucketConfigProviderTest}; this test guards the factory-layer behaviour that is unique to it:
     * throwing on partial bucket credentials.
     */
    @Test
    void testBucketSpecificPartialCredentialsThrows() {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "global-access-key");
        config.setString("s3.secret-key", "global-secret-key");
        config.setString("s3.region", "us-east-1");
        config.setString("s3.bucket.bad-bucket.access-key", "only-access-key");
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        assertThatThrownBy(() -> factory.configure(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("must be set together");
    }

    @Test
    void testBucketOverrideWinsForConnectionAndEncryptionFields() throws Exception {
        Configuration config = new Configuration();
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));
        // Global
        config.setString("s3.access-key", "global-access");
        config.setString("s3.secret-key", "global-secret");
        config.setString("s3.region", "us-east-1");
        config.setString("s3.endpoint", "http://global.s3:9000");
        config.set(NativeS3FileSystemFactory.PATH_STYLE_ACCESS, false);
        config.setString("s3.sse.type", "sse-s3");
        config.setString("s3.sse.kms.key-id", "global-kms-key");
        // Bucket
        config.setString("s3.bucket.test-bucket.access-key", "bucket-access");
        config.setString("s3.bucket.test-bucket.secret-key", "bucket-secret");
        config.setString("s3.bucket.test-bucket.region", "eu-west-1");
        config.setString("s3.bucket.test-bucket.endpoint", "http://bucket.s3:9000");
        config.setString("s3.bucket.test-bucket.path-style-access", "true");
        config.setString("s3.bucket.test-bucket.sse.type", "sse-kms");
        config.setString("s3.bucket.test-bucket.sse.kms.key-id", "bucket-kms-key");

        S3ClientProvider provider = createFs(config).getClientProvider();

        // All bucket values win over global
        assertThat(provider.getRegion()).isEqualTo("eu-west-1");
        assertThat(provider.getEndpoint()).isEqualTo("http://bucket.s3:9000");
        assertThat(provider.isPathStyleAccess()).isTrue();
        assertThat(provider.getCredentialsProvider().resolveCredentials().accessKeyId())
                .isEqualTo("bucket-access");
        assertThat(provider.getCredentialsProvider().resolveCredentials().secretAccessKey())
                .isEqualTo("bucket-secret");
        assertThat(provider.getEncryptionConfig().getEncryptionType())
                .isEqualTo(S3EncryptionConfig.EncryptionType.SSE_KMS);
        assertThat(provider.getEncryptionConfig().getKmsKeyId()).isEqualTo("bucket-kms-key");
    }

    @Test
    void testBucketOverrideWinsForAssumeRoleFields() throws Exception {
        Configuration config = new Configuration();
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));
        config.setString("s3.access-key", "global-access");
        config.setString("s3.secret-key", "global-secret");
        config.setString("s3.region", "us-east-1");
        // Global assume-role
        config.setString("s3.assume-role.arn", "arn:aws:iam::111111111111:role/GlobalRole");
        config.setString("s3.assume-role.external-id", "global-ext-id");
        config.setString("s3.assume-role.session-name", "global-session");
        config.set(NativeS3FileSystemFactory.ASSUME_ROLE_SESSION_DURATION_SECONDS, 900);
        // Bucket assume-role overrides
        config.setString(
                "s3.bucket.test-bucket.assume-role.arn",
                "arn:aws:iam::222222222222:role/BucketRole");
        config.setString("s3.bucket.test-bucket.assume-role.external-id", "bucket-ext-id");
        config.setString("s3.bucket.test-bucket.assume-role.session-name", "bucket-session");
        config.setString("s3.bucket.test-bucket.assume-role.session-duration", "1800");

        S3ClientProvider provider = createFs(config).getClientProvider();
        assertThat(provider.getAssumeRoleArn())
                .isEqualTo("arn:aws:iam::222222222222:role/BucketRole");
        assertThat(provider.getAssumeRoleExternalId()).isEqualTo("bucket-ext-id");
        assertThat(provider.getAssumeRoleSessionName()).isEqualTo("bucket-session");
        assertThat(provider.getAssumeRoleSessionDurationSeconds()).isEqualTo(1800);
    }

    @Test
    void testBucketOverrideWinsForCredentialsProvider() throws Exception {
        Configuration config = new Configuration();
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));
        config.setString("s3.region", "us-east-1");
        // Global: static credentials
        config.setString("s3.access-key", "global-access");
        config.setString("s3.secret-key", "global-secret");
        // Bucket: AnonymousCredentialsProvider
        config.setString(
                "s3.bucket.test-bucket.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider");
        AwsCredentials creds =
                createFs(config).getClientProvider().getCredentialsProvider().resolveCredentials();

        // Bucket anonymous provider wins; global static key "global-access" must not be used
        assertThat(creds.accessKeyId()).isNotEqualTo("global-access");
    }

    @Test
    void testBucketOverrideIgnoredForDifferentBucket() throws Exception {
        Configuration config = baseConfig();
        config.setString("s3.region", "us-east-1");
        config.setString("s3.endpoint", "http://global.s3:9000");
        config.setString("s3.bucket.other-bucket.region", "ap-south-1");
        config.setString("s3.bucket.other-bucket.endpoint", "http://other.s3:9000");

        // createFs uses URI s3://test-bucket/ — "other-bucket" overrides must NOT apply
        NativeS3FileSystem fs = createFs(config);
        assertThat(fs.getClientProvider().getRegion()).isEqualTo("us-east-1");
        assertThat(fs.getClientProvider().getEndpoint()).isEqualTo("http://global.s3:9000");
    }

    @Test
    void testPartialBucketOverrideFallsBackToGlobal() throws Exception {
        Configuration config = baseConfig();
        config.setString("s3.region", "us-east-1");
        config.setString("s3.endpoint", "http://global.s3:9000");
        // Override only region for the bucket — endpoint falls back to global
        config.setString("s3.bucket.test-bucket.region", "eu-central-1");

        NativeS3FileSystem fs = createFs(config);
        assertThat(fs.getClientProvider().getRegion()).isEqualTo("eu-central-1");
        assertThat(fs.getClientProvider().getEndpoint()).isEqualTo("http://global.s3:9000");
    }

    @Test
    void testMissingBucketNameInUriThrowsIOException() {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        factory.configure(baseConfig());

        assertThatThrownBy(() -> factory.create(URI.create("s3:///path/to/file")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("bucket name");
    }
}
