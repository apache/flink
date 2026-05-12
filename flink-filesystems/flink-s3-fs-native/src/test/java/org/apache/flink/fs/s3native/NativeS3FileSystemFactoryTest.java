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
        Configuration config = baseConfig();
        config.setString("s3.endpoint", "http://localhost:9000");
        assertThat(createFs(config)).isNotNull();
    }

    @Test
    void testS3ACreateFileSystemWithMinimalConfiguration() throws Exception {
        NativeS3FileSystem fs = createS3aFs(baseConfig());
        assertThat(fs.getUri()).isEqualTo(URI.create("s3a://test-bucket/"));
    }

    @Test
    void testS3ACreateFileSystemWithCustomEndpoint() throws Exception {
        Configuration config = baseConfig();
        config.setString("s3.endpoint", "http://localhost:9000");
        assertThat(createS3aFs(config)).isNotNull();
    }

    // --- Path-style access ---

    @Test
    void testPathStyleAccessDefaultIsFalse() throws Exception {
        assertThat(createFs(baseConfig()).getClientProvider().isPathStyleAccess()).isFalse();
    }

    @Test
    void testPathStyleAccessExplicitlyEnabled() throws Exception {
        Configuration config = baseConfig();
        config.set(NativeS3FileSystemFactory.PATH_STYLE_ACCESS, true);
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
    void testExplicitRegionConfiguration() throws Exception {
        Configuration config = baseConfig();
        config.setString("s3.region", "eu-west-1");
        assertThat(createFs(config)).isNotNull();
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
    void testS3AWithSSEConfiguration() throws Exception {
        Configuration config = baseConfig();
        config.setString("s3.sse.type", "sse-s3");
        assertThat(createS3aFs(config)).isNotNull();
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
}
