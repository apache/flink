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
import org.apache.flink.core.fs.FileSystem;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NativeS3FileSystemFactory}. */
class NativeS3FileSystemFactoryTest {

    @Test
    void testSchemeReturnsS3() {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        assertThat(factory.getScheme()).isEqualTo("s3");
    }

    @Test
    void testConfigureAcceptsConfiguration() {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-key");
        config.setString("s3.secret-key", "test-secret");

        // Should not throw
        factory.configure(config);
    }

    @Test
    void testCreateFileSystemWithMinimalConfiguration() throws Exception {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "us-east-1");
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = new URI("s3://test-bucket/");
        FileSystem fs = factory.create(fsUri);

        assertThat(fs).isNotNull();
        assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
        assertThat(fs.getUri()).isEqualTo(fsUri);
    }

    @Test
    void testCreateFileSystemWithCustomEndpoint() throws Exception {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.endpoint", "http://localhost:9000");
        config.setString("s3.region", "us-east-1");
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = new URI("s3://test-bucket/");
        FileSystem fs = factory.create(fsUri);

        assertThat(fs).isNotNull();
        assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
    }

    @Test
    void testPartSizeTooSmallThrowsException() {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "us-east-1");
        config.set(NativeS3FileSystemFactory.PART_UPLOAD_MIN_SIZE, 1024L); // Too small (< 5MB)
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");
        assertThatThrownBy(() -> factory.create(fsUri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be at least");
    }

    @Test
    void testPartSizeTooLargeThrowsException() {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "us-east-1");
        config.set(
                NativeS3FileSystemFactory.PART_UPLOAD_MIN_SIZE, 6L * 1024 * 1024 * 1024); // > 5GB
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");
        assertThatThrownBy(() -> factory.create(fsUri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not exceed 5GB");
    }

    @Test
    void testInvalidMaxConcurrentUploadsThrowsException() {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "us-east-1");
        config.set(NativeS3FileSystemFactory.MAX_CONCURRENT_UPLOADS, 0);
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");
        assertThatThrownBy(() -> factory.create(fsUri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
    }

    @Test
    void testInvalidEntropyKeyThrowsException() {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "us-east-1");
        config.setString("s3.entropy.key", "__INVALID#KEY__"); // Contains #
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");
        assertThatThrownBy(() -> factory.create(fsUri))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Invalid character");
    }

    @Test
    void testInvalidEntropyLengthThrowsException() {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "us-east-1");
        config.setString("s3.entropy.key", "__ENTROPY__");
        config.set(NativeS3FileSystemFactory.ENTROPY_INJECT_LENGTH_OPTION, 0); // Invalid
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");
        assertThatThrownBy(() -> factory.create(fsUri))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("must be > 0");
    }

    @Test
    void testEntropyInjectionWithValidConfiguration() throws Exception {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "us-east-1");
        config.setString("s3.entropy.key", "__ENTROPY__");
        config.set(NativeS3FileSystemFactory.ENTROPY_INJECT_LENGTH_OPTION, 4);
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");
        FileSystem fs = factory.create(fsUri);

        assertThat(fs).isNotNull();
        assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
    }

    @Test
    void testPathStyleAccessAutoEnabledForCustomEndpoint() throws Exception {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.endpoint", "http://minio:9000");
        config.setString("s3.region", "us-east-1");
        config.set(NativeS3FileSystemFactory.PATH_STYLE_ACCESS, false); // Explicitly set to false
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");
        FileSystem fs = factory.create(fsUri);

        // Should succeed - path-style access is auto-enabled
        assertThat(fs).isNotNull();
    }

    @Test
    void testBulkCopyConfiguration() throws Exception {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "us-east-1");
        config.set(NativeS3FileSystemFactory.BULK_COPY_ENABLED, true);
        config.set(NativeS3FileSystemFactory.BULK_COPY_MAX_CONCURRENT, 32);
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");
        FileSystem fs = factory.create(fsUri);

        assertThat(fs).isNotNull();
        assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
    }

    @Test
    void testExplicitRegionConfiguration() throws Exception {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "eu-west-1"); // Explicit non-default region
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");
        FileSystem fs = factory.create(fsUri);

        assertThat(fs).isNotNull();
        assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
    }

    @Test
    void testExplicitRegionTakesPriorityOverAutodiscovery() throws Exception {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "ap-southeast-1"); // Explicit region
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");
        FileSystem fs = factory.create(fsUri);

        // Should succeed with explicit region regardless of environment
        assertThat(fs).isNotNull();
        assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
    }

    @Test
    void testRegionAutodiscoveryWithoutExplicitConfig() throws Exception {
        // This test verifies that region autodiscovery works when no explicit region is set.
        // The test will either succeed (if AWS region can be auto-detected from environment)
        // or fail with a helpful error message.
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        // Intentionally not setting s3.region to test autodiscovery
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");

        try {
            FileSystem fs = factory.create(fsUri);
            assertThat(fs).isNotNull();
            assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
        } catch (IllegalArgumentException e) {
            // If no region can be auto-detected, verify the error message is helpful
            assertThat(e.getMessage()).contains("AWS region could not be determined");
            assertThat(e.getMessage()).contains("s3.region");
            assertThat(e.getMessage()).contains("AWS_REGION");
        }
    }

    @Test
    void testRegionAutodiscoveryWithCustomEndpoint() throws Exception {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.endpoint", "http://localhost:9000");
        // Intentionally not setting s3.region with custom endpoint
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");

        try {
            FileSystem fs = factory.create(fsUri);
            assertThat(fs).isNotNull();
            assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("AWS region could not be determined");
            assertThat(e.getMessage()).contains("AWS_REGION");
            assertThat(e.getMessage()).contains("~/.aws/config");
        }
    }

    @Test
    void testEmptyRegionFallsBackToAutodiscovery() throws Exception {
        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", ""); // Empty region should trigger autodiscovery
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3://test-bucket/");

        try {
            FileSystem fs = factory.create(fsUri);
            assertThat(fs).isNotNull();
            assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
        } catch (IllegalArgumentException e) {
            // If no region can be auto-detected, verify the error message is helpful
            assertThat(e.getMessage()).contains("AWS region could not be determined");
        }
    }

    @Test
    void testS3ASchemeReturnsS3A() {
        NativeS3AFileSystemFactory factory = new NativeS3AFileSystemFactory();
        assertThat(factory.getScheme()).isEqualTo("s3a");
    }

    @Test
    void testS3ACreateFileSystemWithMinimalConfiguration() throws Exception {
        NativeS3AFileSystemFactory factory = new NativeS3AFileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "us-east-1");
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3a://test-bucket/");
        FileSystem fs = factory.create(fsUri);

        assertThat(fs).isNotNull();
        assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
    }

    @Test
    void testS3ACreateFileSystemWithCustomEndpoint() throws Exception {
        NativeS3AFileSystemFactory factory = new NativeS3AFileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.endpoint", "http://localhost:9000");
        config.setString("s3.region", "us-east-1");
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3a://test-bucket/path/to/file");
        FileSystem fs = factory.create(fsUri);

        assertThat(fs).isNotNull();
        assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
    }

    @Test
    void testS3AInheritsAllS3Configuration() throws Exception {
        NativeS3AFileSystemFactory factory = new NativeS3AFileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "eu-west-1");
        config.setString("s3.entropy.key", "__ENTROPY__");
        config.set(NativeS3FileSystemFactory.ENTROPY_INJECT_LENGTH_OPTION, 6);
        config.set(NativeS3FileSystemFactory.BULK_COPY_ENABLED, true);
        config.set(NativeS3FileSystemFactory.USE_ASYNC_OPERATIONS, true);
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3a://test-bucket/");
        FileSystem fs = factory.create(fsUri);

        assertThat(fs).isNotNull();
        assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
    }

    @Test
    void testS3AWithSSEConfiguration() throws Exception {
        NativeS3AFileSystemFactory factory = new NativeS3AFileSystemFactory();
        Configuration config = new Configuration();
        config.setString("s3.access-key", "test-access-key");
        config.setString("s3.secret-key", "test-secret-key");
        config.setString("s3.region", "us-east-1");
        config.setString("s3.sse.type", "sse-s3");
        config.setString("io.tmp.dirs", System.getProperty("java.io.tmpdir"));

        factory.configure(config);

        URI fsUri = URI.create("s3a://test-bucket/");
        FileSystem fs = factory.create(fsUri);

        assertThat(fs).isNotNull();
        assertThat(fs).isInstanceOf(NativeS3FileSystem.class);
    }
}
