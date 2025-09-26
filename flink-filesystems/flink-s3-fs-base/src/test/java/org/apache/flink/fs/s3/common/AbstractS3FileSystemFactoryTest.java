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

package org.apache.flink.fs.s3.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.fs.s3.common.token.AbstractS3DelegationTokenReceiver;
import org.apache.flink.fs.s3.common.writer.S3AccessHelper;
import org.apache.flink.runtime.util.HadoopConfigLoader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AbstractS3FileSystemFactory} V1/V2 compatibility. */
class AbstractS3FileSystemFactoryTest {

    private TestS3FileSystemFactory factory;
    private org.apache.hadoop.conf.Configuration testHadoopConfig;

    @BeforeEach
    void setUp() {
        testHadoopConfig = new org.apache.hadoop.conf.Configuration();
        TestHadoopConfigLoader configLoader = new TestHadoopConfigLoader(testHadoopConfig);
        factory = new TestS3FileSystemFactory("test-factory", configLoader);
    }

    @Test
    void testV1CompatibilityAppliedWhenV1Present() throws Exception {
        // Setup Hadoop config with V2-style duration strings
        testHadoopConfig.set("fs.s3a.connection.timeout", "10s");
        testHadoopConfig.set("fs.s3a.socket.timeout", "30s");
        testHadoopConfig.set("fs.s3a.request.timeout", "2m");

        Configuration flinkConfig = new Configuration();
        factory.configure(flinkConfig);

        // Create file system - this should trigger V1 compatibility if V1 is present
        try {
            factory.create(URI.create("s3://test-bucket/"));
        } catch (Exception e) {
            // Expected to fail due to test implementation, but compatibility should have been
            // applied
        }

        // Verify V1 compatibility was applied if V1 SDK is present
        if (HadoopS3Compatibility.isAwsSdkV1Present()) {
            // Duration strings should be converted to milliseconds
            assertThat(testHadoopConfig.get("fs.s3a.connection.timeout")).isEqualTo("10000");
            assertThat(testHadoopConfig.get("fs.s3a.socket.timeout")).isEqualTo("30000");
            assertThat(testHadoopConfig.get("fs.s3a.request.timeout")).isEqualTo("120000");

            // Credential provider mapping should be set
            String mapping = testHadoopConfig.get("fs.s3a.aws.credentials.provider.mapping");
            assertThat(mapping).isNotNull();
            assertThat(mapping).contains("DynamicTemporaryAWSCredentialsProvider");
            assertThat(mapping).contains("DynamicTemporaryAWSCredentialsProviderV2");

            // Credential provider should be configured
            String providers = testHadoopConfig.get("fs.s3a.aws.credentials.provider");
            assertThat(providers).isNotNull();
            assertThat(providers).contains("DynamicTemporaryAWSCredentialsProvider");
        } else {
            // If V1 not present, durations should remain as-is and V2 provider should be used
            assertThat(testHadoopConfig.get("fs.s3a.connection.timeout")).isEqualTo("10s");
            assertThat(testHadoopConfig.get("fs.s3a.socket.timeout")).isEqualTo("30s");
            assertThat(testHadoopConfig.get("fs.s3a.request.timeout")).isEqualTo("2m");

            String providers = testHadoopConfig.get("fs.s3a.aws.credentials.provider");
            assertThat(providers).contains("DynamicTemporaryAWSCredentialsProviderV2");
        }
    }

    @Test
    void testMillisecondConfigurationsPreserved() throws Exception {
        // Setup Hadoop config with millisecond values (should remain unchanged)
        testHadoopConfig.set("fs.s3a.connection.timeout", "15000");
        testHadoopConfig.set("fs.s3a.socket.timeout", "45000");

        Configuration flinkConfig = new Configuration();
        factory.configure(flinkConfig);

        try {
            factory.create(URI.create("s3://test-bucket/"));
        } catch (Exception e) {
            // Expected to fail due to test implementation
        }

        // Millisecond values should remain unchanged regardless of V1/V2
        assertThat(testHadoopConfig.get("fs.s3a.connection.timeout")).isEqualTo("15000");
        assertThat(testHadoopConfig.get("fs.s3a.socket.timeout")).isEqualTo("45000");
    }

    @Test
    void testCredentialProviderSelectionBasedOnV1Availability() throws Exception {
        Configuration flinkConfig = new Configuration();
        factory.configure(flinkConfig);

        try {
            factory.create(URI.create("s3://test-bucket/"));
        } catch (Exception e) {
            // Expected to fail due to test implementation
        }

        // Verify appropriate credential provider is selected
        String providers = testHadoopConfig.get("fs.s3a.aws.credentials.provider");
        assertThat(providers).isNotNull();

        if (HadoopS3Compatibility.isAwsSdkV1Present()) {
            assertThat(providers).contains("DynamicTemporaryAWSCredentialsProvider");
            assertThat(providers).doesNotContain("DynamicTemporaryAWSCredentialsProviderV2");
        } else {
            assertThat(providers).contains("DynamicTemporaryAWSCredentialsProviderV2");
            assertThat(providers).doesNotContain("DynamicTemporaryAWSCredentialsProvider");
        }
    }

    @Test
    void testExistingCredentialProvidersPreserved() throws Exception {
        // Set existing credential providers
        String existingProviders = "com.example.Provider1,com.example.Provider2";
        testHadoopConfig.set("fs.s3a.aws.credentials.provider", existingProviders);

        Configuration flinkConfig = new Configuration();
        factory.configure(flinkConfig);

        try {
            factory.create(URI.create("s3://test-bucket/"));
        } catch (Exception e) {
            // Expected to fail due to test implementation
        }

        // Verify Flink provider is prepended to existing providers
        String providers = testHadoopConfig.get("fs.s3a.aws.credentials.provider");
        assertThat(providers).isNotNull();
        assertThat(providers).endsWith(existingProviders);

        if (HadoopS3Compatibility.isAwsSdkV1Present()) {
            assertThat(providers)
                    .startsWith(
                            "org.apache.flink.fs.s3.common.token.DynamicTemporaryAWSCredentialsProvider");
        } else {
            assertThat(providers)
                    .startsWith(
                            "org.apache.flink.fs.s3.common.token.DynamicTemporaryAWSCredentialsProviderV2");
        }
    }

    @Test
    void testRegionConfigurationWhenProvided() throws Exception {
        // Configure region through delegation token receiver
        TestS3DelegationTokenReceiver receiver = new TestS3DelegationTokenReceiver();
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("security.delegation.token.provider.s3.region", "us-west-2");
        receiver.init(flinkConfig);

        factory.configure(flinkConfig);

        try {
            factory.create(URI.create("s3://test-bucket/"));
        } catch (Exception e) {
            // Expected to fail due to test implementation
        }

        // Verify region is configured
        assertThat(testHadoopConfig.get("fs.s3a.endpoint.region")).isEqualTo("us-west-2");
    }

    @Test
    void testV1V2ProviderMappingConfigured() throws Exception {
        Configuration flinkConfig = new Configuration();
        factory.configure(flinkConfig);

        try {
            factory.create(URI.create("s3://test-bucket/"));
        } catch (Exception e) {
            // Expected to fail due to test implementation
        }

        // If V1 is present, provider mapping should be configured
        if (HadoopS3Compatibility.isAwsSdkV1Present()) {
            String mapping = testHadoopConfig.get("fs.s3a.aws.credentials.provider.mapping");
            assertThat(mapping).isNotNull();
            assertThat(mapping)
                    .isEqualTo(
                            "org.apache.flink.fs.s3.common.token.DynamicTemporaryAWSCredentialsProvider="
                                    + "org.apache.flink.fs.s3.common.token.DynamicTemporaryAWSCredentialsProviderV2");
        }
    }

    /** Test implementation of HadoopConfigLoader for testing purposes. */
    private static class TestHadoopConfigLoader extends HadoopConfigLoader {
        private final org.apache.hadoop.conf.Configuration config;

        TestHadoopConfigLoader(org.apache.hadoop.conf.Configuration config) {
            super(
                    new String[0],
                    new String[0][0],
                    "",
                    java.util.Collections.emptySet(),
                    java.util.Collections.emptySet(),
                    "");
            this.config = config;
        }

        @Override
        public org.apache.hadoop.conf.Configuration getOrLoadHadoopConfig() {
            return config;
        }
    }

    /** Test implementation of AbstractS3FileSystemFactory for testing purposes. */
    private static class TestS3FileSystemFactory extends AbstractS3FileSystemFactory {

        TestS3FileSystemFactory(String name, HadoopConfigLoader hadoopConfigLoader) {
            super(name, hadoopConfigLoader);
        }

        @Override
        public String getScheme() {
            return "s3";
        }

        @Override
        protected org.apache.hadoop.fs.FileSystem createHadoopFileSystem() {
            // Throw exception for testing - we only want to test the configuration phase
            throw new RuntimeException("Test implementation - configuration testing only");
        }

        @Override
        protected URI getInitURI(URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) {
            return fsUri;
        }

        @Override
        protected S3AccessHelper getS3AccessHelper(org.apache.hadoop.fs.FileSystem fs) {
            return null; // Not needed for these tests
        }
    }

    /** Test implementation of AbstractS3DelegationTokenReceiver for testing purposes. */
    private static class TestS3DelegationTokenReceiver extends AbstractS3DelegationTokenReceiver {
        @Override
        public String serviceName() {
            return "s3";
        }
    }
}
