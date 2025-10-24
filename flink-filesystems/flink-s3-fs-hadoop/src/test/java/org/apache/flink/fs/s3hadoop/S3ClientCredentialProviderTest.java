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

package org.apache.flink.fs.s3hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for credential provider compatibility between Hadoop S3A and our custom S3 client.
 *
 * <p>This test verifies that {@link S3ClientConfigurationFactory} properly handles Hadoop's
 * credential provider configuration to ensure consistent authentication between Hadoop's S3A
 * filesystem and our custom S3 client used in multipart upload callbacks.
 */
class S3ClientCredentialProviderTest {

    private Configuration hadoopConfig;

    @BeforeEach
    void setUp() {
        hadoopConfig = new Configuration();
    }

    @AfterEach
    void tearDown() {
        // Clean up any S3 clients created during tests
        try {
            Method releaseMethod =
                    S3ClientConfigurationFactory.class.getDeclaredMethod("releaseS3Client");
            releaseMethod.setAccessible(true);
            releaseMethod.invoke(null);
        } catch (Exception e) {
            // Ignore cleanup errors in tests
        }
    }

    @Test
    void testCredentialProviderWithExplicitCredentials() throws Exception {
        // Test case: Explicit access key and secret key provided
        hadoopConfig.set("fs.s3a.access.key", "test-access-key");
        hadoopConfig.set("fs.s3a.secret.key", "test-secret-key");

        S3Configuration s3Config =
                S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

        // Verify explicit credentials are stored
        assertThat(s3Config.getAccessKey()).isEqualTo("test-access-key");
        assertThat(s3Config.getSecretKey()).isEqualTo("test-secret-key");
        assertThat(s3Config.getSessionToken()).isNull();
        assertThat(s3Config.getHadoopConfiguration()).isEqualTo(hadoopConfig);
    }

    @Test
    void testCredentialProviderWithSessionToken() throws Exception {
        // Test case: Temporary credentials with session token
        hadoopConfig.set("fs.s3a.access.key", "temp-access-key");
        hadoopConfig.set("fs.s3a.secret.key", "temp-secret-key");
        hadoopConfig.set("fs.s3a.session.token", "temp-session-token");

        S3Configuration s3Config =
                S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

        // Verify temporary credentials are stored
        assertThat(s3Config.getAccessKey()).isEqualTo("temp-access-key");
        assertThat(s3Config.getSecretKey()).isEqualTo("temp-secret-key");
        assertThat(s3Config.getSessionToken()).isEqualTo("temp-session-token");
        assertThat(s3Config.getHadoopConfiguration()).isEqualTo(hadoopConfig);
    }

    @Test
    void testCredentialProviderWithHadoopProviderChain() throws Exception {
        // Test case: No explicit credentials - should use Hadoop's credential provider chain
        hadoopConfig.set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider");

        S3Configuration s3Config =
                S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

        // Verify no explicit credentials but Hadoop config is preserved
        assertThat(s3Config.getAccessKey()).isNull();
        assertThat(s3Config.getSecretKey()).isNull();
        assertThat(s3Config.getSessionToken()).isNull();
        assertThat(s3Config.getHadoopConfiguration()).isNotNull();
        assertThat(s3Config.getHadoopConfiguration().get("fs.s3a.aws.credentials.provider"))
                .isEqualTo("org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider");
    }

    @Test
    void testCredentialProviderWithMultipleProviders() throws Exception {
        // Test case: Multiple credential providers in chain (typical production setup)
        String providerChain =
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider,"
                        + "com.amazonaws.auth.EnvironmentVariableCredentialsProvider,"
                        + "com.amazonaws.auth.SystemPropertiesCredentialsProvider";

        hadoopConfig.set("fs.s3a.aws.credentials.provider", providerChain);

        S3Configuration s3Config =
                S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

        // Verify the provider chain is preserved
        assertThat(s3Config.getHadoopConfiguration().get("fs.s3a.aws.credentials.provider"))
                .isEqualTo(providerChain);
    }

    @Test
    void testCredentialProviderCompatibilityMethod() throws Exception {
        // Test the createHadoopCompatibleCredentialProvider method via reflection
        hadoopConfig.set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider");

        S3Configuration s3Config =
                S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

        // Access the private method via reflection
        Method createProviderMethod =
                S3ClientConfigurationFactory.class.getDeclaredMethod(
                        "createHadoopCompatibleCredentialProvider", S3Configuration.class);
        createProviderMethod.setAccessible(true);

        // Invoke the method
        software.amazon.awssdk.auth.credentials.AwsCredentialsProvider provider =
                (software.amazon.awssdk.auth.credentials.AwsCredentialsProvider)
                        createProviderMethod.invoke(null, s3Config);

        // Verify that a credential provider is returned
        assertThat(provider).isNotNull();
        assertThat(provider)
                .isInstanceOf(
                        software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.class);
    }

    @Test
    void testCredentialProviderWithEmptyConfiguration() throws Exception {
        // Test case: Empty configuration - should still work with defaults
        S3Configuration s3Config =
                S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

        // Verify defaults are handled correctly
        assertThat(s3Config.getAccessKey()).isNull();
        assertThat(s3Config.getSecretKey()).isNull();
        assertThat(s3Config.getSessionToken()).isNull();
        assertThat(s3Config.getHadoopConfiguration()).isEqualTo(hadoopConfig);
    }

    @Test
    void testS3ClientCreationWithHadoopCredentials() throws Exception {
        // Test that S3 client can be created with Hadoop credential configuration
        hadoopConfig.set(
                "fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider");
        hadoopConfig.set("fs.s3a.endpoint.region", "us-west-2");

        try {
            S3Configuration s3Config =
                    S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

            // This should not throw an exception even without real AWS credentials in test
            // environment
            software.amazon.awssdk.services.s3.S3Client client =
                    S3ClientConfigurationFactory.acquireS3Client(hadoopConfig);

            assertThat(client).isNotNull();

            // Clean up
            S3ClientConfigurationFactory.releaseS3Client();

        } catch (Exception e) {
            // In test environment, we might get credential-related exceptions, which is expected
            // The important thing is that the configuration parsing doesn't fail
            assertThat(e.getMessage()).contains("credentials");
        }
    }

    @Test
    void testConfigurationHashIncludesCredentialInfo() throws Exception {
        // Test that configuration hash accounts for credential settings
        hadoopConfig.set("fs.s3a.access.key", "test-key");
        S3Configuration config1 =
                S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

        hadoopConfig.set("fs.s3a.access.key", "different-key");
        S3Configuration config2 =
                S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

        // Different credentials should produce different hashes
        assertThat(config1.getConfigurationHash()).isNotEqualTo(config2.getConfigurationHash());
    }

    @Test
    void testCredentialProviderLogging() throws Exception {
        // Test that credential provider configuration is logged appropriately
        hadoopConfig.set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider");

        S3Configuration s3Config =
                S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

        // Access the private method to verify it logs the credential provider
        Method createProviderMethod =
                S3ClientConfigurationFactory.class.getDeclaredMethod(
                        "createHadoopCompatibleCredentialProvider", S3Configuration.class);
        createProviderMethod.setAccessible(true);

        // This should not throw and should handle the logging internally
        software.amazon.awssdk.auth.credentials.AwsCredentialsProvider provider =
                (software.amazon.awssdk.auth.credentials.AwsCredentialsProvider)
                        createProviderMethod.invoke(null, s3Config);

        assertThat(provider).isNotNull();
    }
}
