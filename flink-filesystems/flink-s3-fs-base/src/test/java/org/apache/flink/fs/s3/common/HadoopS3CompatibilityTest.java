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

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HadoopS3Compatibility}. */
class HadoopS3CompatibilityTest {

    @Test
    void testAwsSdkV1Detection() {
        // Test that V1 detection works
        boolean v1Present = HadoopS3Compatibility.isAwsSdkV1Present();

        // Since we have V1 as optional/provided, this should return true during testing
        assertThat(v1Present).isTrue();
    }

    @Test
    void testApplyV1Compatibility() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();
        Configuration flinkConfig = new Configuration();

        // Set V2-style duration configurations
        hadoopConfig.set("fs.s3a.connection.timeout", "10s");
        hadoopConfig.set("fs.s3a.socket.timeout", "30s");
        hadoopConfig.set("fs.s3a.request.timeout", "2m");
        hadoopConfig.set("fs.s3a.retry.throttle.limit", "5s");

        // Apply V1 compatibility
        HadoopS3Compatibility.applyV1Compatibility(hadoopConfig, flinkConfig);

        // Verify duration strings are converted to milliseconds
        assertThat(hadoopConfig.get("fs.s3a.connection.timeout")).isEqualTo("10000");
        assertThat(hadoopConfig.get("fs.s3a.socket.timeout")).isEqualTo("30000");
        assertThat(hadoopConfig.get("fs.s3a.request.timeout")).isEqualTo("120000");
        assertThat(hadoopConfig.get("fs.s3a.retry.throttle.limit")).isEqualTo("5000");

        // Verify credential provider mapping is set
        String mapping = hadoopConfig.get("fs.s3a.aws.credentials.provider.mapping");
        assertThat(mapping).isNotNull();
        assertThat(mapping).contains("DynamicTemporaryAWSCredentialsProvider");
        assertThat(mapping).contains("DynamicTemporaryAWSCredentialsProviderV2");
    }

    @Test
    void testDurationConversionWithMilliseconds() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();
        Configuration flinkConfig = new Configuration();

        // Set millisecond configurations (should remain unchanged)
        hadoopConfig.set("fs.s3a.connection.timeout", "10000");
        hadoopConfig.set("fs.s3a.socket.timeout", "30000");

        HadoopS3Compatibility.applyV1Compatibility(hadoopConfig, flinkConfig);

        // Verify millisecond values remain unchanged
        assertThat(hadoopConfig.get("fs.s3a.connection.timeout")).isEqualTo("10000");
        assertThat(hadoopConfig.get("fs.s3a.socket.timeout")).isEqualTo("30000");
    }

    @Test
    void testDurationConversionWithDifferentUnits() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();
        Configuration flinkConfig = new Configuration();

        // Test different time units
        hadoopConfig.set("fs.s3a.connection.timeout", "30s"); // 30 seconds
        hadoopConfig.set("fs.s3a.socket.timeout", "5m"); // 5 minutes
        hadoopConfig.set("fs.s3a.request.timeout", "2h"); // 2 hours
        hadoopConfig.set("fs.s3a.retry.interval", "1d"); // 1 day

        HadoopS3Compatibility.applyV1Compatibility(hadoopConfig, flinkConfig);

        // Verify conversions
        assertThat(hadoopConfig.get("fs.s3a.connection.timeout")).isEqualTo("30000"); // 30 * 1000
        assertThat(hadoopConfig.get("fs.s3a.socket.timeout")).isEqualTo("300000"); // 5 * 60 * 1000
        assertThat(hadoopConfig.get("fs.s3a.request.timeout"))
                .isEqualTo("7200000"); // 2 * 60 * 60 * 1000
        assertThat(hadoopConfig.get("fs.s3a.retry.interval"))
                .isEqualTo("86400000"); // 24 * 60 * 60 * 1000
    }

    @Test
    void testCredentialProviderMappingNotOverridden() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();
        Configuration flinkConfig = new Configuration();

        // Set existing mapping
        String existingMapping = "com.example.Provider=com.example.ProviderV2";
        hadoopConfig.set("fs.s3a.aws.credentials.provider.mapping", existingMapping);

        HadoopS3Compatibility.applyV1Compatibility(hadoopConfig, flinkConfig);

        // Verify existing mapping is preserved
        assertThat(hadoopConfig.get("fs.s3a.aws.credentials.provider.mapping"))
                .isEqualTo(existingMapping);
    }

    @Test
    void testCustomDurationKeys() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();
        Configuration flinkConfig = new Configuration();

        // Override the duration keys list
        List<String> customKeys =
                Arrays.asList("fs.s3a.connection.timeout", "fs.s3a.socket.timeout");
        flinkConfig.set(
                ConfigOptions.key("s3.v1-compat.duration-keys")
                        .stringType()
                        .asList()
                        .defaultValues(),
                customKeys);

        // Set some duration configs (only the custom keys should be converted)
        hadoopConfig.set("fs.s3a.connection.timeout", "10s"); // Should be converted
        hadoopConfig.set("fs.s3a.socket.timeout", "30s"); // Should be converted
        hadoopConfig.set("fs.s3a.request.timeout", "2m"); // Should NOT be converted

        HadoopS3Compatibility.applyV1Compatibility(hadoopConfig, flinkConfig);

        // Verify only custom keys are converted
        assertThat(hadoopConfig.get("fs.s3a.connection.timeout")).isEqualTo("10000");
        assertThat(hadoopConfig.get("fs.s3a.socket.timeout")).isEqualTo("30000");
        assertThat(hadoopConfig.get("fs.s3a.request.timeout")).isEqualTo("2m"); // Unchanged
    }

    @Test
    void testInvalidDurationStringsIgnored() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();
        Configuration flinkConfig = new Configuration();

        // Set invalid duration strings
        hadoopConfig.set("fs.s3a.connection.timeout", "invalid");
        hadoopConfig.set("fs.s3a.socket.timeout", "10x"); // Invalid unit
        hadoopConfig.set("fs.s3a.request.timeout", "s"); // Missing number
        hadoopConfig.set("fs.s3a.retry.interval", "10"); // Missing unit

        HadoopS3Compatibility.applyV1Compatibility(hadoopConfig, flinkConfig);

        // Verify invalid strings remain unchanged
        assertThat(hadoopConfig.get("fs.s3a.connection.timeout")).isEqualTo("invalid");
        assertThat(hadoopConfig.get("fs.s3a.socket.timeout")).isEqualTo("10x");
        assertThat(hadoopConfig.get("fs.s3a.request.timeout")).isEqualTo("s");
        assertThat(hadoopConfig.get("fs.s3a.retry.interval")).isEqualTo("10");
    }

    @Test
    void testNullAndEmptyDurationConfigsIgnored() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();
        Configuration flinkConfig = new Configuration();

        // Set null/empty configs - these should be ignored
        hadoopConfig.set("fs.s3a.connection.timeout", "");
        // Don't set fs.s3a.socket.timeout at all (null)

        HadoopS3Compatibility.applyV1Compatibility(hadoopConfig, flinkConfig);

        // Verify null/empty values remain unchanged
        assertThat(hadoopConfig.get("fs.s3a.connection.timeout")).isEqualTo("");
        assertThat(hadoopConfig.get("fs.s3a.socket.timeout")).isNull();
    }
}
