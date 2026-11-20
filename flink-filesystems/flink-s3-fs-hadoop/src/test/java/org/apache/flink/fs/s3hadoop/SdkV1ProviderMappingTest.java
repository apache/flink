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
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER_MAPPING;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the AWS SDK v1 credentials provider mapping of {@link S3FileSystemFactory}. */
class SdkV1ProviderMappingTest {

    private static final URI BUCKET = URI.create("s3a://bucket/");

    static Stream<Map.Entry<String, String>> mappedProviders() {
        return S3FileSystemFactory.SDK_V1_PROVIDER_MAPPING.entrySet().stream();
    }

    @ParameterizedTest
    @MethodSource("mappedProviders")
    void sdkV1ProviderShouldResolveToSdkV2Provider(Map.Entry<String, String> mapping)
            throws Exception {
        final Configuration conf = new Configuration(false);
        conf.set(AWS_CREDENTIALS_PROVIDER, mapping.getKey());
        S3FileSystemFactory.addSdkV1ProviderMapping(conf);

        final AWSCredentialProviderList providers =
                CredentialProviderListFactory.createAWSCredentialProviderList(BUCKET, conf);

        assertThat(providers.getProviders())
                .singleElement()
                .extracting(provider -> provider.getClass().getName())
                .isEqualTo(mapping.getValue());
    }

    @Test
    void newlineSeparatedUserMappingShouldTakePrecedence() throws Exception {
        final Configuration conf = new Configuration(false);
        conf.set(AWS_CREDENTIALS_PROVIDER, "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        conf.set(
                AWS_CREDENTIALS_PROVIDER_MAPPING,
                "com.example.Other=software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider\n"
                        + "com.amazonaws.auth.DefaultAWSCredentialsProviderChain="
                        + "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider");

        S3FileSystemFactory.addSdkV1ProviderMapping(conf);

        assertThat(
                        CredentialProviderListFactory.createAWSCredentialProviderList(BUCKET, conf)
                                .getProviders())
                .singleElement()
                .extracting(provider -> provider.getClass().getName())
                .isEqualTo(
                        "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider");
    }

    @Test
    void userMappingShouldTakePrecedence() {
        final Configuration conf = new Configuration(false);
        conf.set(
                AWS_CREDENTIALS_PROVIDER_MAPPING,
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain=com.example.Custom");

        S3FileSystemFactory.addSdkV1ProviderMapping(conf);

        final String mapping = conf.get(AWS_CREDENTIALS_PROVIDER_MAPPING);
        assertThat(mapping)
                .startsWith(
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain=com.example.Custom,")
                .containsOnlyOnce("com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                .contains(
                        "com.amazonaws.auth.WebIdentityTokenCredentialsProvider="
                                + "software.amazon.awssdk.auth.credentials."
                                + "WebIdentityTokenFileCredentialsProvider");
    }
}
