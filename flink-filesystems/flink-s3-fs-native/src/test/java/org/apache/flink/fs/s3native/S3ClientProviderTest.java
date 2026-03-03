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

import org.apache.flink.fs.s3native.token.DynamicTemporaryAWSCredentialsProvider;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.lang.reflect.Field;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that user configuration intents are correctly mapped into the credentials chain. */
class S3ClientProviderTest {

    private static final String DUMMY_ENDPOINT = "http://localhost:9000";
    private static final String DUMMY_REGION = "us-east-1";

    @Test
    void testDefaultChainIncludesTokenAndFallback() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .accessKey("test-key")
                        .secretKey("test-secret")
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain).hasSize(2);
        assertThat(chain.get(0)).isInstanceOf(DynamicTemporaryAWSCredentialsProvider.class);
        AwsCredentials creds = chain.get(1).resolveCredentials();
        assertThat(creds.accessKeyId()).isEqualTo("test-key");
        assertThat(creds.secretAccessKey()).isEqualTo("test-secret");
    }

    @Test
    void testAssumeRoleWrapsBaseChain() {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .accessKey("test-key")
                        .secretKey("test-secret")
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .assumeRoleArn("arn:aws:iam::123456789012:role/TestRole")
                        .build();

        assertThat(provider.getCredentialsProvider())
                .isInstanceOf(StsAssumeRoleCredentialsProvider.class);
    }

    @Test
    void testStaticCredentialsInDefaultChain() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .accessKey("my-access")
                        .secretKey("my-secret")
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain).hasSize(2);
        AwsCredentials creds = chain.get(1).resolveCredentials();
        assertThat(creds).isInstanceOf(AwsBasicCredentials.class);
        assertThat(creds.accessKeyId()).isEqualTo("my-access");
    }

    @Test
    void testCustomProviderReplacesDefaultChain() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .credentialsProviderClasses("AnonymousCredentialsProvider")
                        .build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain).hasSize(1);
        assertThat(chain.get(0)).isInstanceOf(AnonymousCredentialsProvider.class);
    }

    @Test
    void testCustomProviderChainWithMultipleProviders() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .credentialsProviderClasses(
                                "AnonymousCredentialsProvider,DefaultCredentialsProvider")
                        .build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain).hasSize(2);
        assertThat(chain.get(0)).isInstanceOf(AnonymousCredentialsProvider.class);
    }

    @Test
    void testCustomProviderDoesNotIncludeTokenProvider() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .credentialsProviderClasses("AnonymousCredentialsProvider")
                        .build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain).noneMatch(p -> p instanceof DynamicTemporaryAWSCredentialsProvider);
    }

    @Test
    void testInvalidProviderClassThrows() {
        assertThatThrownBy(
                        () ->
                                S3ClientProvider.builder()
                                        .endpoint(DUMMY_ENDPOINT)
                                        .region(DUMMY_REGION)
                                        .credentialsProviderClasses("com.nonexistent.FakeProvider")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to instantiate credentials provider");
    }

    @Test
    void testEmptyProviderStringThrows() {
        assertThatThrownBy(
                        () ->
                                S3ClientProvider.builder()
                                        .endpoint(DUMMY_ENDPOINT)
                                        .region(DUMMY_REGION)
                                        .credentialsProviderClasses("  ,  , ")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no valid provider class names");
    }

    @SuppressWarnings("unchecked")
    private static List<AwsCredentialsProvider> extractChain(AwsCredentialsProvider provider)
            throws Exception {
        assertThat(provider).isInstanceOf(AwsCredentialsProviderChain.class);
        Field field = AwsCredentialsProviderChain.class.getDeclaredField("credentialsProviders");
        field.setAccessible(true);
        return (List<AwsCredentialsProvider>) field.get(provider);
    }
}
