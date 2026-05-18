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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that user configuration intents are correctly mapped into the credentials chain. */
class S3ClientProviderTest {

    private static final String DUMMY_ENDPOINT = "http://localhost:9000";
    private static final String DUMMY_REGION = "us-east-1";

    private final List<S3ClientProvider> providers = new ArrayList<>();

    /** Tracks a provider so it is closed after the test, releasing its SDK/CRT resources. */
    private S3ClientProvider track(S3ClientProvider provider) {
        providers.add(provider);
        return provider;
    }

    @AfterEach
    void closeProviders() {
        for (S3ClientProvider provider : providers) {
            try {
                provider.closeAsync().get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
        providers.clear();
    }

    @Test
    void testMinimalChainWithoutStaticOrCustom() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder().endpoint(DUMMY_ENDPOINT).region(DUMMY_REGION).build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain).hasSize(2);
        assertThat(chain.get(0)).isInstanceOf(DynamicTemporaryAWSCredentialsProvider.class);
        assertThat(chain.get(1)).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void testChainWithStaticCredentials() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .accessKey("test-key")
                        .secretKey("test-secret")
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain).hasSize(3);
        AwsCredentials creds = chain.get(0).resolveCredentials();
        assertThat(creds).isInstanceOf(AwsBasicCredentials.class);
        assertThat(creds.accessKeyId()).isEqualTo("test-key");
        assertThat(creds.secretAccessKey()).isEqualTo("test-secret");
        assertThat(chain.get(1)).isInstanceOf(DynamicTemporaryAWSCredentialsProvider.class);
        assertThat(chain.get(2)).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void testCustomProviderPrependedToChain() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .credentialsProviderClasses("AnonymousCredentialsProvider")
                        .build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain).hasSize(3);
        assertThat(chain.get(0)).isInstanceOf(AnonymousCredentialsProvider.class);
        assertThat(chain.get(1)).isInstanceOf(DynamicTemporaryAWSCredentialsProvider.class);
        assertThat(chain.get(2)).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void testAllFourTiersActive() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .accessKey("my-key")
                        .secretKey("my-secret")
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .credentialsProviderClasses("AnonymousCredentialsProvider")
                        .build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain).hasSize(4);
        assertThat(chain.get(0)).isInstanceOf(AnonymousCredentialsProvider.class);
        AwsCredentials creds = chain.get(1).resolveCredentials();
        assertThat(creds.accessKeyId()).isEqualTo("my-key");
        assertThat(chain.get(2)).isInstanceOf(DynamicTemporaryAWSCredentialsProvider.class);
        assertThat(chain.get(3)).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void testCustomProviderChainWithMultipleEntries() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .credentialsProviderClasses(
                                "AnonymousCredentialsProvider,EnvironmentVariableCredentialsProvider")
                        .build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain).hasSize(4);
        assertThat(chain.get(0)).isInstanceOf(AnonymousCredentialsProvider.class);
        assertThat(chain.get(1).getClass().getSimpleName())
                .isEqualTo("EnvironmentVariableCredentialsProvider");
        assertThat(chain.get(2)).isInstanceOf(DynamicTemporaryAWSCredentialsProvider.class);
        assertThat(chain.get(3)).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void testAssumeRoleWrapsChain() {
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
    void testTokenProviderAlwaysPresentWithCustomConfig() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .credentialsProviderClasses("AnonymousCredentialsProvider")
                        .build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain).anyMatch(p -> p instanceof DynamicTemporaryAWSCredentialsProvider);
    }

    @Test
    void testDefaultProviderAlwaysTail() throws Exception {
        S3ClientProvider provider =
                S3ClientProvider.builder()
                        .accessKey("key")
                        .secretKey("secret")
                        .endpoint(DUMMY_ENDPOINT)
                        .region(DUMMY_REGION)
                        .credentialsProviderClasses("AnonymousCredentialsProvider")
                        .build();

        List<AwsCredentialsProvider> chain = extractChain(provider.getCredentialsProvider());

        assertThat(chain.get(chain.size() - 1)).isInstanceOf(DefaultCredentialsProvider.class);
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

    @Test
    void testRetryBuilderDefaultsMatchConfigOptions() {
        S3ClientProvider provider =
                S3ClientProvider.builder().endpoint(DUMMY_ENDPOINT).region(DUMMY_REGION).build();

        assertThat(provider.getRetryBaseDelay())
                .isEqualTo(NativeS3FileSystemFactory.RETRY_BASE_DELAY.defaultValue());
        assertThat(provider.getRetryThrottleBaseDelay())
                .isEqualTo(NativeS3FileSystemFactory.RETRY_THROTTLE_BASE_DELAY.defaultValue());
        assertThat(provider.getRetryMaxBackoff())
                .isEqualTo(NativeS3FileSystemFactory.RETRY_MAX_BACKOFF.defaultValue());
    }

    @Test
    void testNegativeRetryBaseDelayThrows() {
        assertThatThrownBy(() -> S3ClientProvider.builder().retryBaseDelay(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryBaseDelay must not be negative");
    }

    @Test
    void testNegativeRetryThrottleBaseDelayThrows() {
        assertThatThrownBy(
                        () ->
                                S3ClientProvider.builder()
                                        .retryThrottleBaseDelay(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryThrottleBaseDelay must not be negative");
    }

    @Test
    void testZeroRetryMaxBackoffThrows() {
        assertThatThrownBy(() -> S3ClientProvider.builder().retryMaxBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxBackoff must be positive");
    }

    @Test
    void testRetryMaxBackoffSmallerThanBaseDelayThrows() {
        assertThatThrownBy(
                        () ->
                                S3ClientProvider.builder()
                                        .endpoint(DUMMY_ENDPOINT)
                                        .region(DUMMY_REGION)
                                        .retryBaseDelay(Duration.ofSeconds(5))
                                        .retryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxBackoff")
                .hasMessageContaining("retryBaseDelay");
    }

    @Test
    void testRetryMaxBackoffSmallerThanThrottleBaseDelayThrows() {
        assertThatThrownBy(
                        () ->
                                S3ClientProvider.builder()
                                        .endpoint(DUMMY_ENDPOINT)
                                        .region(DUMMY_REGION)
                                        .retryThrottleBaseDelay(Duration.ofSeconds(5))
                                        .retryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxBackoff")
                .hasMessageContaining("retryThrottleBaseDelay");
    }

    @Test
    void testCrtDisabledByDefault() {
        S3ClientProvider provider =
                track(
                        S3ClientProvider.builder()
                                .endpoint(DUMMY_ENDPOINT)
                                .region(DUMMY_REGION)
                                .build());
        assertThat(provider.isUseCrt()).isFalse();
        // When CRT is disabled the async client must NOT be a CRT-backed implementation.
        assertThat(provider.getAsyncClient().getClass().getName()).doesNotContain("Crt");
        // No Flink-level default applied; getter returns null when user did not set the value.
        assertThat(provider.getCrtTargetThroughputGbps()).isNull();
        assertThat(provider.getCrtReadBufferSizeInBytes()).isNull();
        assertThat(provider.getCrtMaxNativeMemoryLimitInBytes()).isNull();
        // CRT max concurrency keeps its builder default (independent of s3.connection.max).
        assertThat(provider.getCrtMaxConcurrency())
                .isEqualTo(NativeS3FileSystemFactory.CRT_MAX_CONCURRENCY.defaultValue());
    }

    @Test
    void testCrtFlagIsRecordedAndCrtBranchIsTaken() {
        S3ClientProvider provider =
                track(
                        S3ClientProvider.builder()
                                .endpoint(DUMMY_ENDPOINT)
                                .region(DUMMY_REGION)
                                .useCrt(true)
                                .crtTargetThroughputGbps(20.0)
                                .build());

        assertThat(provider.isUseCrt()).isTrue();
        assertThat(provider.getCrtTargetThroughputGbps()).isEqualTo(20.0);
        assertThat(provider.getAsyncClient().getClass().getName()).contains("Crt");
    }

    @Test
    void testCrtEnabledWithoutThroughputOverrideStillBuildsCrtClient() {
        S3ClientProvider provider =
                track(
                        S3ClientProvider.builder()
                                .endpoint(DUMMY_ENDPOINT)
                                .region(DUMMY_REGION)
                                .useCrt(true)
                                .build());

        assertThat(provider.isUseCrt()).isTrue();
        assertThat(provider.getCrtTargetThroughputGbps()).isNull();
        assertThat(provider.getAsyncClient().getClass().getName()).contains("Crt");
    }

    @Test
    void testCrtMissingJarsMessageIsActionable() {
        // Contract test: if CRT classes are missing at runtime the user must get a message
        // that names the responsible config key, the missing JAR coordinates, and a setup
        // pointer. A full classloader-isolation test would require multi-classloader infra
        // disproportionate to the value; assert the message contract instead.
        String msg = S3ClientProvider.Builder.crtMissingJarsMessage();
        assertThat(msg).contains("s3.crt.enabled=true");
        // aws-crt-client is now bundled in the fat JAR; only aws-crt (JNI) is external
        assertThat(msg).doesNotContain("aws-crt-client");
        assertThat(msg).contains("aws-crt");
        assertThat(msg).contains("tools/download-crt-jars.sh");
        assertThat(msg).contains("plugin");
        assertThat(msg).contains("README");
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
