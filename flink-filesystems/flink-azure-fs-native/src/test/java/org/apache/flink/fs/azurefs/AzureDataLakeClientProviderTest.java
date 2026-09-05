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

package org.apache.flink.fs.azurefs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.time.Duration;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AzureDataLakeClientProvider}. */
class AzureDataLakeClientProviderTest {

    private static final String FAKE_ACCOUNT = "myaccount";
    private static final String EXPECTED_ACCOUNT_URL =
            "https://" + FAKE_ACCOUNT + ".dfs.core.windows.net";
    // Base64-encoded fake key (Azure SDK requires valid base64 for SharedKey)
    private static final String FAKE_KEY = "dGVzdGtleWZvcmF6dXJlc3RvcmFnZWFjY291bnQ=";

    // --- Account name extraction from URI ---

    static Stream<Arguments> validAbfssUris() {
        return Stream.of(
                Arguments.of(
                        "abfss://container@mystorageaccount.dfs.core.windows.net/path",
                        "mystorageaccount",
                        "ABFSS URI (Azure Data Lake Gen2)"),
                Arguments.of(
                        "abfss://container@govaccount.dfs.core.usgovcloudapi.net/path",
                        "govaccount",
                        "Azure Government cloud"),
                Arguments.of(
                        "abfss://container@chinaaccount.dfs.core.chinacloudapi.cn/path",
                        "chinaaccount",
                        "Azure China cloud"));
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("validAbfssUris")
    void shouldExtractAccountNameFromUri(
            final String uriString, final String expectedAccount, final String description)
            throws Exception {
        final URI uri = new URI(uriString);
        final String accountName = AzureDataLakeClientProvider.extractAccountNameFromUri(uri);
        assertThat(accountName).isEqualTo(expectedAccount);
    }

    @Test
    void shouldThrowForNullUri() {
        assertThatThrownBy(() -> AzureDataLakeClientProvider.extractAccountNameFromUri(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowForHttpsScheme() throws Exception {
        final URI uri = new URI("https://devaccount.blob.core.windows.net/container/blob.txt");
        assertThatThrownBy(() -> AzureDataLakeClientProvider.extractAccountNameFromUri(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected abfss scheme");
    }

    @Test
    void shouldThrowForUriWithoutHost() throws Exception {
        final URI uri = new URI("abfss:///path/to/file");
        assertThatThrownBy(() -> AzureDataLakeClientProvider.extractAccountNameFromUri(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host must not be null");
    }

    @Test
    void shouldThrowForHostWithoutDots() throws Exception {
        final URI uri = new URI("abfss://container@storageaccount/path");
        assertThatThrownBy(() -> AzureDataLakeClientProvider.extractAccountNameFromUri(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain a dot");
    }

    // --- Validation / negative tests ---

    @Test
    void shouldThrowWhenNoCredentialsConfigured() {
        assertThatThrownBy(() -> AzureDataLakeClientProvider.builder().build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("credentials not configured");
    }

    // --- Successful construction tests ---

    @Test
    void shouldBuildWithSharedKey() {
        final AzureDataLakeClientProvider provider =
                AzureDataLakeClientProvider.builder()
                        .accountName(FAKE_ACCOUNT)
                        .accountKey(FAKE_KEY)
                        .build();

        assertThat(provider.getAccountUrl()).isEqualTo(EXPECTED_ACCOUNT_URL);
    }

    @Test
    void shouldBuildWithDefaultCredentialWhenOnlyAccountName() {
        final AzureDataLakeClientProvider provider =
                AzureDataLakeClientProvider.builder().accountName(FAKE_ACCOUNT).build();

        assertThat(provider.getAccountUrl()).isEqualTo(EXPECTED_ACCOUNT_URL);
    }

    // --- getFileSystemClient tests ---

    @Test
    void shouldReturnFileSystemClientForValidContainer() {
        final AzureDataLakeClientProvider provider =
                AzureDataLakeClientProvider.builder()
                        .accountName(FAKE_ACCOUNT)
                        .accountKey(FAKE_KEY)
                        .build();

        assertThat(provider.getFileSystemClient("mycontainer").getFileSystemName())
                .isEqualTo("mycontainer");
    }

    // --- Retry configuration tests ---

    @Test
    void shouldRejectNegativeMaxRetries() {
        assertThatThrownBy(() -> AzureDataLakeClientProvider.builder().maxRetries(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }

    @Test
    void shouldRejectNonPositiveBaseDelay() {
        assertThatThrownBy(() -> AzureDataLakeClientProvider.builder().baseDelay(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void shouldRejectNonPositiveMaxDelay() {
        assertThatThrownBy(() -> AzureDataLakeClientProvider.builder().maxDelay(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void shouldRejectBaseDelayExceedingMaxDelay() {
        assertThatThrownBy(
                        () ->
                                AzureDataLakeClientProvider.builder()
                                        .accountName(FAKE_ACCOUNT)
                                        .accountKey(FAKE_KEY)
                                        .baseDelay(Duration.ofSeconds(5))
                                        .maxDelay(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("baseDelay")
                .hasMessageContaining("must not exceed")
                .hasMessageContaining("maxDelay");
    }

    // --- Timeout configuration tests ---

    @Test
    void shouldRejectNonPositiveConnectionTimeout() {
        assertThatThrownBy(
                        () ->
                                AzureDataLakeClientProvider.builder()
                                        .connectionTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void shouldRejectNonPositiveReadTimeout() {
        assertThatThrownBy(() -> AzureDataLakeClientProvider.builder().readTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    // --- HTTP connection pool configuration tests ---

    @Test
    void shouldRejectNegativeMaxIdleConnections() {
        assertThatThrownBy(() -> AzureDataLakeClientProvider.builder().maxIdleConnections(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }

    @Test
    void shouldRejectNonPositiveKeepAliveDuration() {
        assertThatThrownBy(
                        () ->
                                AzureDataLakeClientProvider.builder()
                                        .keepAliveDuration(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void shouldRejectNegativeKeepAliveDuration() {
        assertThatThrownBy(
                        () ->
                                AzureDataLakeClientProvider.builder()
                                        .keepAliveDuration(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }
}
