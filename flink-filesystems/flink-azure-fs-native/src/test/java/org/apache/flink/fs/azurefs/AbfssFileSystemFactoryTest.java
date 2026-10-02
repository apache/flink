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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.fs.cse.CseOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AbfssFileSystemFactory}. */
class AbfssFileSystemFactoryTest {

    private static final String FAKE_ACCOUNT_KEY = "dGVzdGtleWZvcmF6dXJlc3RvcmFnZWFjY291bnQ=";
    private static final URI TEST_URI =
            URI.create("abfss://container@myaccount.dfs.core.windows.net/");

    private AbfssFileSystemFactory factory;

    @BeforeEach
    void setUp() {
        factory = new AbfssFileSystemFactory();
    }

    @Test
    void shouldReturnAbfssScheme() {
        assertThat(factory.getScheme()).isEqualTo("abfss");
    }

    @Test
    void shouldReturnNegativePriority() {
        assertThat(factory.getPriority()).isEqualTo(-1);
    }

    /** Verifies factory wiring, not actual Azure connectivity (SDK defers authentication). */
    @ParameterizedTest
    @MethodSource("validConfigurations")
    void shouldCreateFileSystemWithValidConfiguration(@Nullable final Configuration config)
            throws Exception {
        if (config != null) {
            factory.configure(config);
        }
        final AzureDataLakeFileSystem fs = (AzureDataLakeFileSystem) factory.create(TEST_URI);
        assertThat(fs).isNotNull();
        assertThat(fs.getUri()).isEqualTo(TEST_URI);
    }

    private static Stream<Arguments> validConfigurations() {
        final Configuration sharedKeyAuth = new Configuration();
        sharedKeyAuth.set(AzureFileSystemOptions.ACCOUNT_KEY, FAKE_ACCOUNT_KEY);

        final Configuration defaultAzureCredential = new Configuration();
        // No account key — should use DefaultAzureCredential

        final Configuration customRetry = new Configuration();
        customRetry.set(AzureFileSystemOptions.ACCOUNT_KEY, FAKE_ACCOUNT_KEY);
        customRetry.set(AzureFileSystemOptions.RETRY_MAX_RETRIES, 10);
        customRetry.set(AzureFileSystemOptions.RETRY_BASE_DELAY, Duration.ofMillis(200));
        customRetry.set(AzureFileSystemOptions.RETRY_MAX_DELAY, Duration.ofSeconds(30));

        final Configuration customHttpTimeouts = new Configuration();
        customHttpTimeouts.set(AzureFileSystemOptions.ACCOUNT_KEY, FAKE_ACCOUNT_KEY);
        customHttpTimeouts.set(AzureFileSystemOptions.CONNECTION_TIMEOUT, Duration.ofSeconds(120));
        customHttpTimeouts.set(AzureFileSystemOptions.READ_TIMEOUT, Duration.ofSeconds(120));

        final Configuration customReadBuffer = new Configuration();
        customReadBuffer.set(AzureFileSystemOptions.ACCOUNT_KEY, FAKE_ACCOUNT_KEY);
        customReadBuffer.set(AzureFileSystemOptions.READ_BUFFER_SIZE, new MemorySize(512 * 1024));

        return Stream.of(
                Arguments.of(sharedKeyAuth), // shared key auth
                Arguments.of(defaultAzureCredential), // default Azure credential
                Arguments.of((Configuration) null), // unconfigured defaults
                Arguments.of(customRetry), // custom retry settings
                Arguments.of(customHttpTimeouts), // custom HTTP timeouts
                Arguments.of(customReadBuffer)); // custom read buffer size
    }

    @Test
    void shouldThrowWhenReadBufferSizeIsZero() {
        final Configuration config = new Configuration();
        config.set(AzureFileSystemOptions.READ_BUFFER_SIZE, new MemorySize(0));

        factory.configure(config);

        assertThatThrownBy(() -> factory.create(TEST_URI))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Read buffer size must be positive");
    }

    @Test
    void shouldThrowWhenWriteRequestSizeIsZero() {
        final Configuration config = new Configuration();
        config.set(AzureFileSystemOptions.WRITE_REQUEST_SIZE, new MemorySize(0));

        factory.configure(config);

        assertThatThrownBy(() -> factory.create(TEST_URI))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Write request size must be positive");
    }

    @Test
    void shouldThrowWhenKeyIdSetWithoutKeyProviderFactoryClass() {
        final Configuration config = new Configuration();
        config.setString(CseOptions.WRITE_KEY_ID.key(), "test-key-id");
        factory.configure(config);

        assertThatThrownBy(() -> factory.create(TEST_URI))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CseOptions.WRITE_KEY_ID.key())
                .hasMessageContaining(CseOptions.KEY_PROVIDER_FACTORY_CLASS.key());
    }

    /** Verifies that an invalid key-provider-factory-class yields an {@link IOException}. */
    @ParameterizedTest
    @MethodSource("invalidFactoryClasses")
    void shouldThrowWhenKeyProviderFactoryClassInvalid(final String factoryClass) {
        final Configuration config = new Configuration();
        config.setString(CseOptions.KEY_PROVIDER_FACTORY_CLASS.key(), factoryClass);
        config.setString(CseOptions.WRITE_KEY_ID.key(), "test-key");
        factory.configure(config);

        assertThatThrownBy(() -> factory.create(TEST_URI))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to instantiate KeyProviderFactory class")
                .hasMessageContaining(factoryClass);
    }

    private static Stream<Arguments> invalidFactoryClasses() {
        return Stream.of(
                // class not on classpath
                Arguments.of("com.example.NonExistent"),
                // class exists but is not a KeyProviderFactory
                Arguments.of("java.lang.String"));
    }
}
