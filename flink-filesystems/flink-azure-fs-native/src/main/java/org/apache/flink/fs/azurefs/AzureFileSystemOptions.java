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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import javax.annotation.concurrent.Immutable;

import java.time.Duration;

/**
 * Configuration options for the native Azure Data Lake Storage Gen2 filesystem.
 *
 * <p>This is the single source of truth for all config keys and defaults used by both {@link
 * AbfssFileSystemFactory} and {@link AzureDataLakeClientProvider}.
 */
@Experimental
@Immutable
public final class AzureFileSystemOptions {

    private AzureFileSystemOptions() {}

    // -------------------------------------------------------------------------
    // Core credentials
    // -------------------------------------------------------------------------

    /** Azure Storage account key for SharedKey authentication. */
    public static final ConfigOption<String> ACCOUNT_KEY =
            ConfigOptions.key("fs.azure.account-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Azure Storage account key");

    // -------------------------------------------------------------------------
    // Retry policy
    // -------------------------------------------------------------------------

    /** Maximum number of retry attempts for transient failures. */
    public static final ConfigOption<Integer> RETRY_MAX_RETRIES =
            ConfigOptions.key("fs.azure.retry.max-retries")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Maximum number of retry attempts for transient failures. "
                                    + "Set to 0 to disable retries.");

    /** Base delay for exponential backoff retry strategy. */
    public static final ConfigOption<Duration> RETRY_BASE_DELAY =
            ConfigOptions.key("fs.azure.retry.base-delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDescription("Base delay for exponential backoff retry strategy.");

    /** Maximum delay between retry attempts. */
    public static final ConfigOption<Duration> RETRY_MAX_DELAY =
            ConfigOptions.key("fs.azure.retry.max-delay")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(20))
                    .withDescription("Maximum delay between retry attempts.");

    // -------------------------------------------------------------------------
    // HTTP timeouts
    // -------------------------------------------------------------------------

    /** Connection timeout for Azure HTTP client. */
    public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
            ConfigOptions.key("fs.azure.http.connection-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("Connection timeout for Azure HTTP client.");

    /** Read timeout for Azure HTTP client. */
    public static final ConfigOption<Duration> READ_TIMEOUT =
            ConfigOptions.key("fs.azure.http.read-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("Read timeout for Azure HTTP client.");

    /** Maximum number of idle connections in the HTTP connection pool. */
    public static final ConfigOption<Integer> HTTP_MAX_IDLE_CONNECTIONS =
            ConfigOptions.key("fs.azure.http.max-idle-connections")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "Maximum number of idle connections in the HTTP connection pool.");

    /** Keep-alive duration for idle HTTP connections. */
    public static final ConfigOption<Duration> HTTP_KEEP_ALIVE_DURATION =
            ConfigOptions.key("fs.azure.http.keep-alive-duration")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Keep-alive duration for idle HTTP connections. "
                                    + "Connections idle longer than this are evicted.");

    // -------------------------------------------------------------------------
    // Write / upload
    // -------------------------------------------------------------------------

    /**
     * Size of each write request (block size). Controls both the block size for multi-block uploads
     * and the maximum single-upload threshold. Files larger than this are uploaded in blocks.
     */
    public static final ConfigOption<MemorySize> WRITE_REQUEST_SIZE =
            ConfigOptions.key("fs.azure.write.request-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(8))
                    .withDescription(
                            "Block size for uploads. Also sets the single-upload threshold: "
                                    + "files larger than this are uploaded in blocks.");

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Read buffer size for input streams.
     *
     * <p>Controls how much data is fetched per HTTP GET range request. Smaller buffers increase
     * round trips; larger buffers waste bandwidth on small or random reads. The default (256 KB)
     * balances latency and throughput for typical checkpoint/state file access patterns. This is
     * independent of {@link #WRITE_REQUEST_SIZE} which uses larger blocks (default 8 MB) to
     * amortize HTTP overhead on sequential uploads.
     */
    public static final ConfigOption<MemorySize> READ_BUFFER_SIZE =
            ConfigOptions.key("fs.azure.read.buffer-size")
                    .memoryType()
                    .defaultValue(new MemorySize(256 * 1024L))
                    .withDescription("Read buffer size for input streams.");
}
