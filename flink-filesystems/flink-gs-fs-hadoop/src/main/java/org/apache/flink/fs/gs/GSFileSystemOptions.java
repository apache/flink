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

package org.apache.flink.fs.gs;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.Optional;

import static org.apache.flink.configuration.description.LinkElement.link;

/** The GS file system options. */
public class GSFileSystemOptions {

    /* Flink config option to set the bucket name for temporary blobs. */
    public static final ConfigOption<String> WRITER_TEMPORARY_BUCKET_NAME =
            ConfigOptions.key("gs.writer.temporary.bucket.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "This option sets the bucket name used by the recoverable writer to store temporary files. "
                                    + "If not specified, temporary files are stored in the same bucket as the final file being written.");

    /* Flink config option to set the chunk size for writing to GCS. */
    public static final ConfigOption<MemorySize> WRITER_CHUNK_SIZE =
            ConfigOptions.key("gs.writer.chunk.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "This option sets the chunk size for writes to the underlying Google storage. If set, this must be a multiple "
                                    + "of 256KB. If not set, writes will use Google's default chunk size.");

    /* Flink config option to determine if entropy should be enabled in filesink gcs path. */
    public static final ConfigOption<Boolean> ENABLE_FILESINK_ENTROPY =
            ConfigOptions.key("gs.filesink.entropy.enabled")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription(
                            "This option can be used to improve performance due to hotspotting "
                                    + "issues on GCS. If this is enabled, entropy in the form of "
                                    + "temporary object id will be injected in the beginning of "
                                    + "gcs path of the temporary objects. The final object path "
                                    + "remains unchanged.");

    /**
     * Flink config option to set the http connection timeout. It will be used by cloud-storage
     * library.
     */
    public static final ConfigOption<Integer> GCS_HTTP_CONNECT_TIMEOUT =
            ConfigOptions.key("gs.http.connect-timeout")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "This option sets the timeout in milliseconds to establish a connection. GCS default will be used if not configured.");

    /**
     * Flink config option to set the http read timeout. It will be used by cloud-storage library.
     */
    public static final ConfigOption<Integer> GCS_HTTP_READ_TIMEOUT =
            ConfigOptions.key("gs.http.read-timeout")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "If configured this option sets the timeout in milliseconds to read data from an established connection. GCS default will be used if not configured");

    /**
     * Flink config option to set the http read timeout. It will be used by cloud-storage library.
     */
    public static final ConfigOption<Integer> GCS_RETRY_MAX_ATTEMPT =
            ConfigOptions.key("gs.retry.max-attempt")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If configured this option defines the maximum number of retry attempts to perform. GCS default will be used if not configured. See GCS %s for more information.",
                                            link(
                                                    "https://cloud.google.com/java/docs/reference/gax/latest/com.google.api.gax.retrying.RetrySettings#com_google_api_gax_retrying_RetrySettings_getMaxAttempts__",
                                                    "documentation"))
                                    .build());

    public static final ConfigOption<Duration> GCS_RETRY_INIT_RPC_TIMEOUT =
            ConfigOptions.key("gs.retry.init-rpc-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If configured this option controls the timeout for the initial RPC. Subsequent calls will use this value adjusted according to the gs.retry.rpc-timeout-multiplier. GCS default will be used if not configured. See GCS %s for more information.",
                                            link(
                                                    "https://cloud.google.com/java/docs/reference/gax/latest/com.google.api.gax.retrying.RetrySettings#com_google_api_gax_retrying_RetrySettings_getInitialRpcTimeout__",
                                                    "documentation"))
                                    .build());

    public static final ConfigOption<Double> GCS_RETRY_RPC_TIMEOUT_MULTIPLIER =
            ConfigOptions.key("gs.retry.rpc-timeout-multiplier")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If configured this option controls the change in delay before the next retry or poll. The timeout of the previous call is multiplied by the RpcTimeoutMultiplier to calculate the timeout for the next call. See GCS %s for more information.",
                                            link(
                                                    "https://cloud.google.com/java/docs/reference/gax/latest/com.google.api.gax.retrying.RetrySettings#com_google_api_gax_retrying_RetrySettings_getRpcTimeoutMultiplier__",
                                                    "documentation"))
                                    .build());

    public static final ConfigOption<Duration> GCS_RETRY_MAX_RPC_TIMEOUT =
            ConfigOptions.key("gs.retry.max-rpc-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If configured this option puts a limit on the value of the RPC timeout, so that the max rpc timeout can't increase the RPC timeout higher than this amount. GCS default will be used if not configured. See GCS %s for more information.",
                                            link(
                                                    "https://cloud.google.com/java/docs/reference/gax/latest/com.google.api.gax.retrying.RetrySettings#com_google_api_gax_retrying_RetrySettings_getMaxRpcTimeout__",
                                                    "documentation"))
                                    .build());

    public static final ConfigOption<Duration> GCS_RETRY_TOTAL_TIMEOUT =
            ConfigOptions.key("gs.retry.total-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If configured total duration during which retries could be attempted. GCS default will be used if not configured. See GCS %s for more information.",
                                            link(
                                                    "https://cloud.google.com/java/docs/reference/gax/latest/com.google.api.gax.retrying.RetrySettings#com_google_api_gax_retrying_RetrySettings_getTotalTimeout__",
                                                    "documentation"))
                                    .build());

    /** The Flink configuration. */
    private final Configuration flinkConfig;

    /**
     * Constructs an options instance.
     *
     * @param flinkConfig The Flink configuration
     */
    public GSFileSystemOptions(Configuration flinkConfig) {
        this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
        this.flinkConfig
                .getOptional(WRITER_CHUNK_SIZE)
                .ifPresent(
                        chunkSize ->
                                Preconditions.checkArgument(
                                        chunkSize.getBytes() > 0
                                                && chunkSize.getBytes() % (256 * 1024) == 0,
                                        "Writer chunk size must be greater than zero and a multiple of 256KB"));
    }

    /**
     * The temporary bucket name to use for recoverable writes, if different from the final bucket
     * name.
     */
    public Optional<String> getWriterTemporaryBucketName() {
        return flinkConfig.getOptional(WRITER_TEMPORARY_BUCKET_NAME);
    }

    /** Timeout in millisecond to establish the connection. */
    public Optional<Integer> getHTTPConnectionTimeout() {
        return flinkConfig.getOptional(GCS_HTTP_CONNECT_TIMEOUT);
    }

    /** Timeout in millisecond to read content from connection. */
    public Optional<Integer> getHTTPReadTimeout() {
        return flinkConfig.getOptional(GCS_HTTP_READ_TIMEOUT);
    }

    public Optional<Integer> getMaxAttempts() {
        return flinkConfig.getOptional(GCS_RETRY_MAX_ATTEMPT);
    }

    public Optional<org.threeten.bp.Duration> getInitialRpcTimeout() {
        return flinkConfig
                .getOptional(GCS_RETRY_INIT_RPC_TIMEOUT)
                .map(timeout -> org.threeten.bp.Duration.ofMillis(timeout.toMillis()));
    }

    public Optional<Double> getRpcTimeoutMultiplier() {
        return flinkConfig.getOptional(GCS_RETRY_RPC_TIMEOUT_MULTIPLIER);
    }

    public Optional<org.threeten.bp.Duration> getMaxRpcTimeout() {
        return flinkConfig
                .getOptional(GCS_RETRY_MAX_RPC_TIMEOUT)
                .map(timeout -> org.threeten.bp.Duration.ofMillis(timeout.toMillis()));
    }

    public Optional<org.threeten.bp.Duration> getTotalTimeout() {
        return flinkConfig
                .getOptional(GCS_RETRY_TOTAL_TIMEOUT)
                .map(timeout -> org.threeten.bp.Duration.ofMillis(timeout.toMillis()));
    }

    /** The chunk size to use for writes on the underlying Google WriteChannel. */
    public Optional<MemorySize> getWriterChunkSize() {
        return flinkConfig.getOptional(WRITER_CHUNK_SIZE);
    }

    /** Whether entropy insertion is enabled in filesink path. */
    public Boolean isFileSinkEntropyEnabled() {
        return flinkConfig.get(ENABLE_FILESINK_ENTROPY);
    }

    @Override
    public String toString() {
        return "GSFileSystemOptions{"
                + "writerTemporaryBucketName="
                + getWriterTemporaryBucketName()
                + ", writerChunkSize="
                + getWriterChunkSize()
                + '}';
    }
}
