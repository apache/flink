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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.io.IOException;
import java.net.URI;

public class NativeS3FileSystemFactory implements FileSystemFactory {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3FileSystemFactory.class);

    private static final String INVALID_ENTROPY_KEY_CHARS = "^.*[~#@*+%{}<>\\[\\]|\"\\\\].*$";

    public static final long S3_MULTIPART_MIN_PART_SIZE = 5L << 20;

    public static final ConfigOption<String> ACCESS_KEY =
            ConfigOptions.key("s3.access-key")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("s3.access.key")
                    .withDescription("AWS access key");

    public static final ConfigOption<String> SECRET_KEY =
            ConfigOptions.key("s3.secret-key")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("s3.secret.key")
                    .withDescription("AWS secret key");

    public static final ConfigOption<String> REGION =
            ConfigOptions.key("s3.region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "AWS region. If not specified, the region will be automatically detected using AWS SDK's "
                                    + "DefaultAwsRegionProviderChain, which checks (in order): AWS_REGION env var, "
                                    + "~/.aws/config, EC2 instance metadata, and bucket location API.");

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("s3.endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Custom S3 endpoint");

    public static final ConfigOption<Boolean> PATH_STYLE_ACCESS =
            ConfigOptions.key("s3.path-style-access")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys("s3.path.style.access")
                    .withDescription("Use path-style access for S3 (for S3-compatible storage)");

    public static final ConfigOption<Boolean> USE_ANONYMOUS_CREDENTIALS =
            ConfigOptions.key("s3.anonymous-credentials")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Use anonymous (unsigned) requests - useful for public buckets or MinIO testing");

    public static final ConfigOption<Long> PART_UPLOAD_MIN_SIZE =
            ConfigOptions.key("s3.upload.min.part.size")
                    .longType()
                    .defaultValue(S3_MULTIPART_MIN_PART_SIZE)
                    .withDescription(
                            "Minimum size of data buffered locally before sending to S3 (5MB to 5GB)");

    public static final ConfigOption<Integer> MAX_CONCURRENT_UPLOADS =
            ConfigOptions.key("s3.upload.max.concurrent.uploads")
                    .intType()
                    .defaultValue(Runtime.getRuntime().availableProcessors())
                    .withDescription("Maximum number of concurrent part uploads per stream");

    public static final ConfigOption<String> ENTROPY_INJECT_KEY_OPTION =
            ConfigOptions.key("s3.entropy.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Key to be replaced by random entropy for sharding optimization");

    public static final ConfigOption<Integer> ENTROPY_INJECT_LENGTH_OPTION =
            ConfigOptions.key("s3.entropy.length")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Number of random characters for entropy injection");

    public static final ConfigOption<Boolean> BULK_COPY_ENABLED =
            ConfigOptions.key("s3.bulk-copy.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable bulk copy operations using S3TransferManager");

    public static final ConfigOption<Integer> BULK_COPY_MAX_CONCURRENT =
            ConfigOptions.key("s3.bulk-copy.max-concurrent")
                    .intType()
                    .defaultValue(16)
                    .withDescription("Maximum number of concurrent copy operations");

    public static final ConfigOption<Boolean> USE_ASYNC_OPERATIONS =
            ConfigOptions.key("s3.async.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enable async read/write operations using S3TransferManager for improved performance");

    public static final ConfigOption<Integer> READ_BUFFER_SIZE =
            ConfigOptions.key("s3.read.buffer.size")
                    .intType()
                    .defaultValue(256 * 1024) // 256KB default
                    .withDescription(
                            "Read buffer size in bytes for S3 input streams. "
                                    + "Larger buffers improve throughput but consume more memory. "
                                    + "Range: 64KB - 4MB. Default: 256KB");

    private Configuration flinkConfig;

    @Override
    public String getScheme() {
        return "s3";
    }

    @Override
    public void configure(Configuration config) {
        this.flinkConfig = config;
        LOG.info("S3 FileSystem Factory configured. Config keys: {}", config.keySet());
        LOG.info("Access key in config: {}", config.contains(ACCESS_KEY));
        LOG.info("Endpoint in config: {}", config.contains(ENDPOINT));
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        Configuration config = this.flinkConfig;

        if (config == null) {
            LOG.warn("Creating S3 FileSystem without configuration. Using defaults.");
            config = new Configuration();
        }
        LOG.info("Creating Native S3 FileSystem for URI: {}", fsUri);
        String accessKey = config.get(ACCESS_KEY);
        String secretKey = config.get(SECRET_KEY);
        String region = config.get(REGION);
        String endpoint = config.get(ENDPOINT);
        boolean pathStyleAccess = config.get(PATH_STYLE_ACCESS);
        // Auto-enable path-style access for custom endpoints (MinIO, LocalStack, etc.)
        if (endpoint != null && !pathStyleAccess) {
            LOG.info(
                    "Custom endpoint detected ({}), automatically enabling path-style access for S3-compatible storage",
                    endpoint);
            pathStyleAccess = true;
        }
        LOG.info(
                "Initializing S3 filesystem - endpoint: {}, region: {}, pathStyle: {}",
                endpoint != null ? endpoint : "AWS S3",
                region,
                pathStyleAccess);

        if (accessKey == null || secretKey == null) {
            LOG.info(
                    "S3 credentials not provided, using default AWS credentials chain (environment, system properties, IAM, etc.)");
        }

        S3ClientProvider clientProvider =
                S3ClientProvider.builder()
                        .accessKey(accessKey)
                        .secretKey(secretKey)
                        .region(region)
                        .endpoint(endpoint)
                        .pathStyleAccess(pathStyleAccess)
                        .build();

        String entropyInjectionKey = config.get(ENTROPY_INJECT_KEY_OPTION);
        int numEntropyChars = -1;
        if (entropyInjectionKey != null) {
            if (entropyInjectionKey.matches(INVALID_ENTROPY_KEY_CHARS)) {
                throw new IllegalConfigurationException(
                        "Invalid character in entropy injection key '"
                                + entropyInjectionKey
                                + "'. Special characters are not allowed.");
            }
            numEntropyChars = config.get(ENTROPY_INJECT_LENGTH_OPTION);
            if (numEntropyChars <= 0) {
                throw new IllegalConfigurationException(
                        ENTROPY_INJECT_LENGTH_OPTION.key()
                                + " must be greater than 0, got: "
                                + numEntropyChars);
            }
            LOG.info(
                    "Entropy injection enabled - key: {}, length: {}",
                    entropyInjectionKey,
                    numEntropyChars);
        }

        final String[] localTmpDirectories = ConfigurationUtils.parseTempDirectories(config);
        Preconditions.checkArgument(
                localTmpDirectories.length > 0, "No temporary directories configured");
        final String localTmpDirectory = localTmpDirectories[0];

        final long s3minPartSize = config.get(PART_UPLOAD_MIN_SIZE);
        final int maxConcurrentUploads = config.get(MAX_CONCURRENT_UPLOADS);

        // Validate configuration parameters
        if (s3minPartSize < S3_MULTIPART_MIN_PART_SIZE) {
            throw new IllegalConfigurationException(
                    String.format(
                            "%s must be at least %d bytes (5MB), got: %d",
                            PART_UPLOAD_MIN_SIZE.key(), S3_MULTIPART_MIN_PART_SIZE, s3minPartSize));
        }
        if (s3minPartSize > 5L * 1024 * 1024 * 1024) { // 5GB max
            throw new IllegalConfigurationException(
                    String.format(
                            "%s must not exceed 5GB, got: %d bytes",
                            PART_UPLOAD_MIN_SIZE.key(), s3minPartSize));
        }
        if (maxConcurrentUploads <= 0) {
            throw new IllegalConfigurationException(
                    String.format(
                            "%s must be greater than 0, got: %d",
                            MAX_CONCURRENT_UPLOADS.key(), maxConcurrentUploads));
        }
        if (maxConcurrentUploads > 10000) {
            LOG.warn(
                    "Very high value for {} ({}), this may cause excessive resource usage",
                    MAX_CONCURRENT_UPLOADS.key(),
                    maxConcurrentUploads);
        }

        // Check async operations configuration
        boolean useAsyncOperations = config.get(USE_ASYNC_OPERATIONS);
        LOG.info("Async read/write operations: {}", useAsyncOperations ? "ENABLED" : "DISABLED");

        // Get read buffer size with validation
        int readBufferSize = config.get(READ_BUFFER_SIZE);
        if (readBufferSize < 64 * 1024) {
            LOG.warn("Read buffer size {} is too small, using minimum 64KB", readBufferSize);
            readBufferSize = 64 * 1024;
        } else if (readBufferSize > 4 * 1024 * 1024) {
            LOG.warn("Read buffer size {} is too large, using maximum 4MB", readBufferSize);
            readBufferSize = 4 * 1024 * 1024;
        }
        LOG.info(
                "S3 read buffer size: {} KB (optimized for memory efficiency)",
                readBufferSize / 1024);

        NativeS3BulkCopyHelper bulkCopyHelper = null;
        if (config.get(BULK_COPY_ENABLED)) {
            S3AsyncClient asyncClient = clientProvider.getAsyncClient();

            S3TransferManager transferManager =
                    S3TransferManager.builder().s3Client(asyncClient).build();

            bulkCopyHelper =
                    new NativeS3BulkCopyHelper(
                            transferManager, config.get(BULK_COPY_MAX_CONCURRENT));

            LOG.info(
                    "Bulk copy enabled with max concurrent operations: {}",
                    config.get(BULK_COPY_MAX_CONCURRENT));
        }

        return new NativeS3FileSystem(
                clientProvider,
                fsUri,
                entropyInjectionKey,
                numEntropyChars,
                localTmpDirectory,
                s3minPartSize,
                maxConcurrentUploads,
                bulkCopyHelper,
                useAsyncOperations,
                readBufferSize);
    }
}
