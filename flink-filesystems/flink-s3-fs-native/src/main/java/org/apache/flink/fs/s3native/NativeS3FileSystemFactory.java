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

    // Server-Side Encryption (SSE) Configuration
    public static final ConfigOption<String> SSE_TYPE =
            ConfigOptions.key("s3.sse.type")
                    .stringType()
                    .defaultValue("none")
                    .withDescription(
                            "Server-side encryption type. Valid values: "
                                    + "'none' (no encryption), "
                                    + "'sse-s3' or 'AES256' (S3-managed keys), "
                                    + "'sse-kms' or 'aws:kms' (KMS-managed keys)");

    public static final ConfigOption<String> SSE_KMS_KEY_ID =
            ConfigOptions.key("s3.sse.kms.key-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "KMS key ID, ARN, or alias for SSE-KMS encryption. "
                                    + "If not specified with SSE-KMS, the default AWS-managed key (aws/s3) is used. "
                                    + "Example: 'arn:aws:kms:us-east-1:123456789:key/12345678-1234-1234-1234-123456789abc' "
                                    + "or 'alias/my-s3-key'");

    // IAM Assume Role Configuration
    public static final ConfigOption<String> ASSUME_ROLE_ARN =
            ConfigOptions.key("s3.assume-role.arn")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "ARN of the IAM role to assume for S3 access. "
                                    + "Enables cross-account access or temporary elevated permissions. "
                                    + "Example: 'arn:aws:iam::123456789012:role/S3AccessRole'");

    public static final ConfigOption<String> ASSUME_ROLE_EXTERNAL_ID =
            ConfigOptions.key("s3.assume-role.external-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "External ID for assume role (required for cross-account access with external ID condition)");

    public static final ConfigOption<String> ASSUME_ROLE_SESSION_NAME =
            ConfigOptions.key("s3.assume-role.session-name")
                    .stringType()
                    .defaultValue("flink-s3-session")
                    .withDescription("Session name for the assumed role session");

    public static final ConfigOption<Integer> ASSUME_ROLE_SESSION_DURATION_SECONDS =
            ConfigOptions.key("s3.assume-role.session-duration")
                    .intType()
                    .defaultValue(3600) // 1 hour default
                    .withDescription(
                            "Duration in seconds for the assumed role session (900-43200 seconds, default: 3600)");

    private Configuration flinkConfig;

    @Override
    public String getScheme() {
        return "s3";
    }

    @Override
    public void configure(Configuration config) {
        this.flinkConfig = config;
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        Configuration config = this.flinkConfig;
        if (config == null) {
            config = new Configuration();
        }

        String accessKey = config.get(ACCESS_KEY);
        String secretKey = config.get(SECRET_KEY);
        String region = config.get(REGION);
        String endpoint = config.get(ENDPOINT);
        boolean pathStyleAccess = config.get(PATH_STYLE_ACCESS);

        if (endpoint != null && !pathStyleAccess) {
            pathStyleAccess = true;
        }

        S3EncryptionConfig encryptionConfig =
                S3EncryptionConfig.fromConfig(config.get(SSE_TYPE), config.get(SSE_KMS_KEY_ID));

        S3ClientProvider clientProvider =
                S3ClientProvider.builder()
                        .accessKey(accessKey)
                        .secretKey(secretKey)
                        .region(region)
                        .endpoint(endpoint)
                        .pathStyleAccess(pathStyleAccess)
                        .assumeRoleArn(config.get(ASSUME_ROLE_ARN))
                        .assumeRoleExternalId(config.get(ASSUME_ROLE_EXTERNAL_ID))
                        .assumeRoleSessionName(config.get(ASSUME_ROLE_SESSION_NAME))
                        .assumeRoleSessionDurationSeconds(
                                config.get(ASSUME_ROLE_SESSION_DURATION_SECONDS))
                        .encryptionConfig(encryptionConfig)
                        .build();

        String entropyInjectionKey = config.get(ENTROPY_INJECT_KEY_OPTION);
        int numEntropyChars = -1;
        if (entropyInjectionKey != null) {
            if (entropyInjectionKey.matches(INVALID_ENTROPY_KEY_CHARS)) {
                throw new IllegalConfigurationException(
                        "Invalid character in entropy injection key: " + entropyInjectionKey);
            }
            numEntropyChars = config.get(ENTROPY_INJECT_LENGTH_OPTION);
            if (numEntropyChars <= 0) {
                throw new IllegalConfigurationException(
                        ENTROPY_INJECT_LENGTH_OPTION.key() + " must be > 0");
            }
        }

        final String[] localTmpDirectories = ConfigurationUtils.parseTempDirectories(config);
        Preconditions.checkArgument(
                localTmpDirectories.length > 0,
                "No temporary directories configured. "
                        + "Please configure at least one temp directory via 'io.tmp.dirs' or similar.");
        final String localTmpDirectory = localTmpDirectories[0];

        final long s3minPartSize = config.get(PART_UPLOAD_MIN_SIZE);
        final int maxConcurrentUploads = config.get(MAX_CONCURRENT_UPLOADS);

        // Validate part size: S3 requires minimum 5MB and maximum 5GB per part
        Preconditions.checkArgument(
                s3minPartSize >= S3_MULTIPART_MIN_PART_SIZE,
                "%s must be at least 5MB (5242880 bytes), but was %d bytes",
                PART_UPLOAD_MIN_SIZE.key(),
                s3minPartSize);
        Preconditions.checkArgument(
                s3minPartSize <= 5L * 1024 * 1024 * 1024,
                "%s must not exceed 5GB (5368709120 bytes), but was %d bytes",
                PART_UPLOAD_MIN_SIZE.key(),
                s3minPartSize);
        Preconditions.checkArgument(
                maxConcurrentUploads > 0,
                "%s must be positive, but was %d",
                MAX_CONCURRENT_UPLOADS.key(),
                maxConcurrentUploads);

        boolean useAsyncOperations = config.get(USE_ASYNC_OPERATIONS);

        // Validate and clamp read buffer size to sensible range [64KB, 4MB]
        // We clip rather than throw to provide flexibility while preventing extreme values
        int configuredReadBufferSize = config.get(READ_BUFFER_SIZE);
        int readBufferSize =
                Math.max(64 * 1024, Math.min(configuredReadBufferSize, 4 * 1024 * 1024));
        if (readBufferSize != configuredReadBufferSize) {
            LOG.warn(
                    "{} value {} was outside valid range [64KB, 4MB]. Using {} instead.",
                    READ_BUFFER_SIZE.key(),
                    configuredReadBufferSize,
                    readBufferSize);
        }

        NativeS3BulkCopyHelper bulkCopyHelper = null;
        if (config.get(BULK_COPY_ENABLED)) {
            S3TransferManager transferManager =
                    S3TransferManager.builder().s3Client(clientProvider.getAsyncClient()).build();
            bulkCopyHelper =
                    new NativeS3BulkCopyHelper(
                            transferManager, config.get(BULK_COPY_MAX_CONCURRENT));
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
