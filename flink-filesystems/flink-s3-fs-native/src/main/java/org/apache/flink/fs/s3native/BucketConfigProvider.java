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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Parses bucket-specific S3 configuration using format {@code s3.bucket.<bucket-name>.<property>}.
 *
 * <p>Enables per-bucket overrides for endpoints, credentials, encryption, and IAM roles. Bucket
 * names containing dots are supported; properties are matched by longest suffix first.
 *
 * <p>Immutable and thread-safe after construction.
 */
@Internal
final class BucketConfigProvider {

    private static final Logger LOG = LoggerFactory.getLogger(BucketConfigProvider.class);
    static final String BUCKET_CONFIG_PREFIX = "s3.bucket.";
    static final Map<String, BiConsumer<S3BucketConfig.Builder, String>> PROPERTY_APPLICATORS;
    static final List<String> KNOWN_PROPERTIES_BY_LENGTH;

    static {
        final Map<String, BiConsumer<S3BucketConfig.Builder, String>> applicators =
                new LinkedHashMap<>();
        applicators.put("access-key", S3BucketConfig.Builder::accessKey);
        applicators.put("assume-role.arn", S3BucketConfig.Builder::assumeRoleArn);
        applicators.put("assume-role.external-id", S3BucketConfig.Builder::assumeRoleExternalId);
        applicators.put(
                "assume-role.session-duration",
                (b, v) -> {
                    try {
                        b.assumeRoleSessionDurationSeconds(Integer.parseInt(v));
                    } catch (NumberFormatException e) {
                        throw new IllegalConfigurationException(
                                String.format(
                                        "Invalid assume-role.session-duration '%s' for bucket '%s'. "
                                                + "Must be a valid integer (e.g., 3600)",
                                        v, b.getBucketName()),
                                e);
                    }
                });
        applicators.put("assume-role.session-name", S3BucketConfig.Builder::assumeRoleSessionName);
        applicators.put("aws.credentials.provider", S3BucketConfig.Builder::credentialsProvider);
        applicators.put("endpoint", S3BucketConfig.Builder::endpoint);
        applicators.put(
                "path-style-access",
                (b, v) -> {
                    if (!"true".equalsIgnoreCase(v) && !"false".equalsIgnoreCase(v)) {
                        throw new IllegalConfigurationException(
                                String.format(
                                        "Invalid path-style-access '%s' for bucket '%s'. "
                                                + "Must be 'true' or 'false'",
                                        v, b.getBucketName()));
                    }
                    b.pathStyleAccess(Boolean.parseBoolean(v));
                });
        applicators.put("region", S3BucketConfig.Builder::region);
        applicators.put("secret-key", S3BucketConfig.Builder::secretKey);
        applicators.put("sse.kms.key-id", S3BucketConfig.Builder::sseKmsKeyId);
        applicators.put("sse.type", S3BucketConfig.Builder::sseType);
        PROPERTY_APPLICATORS = Collections.unmodifiableMap(applicators);

        KNOWN_PROPERTIES_BY_LENGTH =
                applicators.keySet().stream()
                        .sorted(Comparator.comparingInt(String::length).reversed())
                        .collect(Collectors.toList());
    }

    private final Map<String, S3BucketConfig> bucketConfigs;

    BucketConfigProvider(Configuration flinkConfig) {
        this.bucketConfigs = Collections.unmodifiableMap(parseBucketConfigs(flinkConfig));
    }

    @Nullable
    S3BucketConfig getBucketConfig(String bucketName) {
        return bucketConfigs.get(bucketName);
    }

    @VisibleForTesting
    boolean hasBucketConfig(String bucketName) {
        return bucketConfigs.containsKey(bucketName);
    }

    @VisibleForTesting
    int size() {
        return bucketConfigs.size();
    }

    private static Map<String, S3BucketConfig> parseBucketConfigs(Configuration flinkConfig) {
        final Map<String, Map<String, String>> rawConfigs = new HashMap<>();

        for (final String key : flinkConfig.keySet()) {
            if (!key.startsWith(BUCKET_CONFIG_PREFIX)) {
                continue;
            }
            final String suffix = key.substring(BUCKET_CONFIG_PREFIX.length());
            if (StringUtils.isNullOrWhitespaceOnly(suffix)) {
                continue;
            }
            final String value = flinkConfig.getString(key, null);
            if (StringUtils.isNullOrWhitespaceOnly(value)) {
                continue;
            }

            boolean matched = false;
            for (final String prop : KNOWN_PROPERTIES_BY_LENGTH) {
                if (suffix.endsWith("." + prop)) {
                    final String bucketName =
                            suffix.substring(0, suffix.length() - prop.length() - 1);
                    if (StringUtils.isNullOrWhitespaceOnly(bucketName)) {
                        LOG.warn(
                                "Ignoring bucket config key '{}': "
                                        + "resolved bucket name is empty (missing bucket name between "
                                        + "'s3.bucket.' prefix and '.{}' property?).",
                                key,
                                prop);
                    } else {
                        rawConfigs
                                .computeIfAbsent(bucketName, k -> new HashMap<>())
                                .put(prop, value);
                    }
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                LOG.warn(
                        "Ignoring unrecognized bucket config key '{}'. "
                                + "Known bucket-level properties: {}",
                        key,
                        PROPERTY_APPLICATORS.keySet());
            }
        }

        final Map<String, S3BucketConfig> result = new HashMap<>();
        for (final Map.Entry<String, Map<String, String>> entry : rawConfigs.entrySet()) {
            final String bucketName = entry.getKey();
            final Map<String, String> props = entry.getValue();

            final S3BucketConfig bucketConfig = buildBucketConfig(bucketName, props);
            if (bucketConfig.hasAnyOverride()) {
                result.put(bucketName, bucketConfig);
                LOG.info(
                        "Registered bucket-specific configuration for bucket '{}': {}",
                        bucketName,
                        bucketConfig);
            }
        }

        return result;
    }

    private static S3BucketConfig buildBucketConfig(String bucketName, Map<String, String> props) {
        final S3BucketConfig.Builder builder = S3BucketConfig.builder(bucketName);

        for (final Map.Entry<String, BiConsumer<S3BucketConfig.Builder, String>> entry :
                PROPERTY_APPLICATORS.entrySet()) {
            final String value = props.get(entry.getKey());
            if (value != null) {
                entry.getValue().accept(builder, value);
            }
        }

        return builder.build();
    }
}
