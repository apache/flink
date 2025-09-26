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

package org.apache.flink.fs.s3.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for handling Hadoop S3 configuration compatibility between AWS SDK V1 and V2.
 *
 * <p>This class provides compatibility transformations needed when using Hadoop 3.4.x with AWS SDK
 * V1 components. It automatically detects V1 presence and applies necessary configuration
 * adjustments.
 */
@Internal
public final class HadoopS3Compatibility {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopS3Compatibility.class);

    private static final String AWS_SDK_V1_CREDENTIALS_PROVIDER_CLASS =
            "com.amazonaws.auth.AWSCredentialsProvider";

    private static final String CREDENTIAL_PROVIDER_MAPPING_PROPERTY =
            "fs.s3a.aws.credentials.provider.mapping";

    private static final String V1_PROVIDER_CLASS =
            "org.apache.flink.fs.s3.common.token.DynamicTemporaryAWSCredentialsProvider";
    private static final String V2_PROVIDER_CLASS =
            "org.apache.flink.fs.s3.common.token.DynamicTemporaryAWSCredentialsProviderV2";

    private static final ConfigOption<List<String>> DURATION_CONFIG_KEYS =
            ConfigOptions.key("s3.v1-compat.duration-keys")
                    .stringType()
                    .asList()
                    .defaultValues(
                            "fs.s3a.connection.establish.timeout",
                            "fs.s3a.connection.timeout",
                            "fs.s3a.connection.ttl",
                            "fs.s3a.socket.timeout",
                            "fs.s3a.request.timeout",
                            "fs.s3a.retry.throttle.limit",
                            "fs.s3a.threads.keepalivetime",
                            "fs.s3a.retry.interval",
                            "fs.s3a.retry.throttle.interval")
                    .withDescription(
                            "List of Hadoop S3 configuration keys that should have their "
                                    + "duration strings converted to milliseconds for AWS SDK V1 compatibility.");

    private static final String DURATION_PATTERN = "\\d+[smhd]$";

    /**
     * Applies AWS SDK V1 compatibility configurations.
     *
     * <p>This method performs the following compatibility adjustments:
     *
     * <ul>
     *   <li>Sets up credential provider mapping from V1 to V2 providers
     *   <li>Converts duration strings to milliseconds for V1 timeout configurations
     * </ul>
     *
     * <p>Ref: <a
     * href="https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html"></a>
     *
     * @param hadoopConfig the Hadoop configuration to modify
     * @param flinkConfig the Flink configuration to read settings from
     */
    static void applyV1Compatibility(
            org.apache.hadoop.conf.Configuration hadoopConfig, Configuration flinkConfig) {
        LOG.debug("Applying AWS SDK V1 compatibility configurations");
        setCredentialProviderMapping(hadoopConfig);
        convertDurationStringsToMilliseconds(hadoopConfig, flinkConfig);
    }

    public static boolean isAwsSdkV1Present() {
        try {
            Class.forName(AWS_SDK_V1_CREDENTIALS_PROVIDER_CLASS);
            LOG.debug("AWS SDK V1 detected in classpath");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static void setCredentialProviderMapping(
            org.apache.hadoop.conf.Configuration hadoopConfig) {
        if (hadoopConfig.get(CREDENTIAL_PROVIDER_MAPPING_PROPERTY) == null) {
            String mapping = V1_PROVIDER_CLASS + "=" + V2_PROVIDER_CLASS;
            LOG.debug("Setting credential provider mapping: {}", mapping);
            hadoopConfig.set(CREDENTIAL_PROVIDER_MAPPING_PROPERTY, mapping);
        } else {
            LOG.debug("Credential provider mapping already configured, skipping");
        }
    }

    private static void convertDurationStringsToMilliseconds(
            org.apache.hadoop.conf.Configuration hadoopConfig, Configuration flinkConfig) {
        List<String> durationKeys = flinkConfig.get(DURATION_CONFIG_KEYS);
        for (String key : durationKeys) {
            String value = hadoopConfig.get(key);
            if (value != null && isDurationString(value)) {
                try {
                    long milliseconds = parseDurationToMillis(value);
                    LOG.debug(
                            "Converting duration config {} from '{}' to '{}' milliseconds",
                            key,
                            value,
                            milliseconds);
                    hadoopConfig.set(key, String.valueOf(milliseconds));
                } catch (NumberFormatException e) {
                    LOG.warn(
                            "Failed to convert duration string '{}' for key '{}': {}",
                            value,
                            key,
                            e.getMessage());
                }
            }
        }
    }

    private static boolean isDurationString(String value) {
        return value.matches(DURATION_PATTERN);
    }

    private static long parseDurationToMillis(String duration) {
        if (duration.length() < 2) {
            throw new NumberFormatException("Invalid duration format: " + duration);
        }

        char unit = duration.charAt(duration.length() - 1);
        long value = Long.parseLong(duration.substring(0, duration.length() - 1));

        switch (unit) {
            case 's':
                return TimeUnit.SECONDS.toMillis(value);
            case 'm':
                return TimeUnit.MINUTES.toMillis(value);
            case 'h':
                return TimeUnit.HOURS.toMillis(value);
            case 'd':
                return TimeUnit.DAYS.toMillis(value);
            default:
                throw new NumberFormatException("Unsupported duration unit: " + unit);
        }
    }
}
