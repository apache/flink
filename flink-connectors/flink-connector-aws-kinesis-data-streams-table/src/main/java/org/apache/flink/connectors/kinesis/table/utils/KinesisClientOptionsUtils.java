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

package org.apache.flink.connectors.kinesis.table.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.base.table.options.ConfigurationValidator;
import org.apache.flink.connector.base.table.options.TableOptionsUtils;

import software.amazon.awssdk.http.Protocol;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Class for handling Kinesis async client specific options. */
@PublicEvolving
public class KinesisClientOptionsUtils implements TableOptionsUtils, ConfigurationValidator {

    private static final String CLIENT_MAX_CONCURRENCY_OPTION = "max-concurrency";
    private static final String CLIENT_MAX_TIMEOUT_OPTION = "read-timeout";
    private static final String CLIENT_HTTP_PROTOCOL_VERSION_OPTION = "protocol.version";

    private final Map<String, String> resolvedOptions;

    public KinesisClientOptionsUtils(Map<String, String> resolvedOptions) {
        this.resolvedOptions = resolvedOptions;
    }

    /** Prefix for properties defined in {@link AWSConfigConstants}. */
    public static final String SINK_CLIENT_PREFIX = "sink.http-client.";

    @Override
    public Map<String, String> getProcessedResolvedOptions() {
        Map<String, String> mappedResolvedOptions = new HashMap<>();
        for (String key : resolvedOptions.keySet()) {
            if (key.startsWith(SINK_CLIENT_PREFIX)) {
                mappedResolvedOptions.put(translateClientKeys(key), resolvedOptions.get(key));
            }
        }
        return mappedResolvedOptions;
    }

    @Override
    public List<String> getNonValidatedPrefixes() {
        return Collections.singletonList(SINK_CLIENT_PREFIX);
    }

    @Override
    public Properties getValidatedConfigurations() {
        Properties clientConfigurations = new Properties();
        Map<String, String> mappedProperties = getProcessedResolvedOptions();
        for (Map.Entry<String, String> entry : mappedProperties.entrySet()) {
            clientConfigurations.setProperty(entry.getKey(), entry.getValue());
        }
        validatedConfigurations(clientConfigurations);
        return clientConfigurations;
    }

    private static String translateClientKeys(String key) {
        String truncatedKey = key.substring(SINK_CLIENT_PREFIX.length());
        switch (truncatedKey) {
            case CLIENT_MAX_CONCURRENCY_OPTION:
                return AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY;
            case CLIENT_MAX_TIMEOUT_OPTION:
                return AWSConfigConstants.HTTP_CLIENT_READ_TIMEOUT_MILLIS;
            case CLIENT_HTTP_PROTOCOL_VERSION_OPTION:
                return AWSConfigConstants.HTTP_PROTOCOL_VERSION;
            default:
                return truncatedKey;
        }
    }

    private static void validatedConfigurations(Properties config) {
        ConfigurationValidator.validateOptionalPositiveIntProperty(
                config,
                AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY,
                "Invalid value given for HTTP client max concurrency. Must be positive integer.");
        ConfigurationValidator.validateOptionalPositiveIntProperty(
                config,
                AWSConfigConstants.HTTP_CLIENT_READ_TIMEOUT_MILLIS,
                "Invalid value given for HTTP read timeout. Must be positive integer.");
        validateOptionalHttpProtocolProperty(config);
    }

    private static void validateOptionalHttpProtocolProperty(Properties config) {
        if (config.containsKey(AWSConfigConstants.HTTP_PROTOCOL_VERSION)) {
            try {
                Protocol.valueOf(config.getProperty(AWSConfigConstants.HTTP_PROTOCOL_VERSION));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Invalid value given for HTTP protocol. Must be HTTP1_1 or HTTP2.");
            }
        }
    }
}
