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

package org.apache.flink.connector.kinesis.table.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.table.util.AWSOptionUtils;
import org.apache.flink.connector.aws.table.util.AsyncClientOptionsUtils;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.connector.kinesis.table.KinesisPartitionKeyGeneratorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BATCH_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.SINK_FAIL_ON_ERROR;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.SINK_PARTITIONER;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.SINK_PARTITIONER_FIELD_DELIMITER;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.STREAM;

/**
 * Class for handling kinesis table options, including key mapping and validations and property
 * extraction. Class uses options decorators {@link AWSOptionUtils}, {@link AsyncClientOptionsUtils}
 * for handling each specified set of options.
 */
@Internal
public class KinesisStreamsConnectorOptionsUtils {
    /** Key for accessing kinesisAsyncClient properties. */
    public static final String KINESIS_CLIENT_PROPERTIES_KEY = "sink.client.properties";

    private final AsyncClientOptionsUtils asyncClientOptionsUtils;
    private final AsyncSinkConfigurationValidator asyncSinkconfigurationValidator;
    private final Map<String, String> resolvedOptions;
    private final ReadableConfig tableOptions;
    private final PartitionKeyGenerator<RowData> partitioner;

    /**
     * Prefixes of properties that are validated by downstream components and should not be
     * validated by the Table API infrastructure.
     */
    private static final String[] NON_VALIDATED_PREFIXES =
            new String[] {
                AWSOptionUtils.AWS_PROPERTIES_PREFIX,
                AsyncClientOptionsUtils.SINK_CLIENT_PREFIX,
                KinesisProducerOptionsMapper.KINESIS_PRODUCER_PREFIX
            };

    public KinesisStreamsConnectorOptionsUtils(
            Map<String, String> options,
            ReadableConfig tableOptions,
            RowType physicalType,
            List<String> partitionKeys,
            ClassLoader classLoader) {
        KinesisProducerOptionsMapper optionsMapper =
                new KinesisProducerOptionsMapper(tableOptions, options);
        this.resolvedOptions = optionsMapper.mapDeprecatedClientOptions();
        this.tableOptions = optionsMapper.mapDeprecatedTableOptions();
        this.asyncSinkconfigurationValidator =
                new AsyncSinkConfigurationValidator(this.tableOptions);
        this.asyncClientOptionsUtils = new AsyncClientOptionsUtils(resolvedOptions);
        this.partitioner =
                KinesisPartitionKeyGeneratorFactory.getKinesisPartitioner(
                        tableOptions, physicalType, partitionKeys, classLoader);
    }

    public Properties getValidatedSinkConfigurations() {
        Properties properties = asyncSinkconfigurationValidator.getValidatedConfigurations();
        properties.put(STREAM.key(), tableOptions.get(STREAM));
        Properties kinesisClientProps = asyncClientOptionsUtils.getValidatedConfigurations();

        properties.put(KINESIS_CLIENT_PROPERTIES_KEY, kinesisClientProps);
        properties.put(SINK_PARTITIONER.key(), this.partitioner);

        if (tableOptions.getOptional(SINK_FAIL_ON_ERROR).isPresent()) {
            properties.put(
                    SINK_FAIL_ON_ERROR.key(), tableOptions.getOptional(SINK_FAIL_ON_ERROR).get());
        }
        return properties;
    }

    public List<String> getNonValidatedPrefixes() {
        return Arrays.asList(NON_VALIDATED_PREFIXES);
    }

    /** Class for Mapping and validation of deprecated producer options. */
    @Internal
    public static class KinesisProducerOptionsMapper {
        private static final Logger LOG =
                LoggerFactory.getLogger(KinesisProducerOptionsMapper.class);
        /** prefix for deprecated producer options fallback keys. */
        public static final String KINESIS_PRODUCER_PREFIX = "sink.producer.";

        private static final String KINESIS_PRODUCER_ENDPOINT = "sink.producer.kinesis-endpoint";
        private static final String KINESIS_PRODUCER_PORT = "sink.producer.kinesis-port";
        private static final String KINESIS_PRODUCER_VERIFY_CERTIFICATE =
                "sink.producer.verify-certificate";
        private static final String DEPRECATED_FLUSH_BUFFER_TIMEOUT_KEY =
                "sink.producer.record-max-buffered-time";
        private static final String DEPRECATED_MAX_BATCH_SIZE_KEY =
                "sink.producer.collection-max-size";
        private static final String DEPRECATED_MAX_INFLIGHT_REQUESTS_KEY =
                "sink.producer.collection-max-count";
        private static final String DEPRECATED_SINK_FAIL_ON_ERROR_KEY =
                "sink.producer.fail-on-error";

        private final ReadableConfig tableOptions;
        private final Map<String, String> resolvedOptions;

        public KinesisProducerOptionsMapper(
                ReadableConfig tableOptions, Map<String, String> resolvedOptions) {
            this.tableOptions = tableOptions;
            this.resolvedOptions = resolvedOptions;
        }

        @VisibleForTesting
        public KinesisProducerOptionsMapper(Map<String, String> allOptions) {
            this.tableOptions = Configuration.fromMap(allOptions);
            this.resolvedOptions = allOptions;
        }

        public Map<String, String> mapDeprecatedClientOptions() {
            mapDeprecatedEndpoint();
            mapDeprecatedVerifyCertificate();
            removeMappedOptions();
            resolvedOptions.keySet().forEach(this::warnForDeprecatedOption);
            return resolvedOptions;
        }

        public ReadableConfig mapDeprecatedTableOptions() {
            Configuration mappedConfig = new Configuration();
            mappedConfig.set(STREAM, tableOptions.get(STREAM));
            tableOptions
                    .getOptional(FLUSH_BUFFER_SIZE)
                    .ifPresent(val -> mappedConfig.set(FLUSH_BUFFER_SIZE, val));
            tableOptions
                    .getOptional(MAX_BUFFERED_REQUESTS)
                    .ifPresent(val -> mappedConfig.set(MAX_BUFFERED_REQUESTS, val));
            tableOptions
                    .getOptional(SINK_PARTITIONER)
                    .ifPresent(val -> mappedConfig.set(SINK_PARTITIONER, val));
            tableOptions
                    .getOptional(SINK_PARTITIONER_FIELD_DELIMITER)
                    .ifPresent(val -> mappedConfig.set(SINK_PARTITIONER_FIELD_DELIMITER, val));

            replaceDeprecatedOptionInConfig(
                    FLUSH_BUFFER_TIMEOUT, DEPRECATED_FLUSH_BUFFER_TIMEOUT_KEY, mappedConfig);
            replaceDeprecatedOptionInConfig(
                    MAX_BATCH_SIZE, DEPRECATED_MAX_BATCH_SIZE_KEY, mappedConfig);
            replaceDeprecatedOptionInConfig(
                    MAX_IN_FLIGHT_REQUESTS, DEPRECATED_MAX_INFLIGHT_REQUESTS_KEY, mappedConfig);
            replaceDeprecatedOptionInConfig(
                    SINK_FAIL_ON_ERROR, DEPRECATED_SINK_FAIL_ON_ERROR_KEY, mappedConfig);

            return mappedConfig;
        }

        public static Set<ConfigOption<?>> addDeprecatedKeys(Set<ConfigOption<?>> tableOptions) {
            HashSet<ConfigOption<?>> tableOptionsWithDeprecatedKeys = new HashSet<>(tableOptions);

            tableOptionsWithDeprecatedKeys.remove(FLUSH_BUFFER_TIMEOUT);
            tableOptionsWithDeprecatedKeys.add(
                    FLUSH_BUFFER_TIMEOUT.withDeprecatedKeys(DEPRECATED_FLUSH_BUFFER_TIMEOUT_KEY));

            tableOptionsWithDeprecatedKeys.remove(MAX_BATCH_SIZE);
            tableOptionsWithDeprecatedKeys.add(
                    MAX_BATCH_SIZE.withDeprecatedKeys(DEPRECATED_MAX_BATCH_SIZE_KEY));

            tableOptionsWithDeprecatedKeys.remove(MAX_IN_FLIGHT_REQUESTS);
            tableOptionsWithDeprecatedKeys.add(
                    MAX_IN_FLIGHT_REQUESTS.withDeprecatedKeys(
                            DEPRECATED_MAX_INFLIGHT_REQUESTS_KEY));

            tableOptionsWithDeprecatedKeys.remove(SINK_FAIL_ON_ERROR);
            tableOptionsWithDeprecatedKeys.add(
                    SINK_FAIL_ON_ERROR.withDeprecatedKeys(DEPRECATED_SINK_FAIL_ON_ERROR_KEY));

            return tableOptionsWithDeprecatedKeys;
        }

        private void mapDeprecatedEndpoint() {
            if (resolvedOptions.containsKey(KINESIS_PRODUCER_ENDPOINT)) {
                if (resolvedOptions.containsKey(KINESIS_PRODUCER_PORT)) {
                    resolvedOptions.putIfAbsent(
                            AWSConfigConstants.AWS_ENDPOINT,
                            String.format(
                                    "https://%s:%s",
                                    resolvedOptions.get(KINESIS_PRODUCER_ENDPOINT),
                                    resolvedOptions.get(KINESIS_PRODUCER_PORT)));
                } else {
                    resolvedOptions.putIfAbsent(
                            AWSConfigConstants.AWS_ENDPOINT,
                            String.format(
                                    "https://%s", resolvedOptions.get(KINESIS_PRODUCER_ENDPOINT)));
                }
            }
        }

        private void mapDeprecatedVerifyCertificate() {
            if (resolvedOptions.containsKey(KINESIS_PRODUCER_VERIFY_CERTIFICATE)) {
                String value = resolvedOptions.get(KINESIS_PRODUCER_VERIFY_CERTIFICATE);
                if (value.equalsIgnoreCase("true")) {
                    resolvedOptions.putIfAbsent(AWSConfigConstants.TRUST_ALL_CERTIFICATES, "false");
                } else if (value.equalsIgnoreCase("false")) {
                    resolvedOptions.putIfAbsent(AWSConfigConstants.TRUST_ALL_CERTIFICATES, "true");
                } else {
                    LOG.warn(
                            String.format(
                                    "Option %s is ignored due to invalid value",
                                    KINESIS_PRODUCER_VERIFY_CERTIFICATE));
                }
            }
        }

        private void removeMappedOptions() {
            resolvedOptions.remove(KINESIS_PRODUCER_VERIFY_CERTIFICATE);
            resolvedOptions.remove(KINESIS_PRODUCER_ENDPOINT);
            resolvedOptions.remove(KINESIS_PRODUCER_PORT);
            resolvedOptions.remove(DEPRECATED_FLUSH_BUFFER_TIMEOUT_KEY);
            resolvedOptions.remove(DEPRECATED_MAX_BATCH_SIZE_KEY);
            resolvedOptions.remove(DEPRECATED_MAX_INFLIGHT_REQUESTS_KEY);
            resolvedOptions.remove(DEPRECATED_SINK_FAIL_ON_ERROR_KEY);
        }

        private void warnForDeprecatedOption(String key) {
            if (key.startsWith(KINESIS_PRODUCER_PREFIX)) {
                LOG.warn(String.format("Key %s is unsupported by Kinesis Datastream Sink", key));
            }
        }

        private <T> void replaceDeprecatedOptionInConfig(
                ConfigOption<T> option, String deprecatedKey, Configuration config) {
            tableOptions
                    .getOptional(option.withDeprecatedKeys(deprecatedKey))
                    .ifPresent(v -> config.set(option, v));
            tableOptions.getOptional(option).ifPresent(v -> config.set(option, v));
        }
    }
}
