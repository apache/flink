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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.base.table.options.ConfigurationValidator;
import org.apache.flink.connector.base.table.options.TableOptionsUtils;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;
import org.apache.flink.connector.kinesis.sink.KinesisDataStreamsSinkElementConverter;
import org.apache.flink.connectors.kinesis.table.KinesisPartitionKeyGeneratorFactory;
import org.apache.flink.connectors.kinesis.table.utils.AWSOptionsUtils;
import org.apache.flink.connectors.kinesis.table.utils.KinesisClientOptionsUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connectors.kinesis.table.KinesisConnectorOptions.FLUSH_BUFFER_SIZE;
import static org.apache.flink.connectors.kinesis.table.KinesisConnectorOptions.FLUSH_BUFFER_TIMEOUT;
import static org.apache.flink.connectors.kinesis.table.KinesisConnectorOptions.MAX_BATCH_SIZE;
import static org.apache.flink.connectors.kinesis.table.KinesisConnectorOptions.MAX_BUFFERED_REQUESTS;
import static org.apache.flink.connectors.kinesis.table.KinesisConnectorOptions.MAX_IN_FLIGHT_REQUESTS;
import static org.apache.flink.connectors.kinesis.table.KinesisConnectorOptions.SINK_FAIL_ON_ERROR;
import static org.apache.flink.connectors.kinesis.table.KinesisConnectorOptions.SINK_PARTITIONER;
import static org.apache.flink.connectors.kinesis.table.KinesisConnectorOptions.SINK_PARTITIONER_FIELD_DELIMITER;
import static org.apache.flink.connectors.kinesis.table.KinesisConnectorOptions.STREAM;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/**
 * Class for handling kinesis table options, including key mapping and validations and property
 * extraction. Class uses options decorators {@link AWSOptionsUtils}, {@link
 * KinesisClientOptionsUtils} and {@link KinesisConsumerOptionsUtils} for handling each specified
 * set of options.
 */
@Internal
public class KinesisConnectorOptionsUtils extends AsyncSinkConfigurationValidator {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisConnectorOptionsUtils.class);
    private final AWSOptionsUtils awsOptionsUtils;
    private final KinesisClientOptionsUtils kinesisClientOptionsUtils;
    private final KinesisConsumerOptionsUtils kinesisConsumerOptionsUtils;
    private final KinesisProducerOptionsMapper kinesisProducerOptionsMapper;
    private final Map<String, String> resolvedOptions;
    private final ReadableConfig tableOptions;
    private final KinesisDataStreamsSinkElementConverter.PartitionKeyGenerator<RowData> partitioner;

    public static final String KINESIS_CLIENT_PROPERTIES_KEY = "sink.client.properties";

    /** Options handled and validated by the table-level layer. */
    private static final Set<String> TABLE_LEVEL_OPTIONS =
            new HashSet<>(
                    Arrays.asList(
                            STREAM.key(),
                            FORMAT.key(),
                            SINK_PARTITIONER.key(),
                            SINK_FAIL_ON_ERROR.key(),
                            SINK_PARTITIONER_FIELD_DELIMITER.key(),
                            FLUSH_BUFFER_SIZE.key(),
                            FLUSH_BUFFER_TIMEOUT.key(),
                            MAX_BATCH_SIZE.key(),
                            MAX_BUFFERED_REQUESTS.key(),
                            MAX_IN_FLIGHT_REQUESTS.key()));

    /**
     * Prefixes of properties that are validated by downstream components and should not be
     * validated by the Table API infrastructure.
     */
    private static final String[] NON_VALIDATED_PREFIXES =
            new String[] {
                AWSOptionsUtils.AWS_PROPERTIES_PREFIX,
                KinesisClientOptionsUtils.SINK_CLIENT_PREFIX,
                KinesisConsumerOptionsUtils.CONSUMER_PREFIX,
                KinesisProducerOptionsMapper.KINESIS_PRODUCER_PREFIX
            };

    public KinesisConnectorOptionsUtils(
            Map<String, String> options,
            ReadableConfig tableOptions,
            RowType physicalType,
            List<String> partitionKeys,
            ClassLoader classLoader) {
        super(tableOptions);
        this.resolvedOptions = new HashMap<>();
        // filtering out Table level options as they are handled by factory utils.
        options.entrySet().stream()
                .filter(entry -> !TABLE_LEVEL_OPTIONS.contains(entry.getKey()))
                .forEach(entry -> resolvedOptions.put(entry.getKey(), entry.getValue()));

        this.tableOptions = tableOptions;
        this.awsOptionsUtils = new AWSOptionsUtils(resolvedOptions);
        this.kinesisClientOptionsUtils = new KinesisClientOptionsUtils(resolvedOptions);
        this.kinesisConsumerOptionsUtils =
                new KinesisConsumerOptionsUtils(resolvedOptions, tableOptions.get(STREAM));
        this.kinesisProducerOptionsMapper = new KinesisProducerOptionsMapper(resolvedOptions);
        this.partitioner =
                KinesisPartitionKeyGeneratorFactory.getKinesisPartitioner(
                        tableOptions, physicalType, partitionKeys, classLoader);
    }

    public Properties getValidatedSourceConfigurations() {
        return kinesisConsumerOptionsUtils.getValidatedConfigurations();
    }

    public Properties getValidatedSinkConfigurations() {
        Properties properties = super.getValidatedConfigurations();
        properties.put(STREAM.key(), tableOptions.get(STREAM));
        Properties awsProps = awsOptionsUtils.getValidatedConfigurations();
        Properties kinesisClientProps = kinesisClientOptionsUtils.getValidatedConfigurations();
        Properties producerFallbackProperties =
                kinesisProducerOptionsMapper.getValidatedConfigurations();

        for (Map.Entry<Object, Object> entry : awsProps.entrySet()) {
            if (!properties.containsKey(entry.getKey())) {
                kinesisClientProps.put(entry.getKey(), entry.getValue());
            }
        }

        for (Map.Entry<Object, Object> entry : producerFallbackProperties.entrySet()) {
            if (!properties.containsKey(entry.getKey())) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }

        properties.put(KINESIS_CLIENT_PROPERTIES_KEY, kinesisClientProps);
        properties.put(SINK_PARTITIONER.key(), this.partitioner);

        if (tableOptions.getOptional(SINK_FAIL_ON_ERROR).isPresent()) {
            properties.put(
                    SINK_FAIL_ON_ERROR.key(), tableOptions.getOptional(SINK_FAIL_ON_ERROR).get());
        }
        if (!awsProps.containsKey(AWSConfigConstants.AWS_REGION)) {
            // per requirement in Amazon Kinesis DataStream
            throw new IllegalArgumentException(
                    String.format(
                            "For FlinkKinesisSink AWS region ('%s') must be set in the config.",
                            AWSConfigConstants.AWS_REGION));
        }
        return properties;
    }

    public List<String> getNonValidatedPrefixes() {
        return Arrays.asList(NON_VALIDATED_PREFIXES);
    }

    @Override
    public Properties getValidatedConfigurations() {
        Properties properties = getValidatedSourceConfigurations();
        Properties sinkProperties = getValidatedSinkConfigurations();
        for (Map.Entry<Object, Object> entry : sinkProperties.entrySet()) {
            if (!properties.containsKey(entry.getKey())) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }
        return properties;
    }

    /** Class for Mapping and validation of deprecated producer options. */
    @Internal
    public static class KinesisProducerOptionsMapper
            implements TableOptionsUtils, ConfigurationValidator {
        private static final String KINESIS_PRODUCER_PREFIX = "sink.producer.";
        private static final Map<String, String> kinesisProducerFallbackKeys = new HashMap<>();
        private static final Set<String> kinesisProducerSkipKeys = new HashSet<>();
        private static final String KINESIS_PRODUCER_ENDPOINT = "sink.producer.kinesis-endpoint";
        private static final String KINESIS_PRODUCER_PORT = "sink.producer.kinesis-port";
        private static final String KINESIS_PRODUCER_AGGREGATION =
                "sink.producer.aggregation-enabled";
        private static final String KINESIS_PRODUCER_VERIFY_CERTIFICATE =
                "sink.producer.verify-certificate";

        static {
            kinesisProducerFallbackKeys.put(
                    "sink.producer.record-max-buffered-time", FLUSH_BUFFER_TIMEOUT.key());
            kinesisProducerFallbackKeys.put(
                    "sink.producer.collection-max-size", MAX_BATCH_SIZE.key());
            kinesisProducerFallbackKeys.put(
                    "sink.producer.collection-max-count", MAX_IN_FLIGHT_REQUESTS.key());
            kinesisProducerFallbackKeys.put(
                    "sink.producer.fail-on-error", SINK_FAIL_ON_ERROR.key());
            kinesisProducerSkipKeys.add(KINESIS_PRODUCER_ENDPOINT);
            kinesisProducerSkipKeys.add(KINESIS_PRODUCER_PORT);
            kinesisProducerSkipKeys.add(KINESIS_PRODUCER_AGGREGATION);
            kinesisProducerSkipKeys.add(KINESIS_PRODUCER_VERIFY_CERTIFICATE);
        }

        private final Map<String, String> resolvedOptions;

        public KinesisProducerOptionsMapper(Map<String, String> resolvedOptions) {
            this.resolvedOptions = resolvedOptions;
        }

        @Override
        public Properties getValidatedConfigurations() {
            Properties properties = new Properties();
            properties.putAll(getProcessedResolvedOptions());

            Optional.ofNullable(properties.getProperty(FLUSH_BUFFER_TIMEOUT.key()))
                    .ifPresent(
                            key -> {
                                ConfigurationValidator.validateOptionalPositiveLongProperty(
                                        properties,
                                        FLUSH_BUFFER_TIMEOUT.key(),
                                        "Invalid option flush buffer size. Must be a positive integer");
                                properties.put(FLUSH_BUFFER_TIMEOUT.key(), Long.parseLong(key));
                            });

            Optional.ofNullable(properties.getProperty(MAX_BATCH_SIZE.key()))
                    .ifPresent(
                            key -> {
                                ConfigurationValidator.validateOptionalPositiveIntProperty(
                                        properties,
                                        MAX_BATCH_SIZE.key(),
                                        "Invalid option batch size. Must be a positive integer");
                                properties.put(MAX_BATCH_SIZE.key(), Integer.parseInt(key));
                            });

            Optional.ofNullable(properties.getProperty(MAX_IN_FLIGHT_REQUESTS.key()))
                    .ifPresent(
                            key -> {
                                ConfigurationValidator.validateOptionalPositiveIntProperty(
                                        properties,
                                        MAX_IN_FLIGHT_REQUESTS.key(),
                                        "Invalid option maximum inflight requests. Must be a positive integer");
                                properties.put(MAX_IN_FLIGHT_REQUESTS.key(), Integer.parseInt(key));
                            });

            Optional.ofNullable(properties.getProperty(SINK_FAIL_ON_ERROR.key()))
                    .ifPresent(
                            key -> {
                                ConfigurationValidator.validateOptionalBooleanProperty(
                                        properties,
                                        SINK_FAIL_ON_ERROR.key(),
                                        "Invalid option fail on error. Must be a boolean");
                                properties.put(SINK_FAIL_ON_ERROR.key(), Boolean.parseBoolean(key));
                            });

            return properties;
        }

        @Override
        public Map<String, String> getProcessedResolvedOptions() {
            Map<String, String> processedResolvedOptions = new HashMap<>();
            for (String key : resolvedOptions.keySet()) {
                if (key.startsWith(KINESIS_PRODUCER_PREFIX)) {
                    Optional.ofNullable(kinesisProducerFallbackKeys.get(key))
                            .ifPresent(
                                    mappedKey ->
                                            processedResolvedOptions.put(
                                                    mappedKey, resolvedOptions.get(key)));
                    if (!kinesisProducerFallbackKeys.containsKey(key)
                            && !kinesisProducerSkipKeys.contains(key)) {
                        LOG.warn(
                                String.format(
                                        "Key %s is unsupported by Kinesis Datastream Sink", key));
                    }
                }
            }

            // reconstructing end-point from legacy end-point and port
            if (resolvedOptions.containsKey(KINESIS_PRODUCER_ENDPOINT)) {
                if (resolvedOptions.containsKey(KINESIS_PRODUCER_PORT)) {
                    processedResolvedOptions.putIfAbsent(
                            AWSConfigConstants.AWS_ENDPOINT,
                            String.format(
                                    "https://%s:%s",
                                    resolvedOptions.get(KINESIS_PRODUCER_ENDPOINT),
                                    resolvedOptions.get(KINESIS_PRODUCER_PORT)));
                } else {
                    processedResolvedOptions.putIfAbsent(
                            AWSConfigConstants.AWS_ENDPOINT,
                            String.format(
                                    "https://%s", resolvedOptions.get(KINESIS_PRODUCER_ENDPOINT)));
                }
            }

            // transforming verify certificate options
            if (resolvedOptions.containsKey(KINESIS_PRODUCER_VERIFY_CERTIFICATE)) {
                String value = resolvedOptions.get(KINESIS_PRODUCER_VERIFY_CERTIFICATE);
                if (value.equalsIgnoreCase("true")) {
                    processedResolvedOptions.putIfAbsent(
                            AWSConfigConstants.TRUST_ALL_CERTIFICATES, "false");
                } else if (value.equalsIgnoreCase("false")) {
                    processedResolvedOptions.putIfAbsent(
                            AWSConfigConstants.TRUST_ALL_CERTIFICATES, "true");
                } else {
                    LOG.warn(
                            String.format(
                                    "Option %s is ignored due to invalid value",
                                    KINESIS_PRODUCER_VERIFY_CERTIFICATE));
                }
            }

            // transform deprecated aggregation disabling
            if (resolvedOptions.containsKey(KINESIS_PRODUCER_AGGREGATION)) {
                if (resolvedOptions.get(KINESIS_PRODUCER_AGGREGATION).equalsIgnoreCase("false")) {
                    processedResolvedOptions.putIfAbsent(MAX_BATCH_SIZE.key(), "1");
                } else {
                    LOG.warn(
                            String.format(
                                    "Key %s is unsupported by Kinesis Datastream Sink",
                                    KINESIS_PRODUCER_AGGREGATION));
                }
            }

            return processedResolvedOptions;
        }

        @Override
        public List<String> getNonValidatedPrefixes() {
            return Collections.singletonList(KINESIS_PRODUCER_PREFIX);
        }
    }
}
