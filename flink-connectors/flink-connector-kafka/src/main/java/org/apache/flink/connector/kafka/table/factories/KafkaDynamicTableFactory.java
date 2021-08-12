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

package org.apache.flink.connector.kafka.table.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SinkSemantic;
import org.apache.flink.connector.kafka.table.sink.KafkaDynamicSink;
import org.apache.flink.connector.kafka.table.source.KafkaDynamicTableSource;
import org.apache.flink.connector.kafka.table.utils.SinkBufferFlushMode;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.BOUNDEDNESS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.KEY_FIELDS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.KEY_FORMAT;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.PARTITIONS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.PROPS_GROUP_ID;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STOP_MODE;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STOP_SPECIFIC_OFFSETS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_STOP_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SCAN_TOPIC_PARTITION_DISCOVERY;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SINK_PARTITIONER;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SINK_SEMANTIC;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.TOPIC_PATTERN;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.VALUE_FIELDS_INCLUDE;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.PROPERTIES_PREFIX;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.StartOrStop.START;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.StartOrStop.STOP;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.autoCompleteSchemaRegistrySubject;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.createKeyFormatProjection;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.createValueFormatProjection;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.getBoundedness;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.getFlinkKafkaPartitioner;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.getKafkaProperties;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.getOffsetsInitializer;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.getTopicPartitionSubscription;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.validatePKConstraints;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.validateTableSinkOptions;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.validateTableSourceOptions;

/**
 * Factory for creating configured instances of {@link KafkaDynamicTableSource} and {@link
 * KafkaDynamicSink}.
 */
@Internal
public class KafkaDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "kafka";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPS_BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        options.add(KEY_FORMAT);
        options.add(KEY_FIELDS);
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FORMAT);
        options.add(VALUE_FIELDS_INCLUDE);
        options.add(TOPIC);
        options.add(TOPIC_PATTERN);
        options.add(PARTITIONS);
        options.add(PROPS_GROUP_ID);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_TOPIC_PARTITION_DISCOVERY);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SCAN_STOP_MODE);
        options.add(SCAN_STOP_SPECIFIC_OFFSETS);
        options.add(SCAN_STOP_TIMESTAMP_MILLIS);
        options.add(BOUNDEDNESS);
        options.add(SINK_PARTITIONER);
        options.add(SINK_SEMANTIC);
        options.add(SINK_PARALLELISM);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        // Parse and extract table options
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();

        // Message deserialization
        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                getKeyDecodingFormat(helper);
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);

        // Validate table options
        helper.validateExcept(PROPERTIES_PREFIX);
        validateTableSourceOptions(tableOptions);
        validatePKConstraints(
                context.getObjectIdentifier(), context.getCatalogTable(), valueDecodingFormat);

        // Start and stop offsets initializer
        final OffsetsInitializer startingOffsetsInitializer =
                getOffsetsInitializer(tableOptions, START);
        final OffsetsInitializer stoppingOffsetsInitializer =
                getOffsetsInitializer(tableOptions, STOP);

        // Boundedness
        final Boundedness boundedness = getBoundedness(tableOptions);

        // Subscribing topic partitions
        KafkaDynamicTableSource.TopicPartitionSubscription topicPartitionSubscription =
                getTopicPartitionSubscription(tableOptions);

        // Additional Kafka properties
        final Properties properties = getKafkaProperties(context.getCatalogTable().getOptions());

        // Periodically topic partition discovery
        properties.setProperty(
                KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                String.valueOf(
                        tableOptions
                                .getOptional(SCAN_TOPIC_PARTITION_DISCOVERY)
                                .map(Duration::toMillis)
                                .orElse(-1L)));

        // Key and value format projection
        final DataType physicalDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);
        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        // Key field prefix
        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        // Create dynamic source
        return createKafkaTableSource(
                physicalDataType,
                keyDecodingFormat.orElse(null),
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topicPartitionSubscription,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                boundedness,
                properties,
                context.getObjectIdentifier().toObjectPath());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        this, autoCompleteSchemaRegistrySubject(context));

        final ReadableConfig tableOptions = helper.getOptions();

        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                getKeyEncodingFormat(helper);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        helper.validateExcept(PROPERTIES_PREFIX);

        validateTableSinkOptions(tableOptions);

        validatePKConstraints(
                context.getObjectIdentifier(), context.getCatalogTable(), valueEncodingFormat);

        final DataType physicalDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        final Integer parallelism = tableOptions.getOptional(SINK_PARALLELISM).orElse(null);

        return createKafkaTableSink(
                physicalDataType,
                keyEncodingFormat.orElse(null),
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                tableOptions.get(TOPIC).get(0),
                getKafkaProperties(context.getCatalogTable().getOptions()),
                getFlinkKafkaPartitioner(tableOptions, context.getClassLoader()).orElse(null),
                tableOptions.get(SINK_SEMANTIC),
                parallelism);
    }

    // --------------------------------------------------------------------------------------------

    private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getKeyDecodingFormat(
            TableFactoryHelper helper) {
        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, KEY_FORMAT);
        keyDecodingFormat.ifPresent(
                format -> {
                    if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
                        throw new ValidationException(
                                String.format(
                                        "A key format should only deal with INSERT-only records. "
                                                + "But %s has a changelog mode of %s.",
                                        helper.getOptions().get(KEY_FORMAT),
                                        format.getChangelogMode()));
                    }
                });
        return keyDecodingFormat;
    }

    private static Optional<EncodingFormat<SerializationSchema<RowData>>> getKeyEncodingFormat(
            TableFactoryHelper helper) {
        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, KEY_FORMAT);
        keyEncodingFormat.ifPresent(
                format -> {
                    if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
                        throw new ValidationException(
                                String.format(
                                        "A key format should only deal with INSERT-only records. "
                                                + "But %s has a changelog mode of %s.",
                                        helper.getOptions().get(KEY_FORMAT),
                                        format.getChangelogMode()));
                    }
                });
        return keyEncodingFormat;
    }

    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverDecodingFormat(
                                        DeserializationFormatFactory.class, VALUE_FORMAT));
    }

    private static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            TableFactoryHelper helper) {
        return helper.discoverOptionalEncodingFormat(
                        SerializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverEncodingFormat(
                                        SerializationFormatFactory.class, VALUE_FORMAT));
    }

    // --------------------------------------------------------------------------------------------

    protected KafkaDynamicTableSource createKafkaTableSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            KafkaDynamicTableSource.TopicPartitionSubscription topicPartitionSubscription,
            OffsetsInitializer startingOffsetsInitializer,
            @Nullable OffsetsInitializer stoppingOffsetsInitializer,
            Boundedness boundedness,
            Properties properties,
            ObjectPath objectPath) {

        return new KafkaDynamicTableSource(
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topicPartitionSubscription,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                boundedness,
                properties,
                objectPath,
                false);
    }

    protected KafkaDynamicSink createKafkaTableSink(
            DataType physicalDataType,
            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            String topic,
            Properties properties,
            FlinkKafkaPartitioner<RowData> partitioner,
            SinkSemantic semantic,
            Integer parallelism) {
        return new KafkaDynamicSink(
                physicalDataType,
                physicalDataType,
                keyEncodingFormat,
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topic,
                properties,
                partitioner,
                semantic,
                false,
                SinkBufferFlushMode.DISABLED,
                parallelism);
    }
}
