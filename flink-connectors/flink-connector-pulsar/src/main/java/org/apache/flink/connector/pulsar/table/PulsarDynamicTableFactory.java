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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.common.naming.TopicName;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.ADMIN_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.GENERIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.PARTITION_DISCOVERY_INTERVAL_MILLIS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.PROPERTIES;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.PROPERTIES_PREFIX;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SCAN_STARTUP_SUB_NAME;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SCAN_STARTUP_SUB_START_OFFSET;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SERVICE_URL;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SINK_MESSAGE_ROUTER;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.SINK_SEMANTIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.TOPIC;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.TOPIC_PATTERN;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.VALUE_FIELDS_INCLUDE;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.VALUE_FORMAT;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.createKeyFormatProjection;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.createValueFormatProjection;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.getMessageRouter;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.getPulsarProperties;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.validateSinkMessageRouter;
import static org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions.validateTableSourceOptions;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.apache.flink.types.RowKind.INSERT;

/** Factory for creating configured instances of {@link PulsarDynamicTableFactory}. */
public class PulsarDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "pulsar";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();

        List<String> topics = generateTopic(context.getObjectIdentifier(), tableOptions);
        if (topics != null && !topics.isEmpty()) {
            ((Configuration) tableOptions).set(TOPIC, Collections.singletonList(topics.get(0)));
        }

        String adminUrl = tableOptions.get(ADMIN_URL);
        String serverUrl = tableOptions.get(SERVICE_URL);
        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                getKeyEncodingFormat(helper);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        // Validate the option data type.
        helper.validateExcept(
                PROPERTIES_PREFIX, "type", "table-default-partitions", "default-database");
        // Validate the option values.
        PulsarTableOptions.validateTableSinkOptions(tableOptions);

        Properties properties = getPulsarProperties(context.getCatalogTable().toProperties());
        validatePKConstraints(
                context.getObjectIdentifier(), context.getCatalogTable(), valueEncodingFormat);

        final DataType physicalDataType =
                context.getCatalogTable().getSchema().toPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        return createPulsarTableSink(
                tableOptions,
                topics,
                adminUrl,
                serverUrl,
                keyEncodingFormat,
                valueEncodingFormat,
                properties,
                physicalDataType,
                keyProjection,
                valueProjection,
                keyPrefix,
                context);
    }

    private PulsarDynamicTableSink createPulsarTableSink(
            ReadableConfig tableOptions,
            List<String> topics,
            String adminUrl,
            String serverUrl,
            Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            Properties properties,
            DataType physicalDataType,
            int[] keyProjection,
            int[] valueProjection,
            String keyPrefix,
            Context context) {

        final String formatType =
                tableOptions.getOptional(FORMAT).orElseGet(() -> tableOptions.get(VALUE_FORMAT));
        final Integer parallelism = tableOptions.getOptional(SINK_PARALLELISM).orElse(null);
        return new PulsarDynamicTableSink(
                serverUrl,
                adminUrl,
                topics.get(0),
                physicalDataType,
                properties,
                keyEncodingFormat.orElse(null),
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                PulsarTableOptions.getSinkSemantic(tableOptions),
                formatType,
                false,
                parallelism,
                getMessageRouter(tableOptions, context.getClassLoader()).orElse(null));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();

        List<String> topics = generateTopic(context.getObjectIdentifier(), tableOptions);
        if (topics != null && !topics.isEmpty()) {
            ((Configuration) tableOptions).set(TOPIC, Collections.singletonList(topics.get(0)));
        }

        // Generic Flink Table can reference multiple topics with topicPattern
        String topicPattern = tableOptions.getOptional(TOPIC_PATTERN).orElse(null);
        if (topicPattern != null && !topicPattern.isEmpty()) {
            ((Configuration) tableOptions).set(TOPIC_PATTERN, topicPattern);
        }

        String adminUrl = tableOptions.get(ADMIN_URL);
        String serviceUrl = tableOptions.get(SERVICE_URL);

        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                getKeyDecodingFormat(helper);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);
        // Validate the option data type.
        helper.validateExcept(
                PROPERTIES_PREFIX, "type", "table-default-partitions", "default-database");
        // Validate the option values.
        validateTableSourceOptions(tableOptions);
        validateSinkMessageRouter(tableOptions);
        validatePKConstraints(
                context.getObjectIdentifier(), context.getCatalogTable(), valueDecodingFormat);

        Properties properties = getPulsarProperties(context.getCatalogTable().toProperties());

        final PulsarTableOptions.StartupOptions startupOptions =
                PulsarTableOptions.getStartupOptions(tableOptions);

        final DataType physicalDataType =
                context.getCatalogTable().getSchema().toPhysicalRowDataType();



        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        return createPulsarTableSource(
                physicalDataType,
                keyDecodingFormat.orElse(null),
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topics,
                topicPattern,
                serviceUrl,
                adminUrl,
                properties,
                startupOptions);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SERVICE_URL);
        options.add(ADMIN_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FORMAT);
        options.add(KEY_FORMAT);
        options.add(KEY_FIELDS);
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FORMAT);
        options.add(VALUE_FIELDS_INCLUDE);
        options.add(TOPIC);
        options.add(TOPIC_PATTERN);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_STARTUP_SUB_NAME);
        options.add(SCAN_STARTUP_SUB_START_OFFSET);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(GENERIC);

        options.add(PARTITION_DISCOVERY_INTERVAL_MILLIS);
        options.add(SINK_SEMANTIC);
        options.add(SINK_MESSAGE_ROUTER);
        options.add(SINK_PARALLELISM);
        options.add(PROPERTIES);
        return options;
    }

    // --------------------------------------------------------------------------------------------

    private List<String> generateTopic(ObjectIdentifier table, ReadableConfig tableOptions) {
        List<String> topics = null;
        if (tableOptions.get(GENERIC)) {
            topics = tableOptions.getOptional(TOPIC).orElse(null);
        } else {
            String rawTopic = table.getDatabaseName() + "/" + table.getObjectName();
            final String topic = TopicName.get(rawTopic).toString();
            topics = Collections.singletonList(topic);
        }

        return topics;
    }

    private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getKeyDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, KEY_FORMAT);
        keyDecodingFormat.ifPresent(
                format -> {
                    if (!format.getChangelogMode().containsOnly(INSERT)) {
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
            FactoryUtil.TableFactoryHelper helper) {
        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, KEY_FORMAT);
        keyEncodingFormat.ifPresent(
                format -> {
                    if (!format.getChangelogMode().containsOnly(INSERT)) {
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
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverDecodingFormat(
                                        DeserializationFormatFactory.class, VALUE_FORMAT));
    }

    private static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverEncodingFormat(
                                        SerializationFormatFactory.class, VALUE_FORMAT));
    }

    private static void validatePKConstraints(
            ObjectIdentifier tableName, CatalogTable catalogTable, Format format) {
        if (catalogTable.getSchema().getPrimaryKey().isPresent()
                && format.getChangelogMode().containsOnly(INSERT)) {
            Configuration options = Configuration.fromMap(catalogTable.getOptions());
            String formatName = options.getOptional(FORMAT).orElse(options.get(VALUE_FORMAT));
            throw new ValidationException(
                    String.format(
                            "The Pulsar table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint"
                                    + " on the table, because it can't guarantee the semantic of primary key.",
                            tableName.asSummaryString(), formatName));
        }
    }

    // --------------------------------------------------------------------------------------------
    protected org.apache.flink.streaming.connectors.pulsar.table.PulsarDynamicTableSource createPulsarTableSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            @Nullable List<String> topics,
            @Nullable String topicPattern,
            String serviceUrl,
            String adminUrl,
            Properties properties,
            PulsarTableOptions.StartupOptions startupOptions) {
        return new org.apache.flink.streaming.connectors.pulsar.table.PulsarDynamicTableSource(
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topics,
                topicPattern,
                serviceUrl,
                adminUrl,
                properties,
                startupOptions,
                false);
    }
}
