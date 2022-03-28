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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.sink.PulsarSinkOptions;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.table.sink.PulsarTableSerializationSchemaFactory;
import org.apache.flink.connector.pulsar.table.sink.PulsarTableSink;
import org.apache.flink.connector.pulsar.table.source.PulsarTableDeserializationSchemaFactory;
import org.apache.flink.connector.pulsar.table.source.PulsarTableSource;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.api.SubscriptionType;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.createKeyFormatProjection;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.createValueFormatProjection;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getKeyDecodingFormat;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getKeyEncodingFormat;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getPulsarProperties;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getStartCursor;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getSubscriptionType;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getTopicListFromOptions;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getValueDecodingFormat;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getValueEncodingFormat;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.ADMIN_URL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FORMAT;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SERVICE_URL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_SOURCE_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.TOPIC;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validatePrimaryKeyConstraints;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validateTableSourceOptions;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/**
 * Factory for creating {@link DynamicTableSource} and {@link DynamicTableSink}.
 *
 * <p>The main role of this class is to retrieve config options and validate options from config and
 * the table schema. It also sets default values if a config option is not present.
 */
public class PulsarTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "pulsar";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();

        // Validate configs are not conflict; each options is consumed; no unwanted configs
        // PulsarOptions, PulsarSourceOptions and PulsarSinkOptions is not part of the validation.
        helper.validateExcept(
                PulsarOptions.CLIENT_CONFIG_PREFIX,
                PulsarOptions.ADMIN_CONFIG_PREFIX,
                PulsarSourceOptions.SOURCE_CONFIG_PREFIX,
                PulsarSourceOptions.CONSUMER_CONFIG_PREFIX,
                PulsarSinkOptions.PRODUCER_CONFIG_PREFIX,
                PulsarSinkOptions.SINK_CONFIG_PREFIX);

        validatePrimaryKeyConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                helper);

        validateTableSourceOptions(tableOptions);

        // Retrieve configs
        final List<String> topics = getTopicListFromOptions(tableOptions);
        final Optional<StartCursor> startCursorOptional = getStartCursor(tableOptions);
        final Optional<SubscriptionType> subscriptionTypeOptional =
                getSubscriptionType(tableOptions);

        final Properties properties = getPulsarProperties(tableOptions);
        properties.setProperty(PULSAR_ADMIN_URL.key(), tableOptions.get(ADMIN_URL));
        properties.setProperty(PULSAR_SERVICE_URL.key(), tableOptions.get(SERVICE_URL));
        // Retrieve formats, physical fields (not including computed or metadata fields),
        // and projections and create a schema factory based on such information.
        final DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat =
                getKeyDecodingFormat(helper);
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);

        final DataType physicalDataType = context.getPhysicalRowDataType();

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);
        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final PulsarTableDeserializationSchemaFactory deserializationSchemaFactory =
                new PulsarTableDeserializationSchemaFactory(
                        physicalDataType,
                        keyDecodingFormat,
                        keyProjection,
                        valueDecodingFormat,
                        valueProjection);

        // Set default values.
        final StartCursor startCursor = startCursorOptional.orElse(StartCursor.earliest());
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormatForMetadataPushdown =
                valueDecodingFormat;
        final SubscriptionType subscriptionType =
                subscriptionTypeOptional.orElse(SubscriptionType.Exclusive);

        return new PulsarTableSource(
                deserializationSchemaFactory,
                decodingFormatForMetadataPushdown,
                topics,
                properties,
                startCursor,
                subscriptionType);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();

        // Validate configs are not conflict; each options is consumed; no unwanted configs
        // Validate configs are not conflict; each options is consumed; no unwanted configs
        // PulsarOptions, PulsarSourceOptions and PulsarSinkOptions is not part of the validation.
        helper.validateExcept(
                PulsarOptions.CLIENT_CONFIG_PREFIX,
                PulsarOptions.ADMIN_CONFIG_PREFIX,
                PulsarSourceOptions.SOURCE_CONFIG_PREFIX,
                PulsarSourceOptions.CONSUMER_CONFIG_PREFIX,
                PulsarSinkOptions.PRODUCER_CONFIG_PREFIX,
                PulsarSinkOptions.SINK_CONFIG_PREFIX);

        validatePrimaryKeyConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                helper);

        // Retrieve configs
        final List<String> topics = getTopicListFromOptions(tableOptions);
        final Properties properties = getPulsarProperties(tableOptions);
        properties.setProperty(PULSAR_ADMIN_URL.key(), tableOptions.get(ADMIN_URL));
        properties.setProperty(PULSAR_SERVICE_URL.key(), tableOptions.get(SERVICE_URL));

        final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
                getKeyEncodingFormat(helper);
        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        // Retrieve physical DataType (not including computed or metadata fields)
        final DataType physicalDataType = context.getPhysicalRowDataType();
        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);
        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final PulsarTableSerializationSchemaFactory serializationSchemaFactory =
                new PulsarTableSerializationSchemaFactory(
                        physicalDataType,
                        keyEncodingFormat,
                        keyProjection,
                        valueEncodingFormat,
                        valueProjection);

        // Set default values.
        final DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
        final int parallelism = 1;
        final ChangelogMode changelogMode = valueEncodingFormat.getChangelogMode();

        return new PulsarTableSink(
                physicalDataType,
                serializationSchemaFactory,
                changelogMode,
                topics,
                properties,
                deliveryGuarantee,
                parallelism);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(TOPIC, ADMIN_URL, SERVICE_URL, FORMAT).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                        SOURCE_SOURCE_SUBSCRIPTION_TYPE,
                        SOURCE_START_FROM_MESSAGE_ID,
                        SOURCE_START_FROM_PUBLISH_TIME,
                        SINK_PARALLELISM,
                        KEY_FORMAT,
                        KEY_FIELDS)
                .collect(Collectors.toSet());
    }

    /**
     * Format and Delivery guarantee related options are not forward options.
     *
     * @return
     */
    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        TOPIC,
                        ADMIN_URL,
                        SERVICE_URL,
                        SOURCE_SOURCE_SUBSCRIPTION_TYPE,
                        SOURCE_START_FROM_MESSAGE_ID,
                        SOURCE_START_FROM_PUBLISH_TIME)
                .collect(Collectors.toSet());
    }
}
