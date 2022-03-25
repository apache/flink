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

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

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
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FORMAT;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_SOURCE_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.TOPIC;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validatePrimaryKeyConstraints;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validateTableSourceOptions;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/** Factory for creating configured instances of {@link PulsarTableFactory}. */
public class PulsarTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "pulsar";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();

        // validate configs are not conflict; each options is consumed; no unwanted configs
        helper.validateExcept(
                PulsarSourceOptions.CONSUMER_CONFIG_PREFIX,
                PulsarSinkOptions.PRODUCER_CONFIG_PREFIX);

        validatePrimaryKeyConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                helper);

        validateTableSourceOptions(tableOptions);

        // retrieve configs
        final List<String> topics = getTopicListFromOptions(tableOptions);
        final Optional<StartCursor> startCursorOptional = getStartCursor(tableOptions);
        final Optional<SubscriptionType> subscriptionTypeOptional =
                getSubscriptionType(tableOptions);

        final Properties properties = getPulsarProperties(tableOptions);

        // retrieve formats, physical fields (not including computed or metadata fields),
        // and projections
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

        // set default values
        final StartCursor startCursor = startCursorOptional.orElse(StartCursor.earliest());
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormatForMetadataPushdown =
                valueDecodingFormat;
        final SubscriptionType subscriptionType =
                subscriptionTypeOptional.orElse(SubscriptionType.Exclusive);
        // TODO factory not created yet
        // TODO decodingFormatForMetadataPushdown can be refactored?
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

        // validate configs are not conflict; each options is consumed; no unwanted configs
        helper.validateExcept(
                PulsarSourceOptions.CONSUMER_CONFIG_PREFIX,
                PulsarSinkOptions.PRODUCER_CONFIG_PREFIX);
        validatePrimaryKeyConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                helper);

        // retrieve configs
        final List<String> topics = getTopicListFromOptions(tableOptions);
        final Properties properties = getPulsarProperties(tableOptions);
        final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
                getKeyEncodingFormat(helper);
        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        // retrieve physical DataType (not including computed or metadata fields)
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

        // set default values
        final DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
        final int parallism = 1;
        final ChangelogMode changelogMode = valueEncodingFormat.getChangelogMode();
        // validate configs

        return new PulsarTableSink(
                physicalDataType,
                serializationSchemaFactory,
                changelogMode,
                topics,
                properties,
                deliveryGuarantee,
                parallism);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    // TODO concrete options will be decided after the required options is finished
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TOPIC);
        options.add(PULSAR_ADMIN_URL);
        options.add(PULSAR_SERVICE_URL);
        options.add(FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SOURCE_SOURCE_SUBSCRIPTION_TYPE);
        options.add(SOURCE_START_FROM_MESSAGE_ID);
        options.add(SOURCE_START_FROM_PUBLISH_TIME);
        options.add(SINK_PARALLELISM);
        options.add(KEY_FORMAT);
        options.add(KEY_FIELDS);
        return options;
    }
    // TODO investigate forward options
}
