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
import org.apache.flink.connector.pulsar.table.sink.PulsarDynamicTableSink;
import org.apache.flink.connector.pulsar.table.sink.impl.PulsarSerializationSchemaFactory;
import org.apache.flink.connector.pulsar.table.source.PulsarDynamicTableSource;
import org.apache.flink.connector.pulsar.table.source.impl.PulsarDeserializationSchemaFactory;
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
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getTopicListFromOptions;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getValueDecodingFormat;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getValueEncodingFormat;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FORMAT;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.TOPIC;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.getPulsarProperties;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.getStartCursor;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/** Factory for creating configured instances of {@link PulsarDynamicTableFactory}. */
public class PulsarDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "pulsar";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();

        // validate configs are not conflict; each options is consumed; no unwanted configs
        helper.validateExcept(
                PulsarSourceOptions.CONSUMER_CONFIG_PREFIX,
                PulsarSinkOptions.PRODUCER_CONFIG_PREFIX);
        validatePrimaryKey();

        // TODO validate configs

        // retrieve configs
        final List<String> topics = getTopicListFromOptions(tableOptions);
        final Optional<StartCursor> startCursorOptional = getStartCursor(tableOptions);
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

        final PulsarDeserializationSchemaFactory deserializationSchemaFactory =
                new PulsarDeserializationSchemaFactory(
                        physicalDataType,
                        keyDecodingFormat,
                        keyProjection,
                        valueDecodingFormat,
                        valueProjection);

        // set default values
        final StartCursor startCursor = startCursorOptional.orElse(StartCursor.earliest());
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormatForMetadataPushdown =
                valueDecodingFormat;

        // TODO factory not created yet
        // TODO decodingFormatForMetadataPushdown can be refactored?
        return new PulsarDynamicTableSource(
                deserializationSchemaFactory,
                decodingFormatForMetadataPushdown,
                topics,
                properties,
                startCursor);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();

        // validate configs are not conflict; each options is consumed; no unwanted configs
        helper.validateExcept(
                PulsarSourceOptions.CONSUMER_CONFIG_PREFIX,
                PulsarSinkOptions.PRODUCER_CONFIG_PREFIX);
        validatePrimaryKey();

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

        final PulsarSerializationSchemaFactory serializationSchemaFactory = new PulsarSerializationSchemaFactory(
                physicalDataType,
                keyEncodingFormat,
                keyProjection,
                valueEncodingFormat,
                valueProjection
        );

        // set default values
        final DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
        final int parallism = 1;
        final ChangelogMode changelogMode = valueEncodingFormat.getChangelogMode();
        // validate configs

        return new PulsarDynamicTableSink(
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
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SINK_PARALLELISM);
        options.add(KEY_FORMAT);
        options.add(KEY_FIELDS);
        return options;
    }

    private void validatePrimaryKey() {
        // TODO implement me
    }
}
