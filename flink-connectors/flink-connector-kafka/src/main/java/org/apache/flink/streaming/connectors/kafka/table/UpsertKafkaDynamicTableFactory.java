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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
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
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.KEY_FIELDS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.VALUE_FIELDS_INCLUDE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.VALUE_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.createKeyFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.createValueFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getKafkaProperties;

/** Upsert-Kafka factory. */
public class UpsertKafkaDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "upsert-kafka";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPS_BOOTSTRAP_SERVERS);
        options.add(TOPIC);
        options.add(KEY_FORMAT);
        options.add(VALUE_FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FIELDS_INCLUDE);
        options.add(FactoryUtil.SINK_PARALLELISM);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();
        DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, KEY_FORMAT);
        DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, VALUE_FORMAT);

        // Validate the option data type.
        helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
        TableSchema schema = context.getCatalogTable().getSchema();
        validateTableOptions(tableOptions, keyDecodingFormat, valueDecodingFormat, schema);

        Tuple2<int[], int[]> keyValueProjections =
                createKeyValueProjections(context.getCatalogTable());
        String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);
        Properties properties = getKafkaProperties(context.getCatalogTable().getOptions());
        // always use earliest to keep data integrity
        StartupMode earliest = StartupMode.EARLIEST;

        return new KafkaDynamicSource(
                schema.toPhysicalRowDataType(),
                keyDecodingFormat,
                new DecodingFormatWrapper(valueDecodingFormat),
                keyValueProjections.f0,
                keyValueProjections.f1,
                keyPrefix,
                KafkaOptions.getSourceTopics(tableOptions),
                KafkaOptions.getSourceTopicPattern(tableOptions),
                properties,
                earliest,
                Collections.emptyMap(),
                0,
                true);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, KEY_FORMAT);
        EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, VALUE_FORMAT);

        // Validate the option data type.
        helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
        TableSchema schema = context.getCatalogTable().getSchema();
        validateTableOptions(tableOptions, keyEncodingFormat, valueEncodingFormat, schema);

        Tuple2<int[], int[]> keyValueProjections =
                createKeyValueProjections(context.getCatalogTable());
        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);
        final Properties properties = getKafkaProperties(context.getCatalogTable().getOptions());

        Integer parallelism = tableOptions.get(FactoryUtil.SINK_PARALLELISM);

        // use {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner}.
        // it will use hash partition if key is set else in round-robin behaviour.
        return new KafkaDynamicSink(
                schema.toPhysicalRowDataType(),
                keyEncodingFormat,
                new EncodingFormatWrapper(valueEncodingFormat),
                keyValueProjections.f0,
                keyValueProjections.f1,
                keyPrefix,
                tableOptions.get(TOPIC).get(0),
                properties,
                null,
                KafkaSinkSemantic.AT_LEAST_ONCE,
                true,
                parallelism);
    }

    private Tuple2<int[], int[]> createKeyValueProjections(CatalogTable catalogTable) {
        TableSchema schema = catalogTable.getSchema();
        // primary key should validated earlier
        List<String> keyFields = schema.getPrimaryKey().get().getColumns();
        DataType physicalDataType = schema.toPhysicalRowDataType();

        Configuration tableOptions = Configuration.fromMap(catalogTable.getOptions());
        // upsert-kafka will set key.fields to primary key fields by default
        tableOptions.set(KEY_FIELDS, keyFields);

        int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);
        int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        return Tuple2.of(keyProjection, valueProjection);
    }

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    private static void validateTableOptions(
            ReadableConfig tableOptions, Format keyFormat, Format valueFormat, TableSchema schema) {
        validateTopic(tableOptions);
        validateFormat(keyFormat, valueFormat, tableOptions);
        validatePKConstraints(schema);
    }

    private static void validateTopic(ReadableConfig tableOptions) {
        List<String> topic = tableOptions.get(TOPIC);
        if (topic.size() > 1) {
            throw new ValidationException(
                    "The 'upsert-kafka' connector doesn't support topic list now. "
                            + "Please use single topic as the value of the parameter 'topic'.");
        }
    }

    private static void validateFormat(
            Format keyFormat, Format valueFormat, ReadableConfig tableOptions) {
        if (!keyFormat.getChangelogMode().containsOnly(RowKind.INSERT)) {
            String identifier = tableOptions.get(KEY_FORMAT);
            throw new ValidationException(
                    String.format(
                            "'upsert-kafka' connector doesn't support '%s' as key format, "
                                    + "because '%s' is not in insert-only mode.",
                            identifier, identifier));
        }
        if (!valueFormat.getChangelogMode().containsOnly(RowKind.INSERT)) {
            String identifier = tableOptions.get(VALUE_FORMAT);
            throw new ValidationException(
                    String.format(
                            "'upsert-kafka' connector doesn't support '%s' as value format, "
                                    + "because '%s' is not in insert-only mode.",
                            identifier, identifier));
        }
    }

    private static void validatePKConstraints(TableSchema schema) {
        if (!schema.getPrimaryKey().isPresent()) {
            throw new ValidationException(
                    "'upsert-kafka' tables require to define a PRIMARY KEY constraint. "
                            + "The PRIMARY KEY specifies which columns should be read from or write to the Kafka message key. "
                            + "The PRIMARY KEY also defines records in the 'upsert-kafka' table should update or delete on which keys.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Format wrapper
    // --------------------------------------------------------------------------------------------

    /**
     * It is used to wrap the decoding format and expose the desired changelog mode. It's only works
     * for insert-only format.
     */
    protected static class DecodingFormatWrapper
            implements DecodingFormat<DeserializationSchema<RowData>> {
        private final DecodingFormat<DeserializationSchema<RowData>> innerDecodingFormat;

        private static final ChangelogMode SOURCE_CHANGELOG_MODE =
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();

        public DecodingFormatWrapper(
                DecodingFormat<DeserializationSchema<RowData>> innerDecodingFormat) {
            this.innerDecodingFormat = innerDecodingFormat;
        }

        @Override
        public DeserializationSchema<RowData> createRuntimeDecoder(
                DynamicTableSource.Context context, DataType producedDataType) {
            return innerDecodingFormat.createRuntimeDecoder(context, producedDataType);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return SOURCE_CHANGELOG_MODE;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            DecodingFormatWrapper that = (DecodingFormatWrapper) obj;
            return Objects.equals(innerDecodingFormat, that.innerDecodingFormat);
        }

        @Override
        public int hashCode() {
            return Objects.hash(innerDecodingFormat);
        }
    }

    /**
     * It is used to wrap the encoding format and expose the desired changelog mode. It's only works
     * for insert-only format.
     */
    protected static class EncodingFormatWrapper
            implements EncodingFormat<SerializationSchema<RowData>> {
        private final EncodingFormat<SerializationSchema<RowData>> innerEncodingFormat;

        public static final ChangelogMode SINK_CHANGELOG_MODE =
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();

        public EncodingFormatWrapper(
                EncodingFormat<SerializationSchema<RowData>> innerEncodingFormat) {
            this.innerEncodingFormat = innerEncodingFormat;
        }

        @Override
        public SerializationSchema<RowData> createRuntimeEncoder(
                DynamicTableSink.Context context, DataType consumedDataType) {
            return innerEncodingFormat.createRuntimeEncoder(context, consumedDataType);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return SINK_CHANGELOG_MODE;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            EncodingFormatWrapper that = (EncodingFormatWrapper) obj;
            return Objects.equals(innerEncodingFormat, that.innerEncodingFormat);
        }

        @Override
        public int hashCode() {
            return Objects.hash(innerEncodingFormat);
        }
    }
}
