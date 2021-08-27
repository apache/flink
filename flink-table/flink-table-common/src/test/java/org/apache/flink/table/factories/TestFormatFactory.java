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

package org.apache.flink.table.factories;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Tests implementations for {@link DeserializationFormatFactory} and {@link
 * SerializationFormatFactory}.
 */
public class TestFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "test-format";

    public static final ConfigOption<String> DELIMITER =
            ConfigOptions.key("delimiter")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("deprecated-delimiter");

    public static final ConfigOption<Boolean> FAIL_ON_MISSING =
            ConfigOptions.key("fail-on-missing")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys("fallback-fail-on-missing");

    public static final ConfigOption<List<String>> CHANGELOG_MODE =
            ConfigOptions.key("changelog-mode").stringType().asList().noDefaultValue();

    private static final ConfigOption<Map<String, String>> READABLE_METADATA =
            ConfigOptions.key("readable-metadata")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "Optional map of 'metadata_key:data_type,...'. The order will be alphabetically.");

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatConfig) {

        FactoryUtil.validateFactoryOptions(this, formatConfig);

        final Map<String, DataType> readableMetadata =
                convertToMetadataMap(formatConfig.get(READABLE_METADATA), context.getClassLoader());

        final ChangelogMode changelogMode = parseChangelogMode(formatConfig);

        return new DecodingFormatMock(
                formatConfig.get(DELIMITER),
                formatConfig.get(FAIL_ON_MISSING),
                changelogMode,
                readableMetadata);
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatConfig) {

        FactoryUtil.validateFactoryOptions(this, formatConfig);

        final ChangelogMode changelogMode = parseChangelogMode(formatConfig);

        return new EncodingFormatMock(formatConfig.get(DELIMITER), changelogMode);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DELIMITER);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FAIL_ON_MISSING);
        options.add(CHANGELOG_MODE);
        options.add(READABLE_METADATA);
        return options;
    }

    private static Map<String, DataType> convertToMetadataMap(
            Map<String, String> metadataOption, ClassLoader classLoader) {
        return metadataOption.keySet().stream()
                .sorted()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                key -> {
                                    final String typeString = metadataOption.get(key);
                                    final LogicalType type =
                                            LogicalTypeParser.parse(typeString, classLoader);
                                    return TypeConversions.fromLogicalToDataType(type);
                                },
                                (u, v) -> {
                                    throw new IllegalStateException();
                                },
                                LinkedHashMap::new));
    }

    // --------------------------------------------------------------------------------------------
    // Table source format & deserialization schema
    // --------------------------------------------------------------------------------------------

    /** {@link DecodingFormat} for testing. */
    public static class DecodingFormatMock
            implements DecodingFormat<DeserializationSchema<RowData>> {

        public final String delimiter;
        public final Boolean failOnMissing;
        private final ChangelogMode changelogMode;
        public final Map<String, DataType> readableMetadata;

        // we make the format stateful for capturing parameterization during testing
        public DataType producedDataType;
        public List<String> metadataKeys;

        public DecodingFormatMock(
                String delimiter,
                Boolean failOnMissing,
                ChangelogMode changelogMode,
                Map<String, DataType> readableMetadata) {
            this.delimiter = delimiter;
            this.failOnMissing = failOnMissing;
            this.changelogMode = changelogMode;
            this.readableMetadata = readableMetadata;
            this.metadataKeys = Collections.emptyList();
        }

        public DecodingFormatMock(String delimiter, Boolean failOnMissing) {
            this(delimiter, failOnMissing, ChangelogMode.insertOnly(), Collections.emptyMap());
        }

        @Override
        public DeserializationSchema<RowData> createRuntimeDecoder(
                DynamicTableSource.Context context, DataType physicalDataType) {

            final List<DataTypes.Field> metadataFields =
                    metadataKeys.stream()
                            .map(k -> DataTypes.FIELD(k, readableMetadata.get(k)))
                            .collect(Collectors.toList());

            this.producedDataType = DataTypeUtils.appendRowFields(physicalDataType, metadataFields);

            return new DeserializationSchemaMock(context.createTypeInformation(producedDataType));
        }

        @Override
        public Map<String, DataType> listReadableMetadata() {
            return readableMetadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys) {
            this.metadataKeys = metadataKeys;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return changelogMode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DecodingFormatMock that = (DecodingFormatMock) o;
            return delimiter.equals(that.delimiter)
                    && failOnMissing.equals(that.failOnMissing)
                    && changelogMode.equals(that.changelogMode)
                    && readableMetadata.equals(that.readableMetadata)
                    && Objects.equals(producedDataType, that.producedDataType)
                    && Objects.equals(metadataKeys, that.metadataKeys);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    delimiter,
                    failOnMissing,
                    changelogMode,
                    readableMetadata,
                    producedDataType,
                    metadataKeys);
        }
    }

    /** {@link DeserializationSchema} for testing. */
    private static class DeserializationSchemaMock implements DeserializationSchema<RowData> {

        private final TypeInformation<RowData> producedTypeInfo;

        private DeserializationSchemaMock(TypeInformation<RowData> producedTypeInfo) {
            this.producedTypeInfo = producedTypeInfo;
        }

        @Override
        public RowData deserialize(byte[] message) {
            String msg = "Test deserialization schema doesn't support deserialize.";
            throw new UnsupportedOperationException(msg);
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return producedTypeInfo;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Table sink format & serialization schema
    // --------------------------------------------------------------------------------------------

    /** {@link EncodingFormat} for testing. */
    public static class EncodingFormatMock implements EncodingFormat<SerializationSchema<RowData>> {

        public final String delimiter;

        // we make the format stateful for capturing parameterization during testing
        public DataType consumedDataType;

        private ChangelogMode changelogMode;

        public EncodingFormatMock(String delimiter, ChangelogMode changelogMode) {
            this.delimiter = delimiter;
            this.changelogMode = changelogMode;
        }

        public EncodingFormatMock(String delimiter) {
            this(delimiter, ChangelogMode.insertOnly());
        }

        @Override
        public SerializationSchema<RowData> createRuntimeEncoder(
                DynamicTableSink.Context context, DataType physicalDataType) {
            this.consumedDataType = physicalDataType;
            return new SerializationSchemaMock();
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return changelogMode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EncodingFormatMock that = (EncodingFormatMock) o;
            return delimiter.equals(that.delimiter)
                    && changelogMode.equals(that.changelogMode)
                    && Objects.equals(consumedDataType, that.consumedDataType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(delimiter, changelogMode, consumedDataType);
        }
    }

    /** {@link SerializationSchema} for testing. */
    private static class SerializationSchemaMock implements SerializationSchema<RowData> {
        @Override
        public byte[] serialize(RowData element) {
            String msg = "Test serialization schema doesn't support serialize.";
            throw new UnsupportedOperationException(msg);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Utils
    // --------------------------------------------------------------------------------------------

    private static ChangelogMode parseChangelogMode(ReadableConfig config) {
        if (config.getOptional(CHANGELOG_MODE).isPresent()) {
            ChangelogMode.Builder builder = ChangelogMode.newBuilder();
            for (String mode : config.get(CHANGELOG_MODE)) {
                switch (mode) {
                    case "I":
                        builder.addContainedKind(RowKind.INSERT);
                        break;
                    case "UA":
                        builder.addContainedKind(RowKind.UPDATE_AFTER);
                        break;
                    case "UB":
                        builder.addContainedKind(RowKind.UPDATE_BEFORE);
                        break;
                    case "D":
                        builder.addContainedKind(RowKind.DELETE);
                        break;
                    default:
                        throw new IllegalArgumentException(
                                String.format(
                                        "Unrecognized type %s for config %s",
                                        mode, CHANGELOG_MODE.key()));
                }
            }
            return builder.build();
        } else {
            return ChangelogMode.insertOnly();
        }
    }
}
