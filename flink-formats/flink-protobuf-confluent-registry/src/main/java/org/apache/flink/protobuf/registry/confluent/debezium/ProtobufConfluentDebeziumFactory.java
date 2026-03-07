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

package org.apache.flink.protobuf.registry.confluent.debezium;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatFactoryUtils;
import org.apache.flink.protobuf.registry.confluent.SchemaRegistryClientProviders;
import org.apache.flink.protobuf.registry.confluent.dynamic.deserializer.ProtoRegistryDynamicDeserializationSchema;
import org.apache.flink.protobuf.registry.confluent.dynamic.serializer.ProtoRegistryDynamicSerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Set;

import static org.apache.flink.protobuf.registry.confluent.ProtobufConfluentFormatOptions.URL;

public class ProtobufConfluentDebeziumFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "protobuf-confluent-debezium";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        String schemaRegistryURL = formatOptions.get(URL);
        SchemaRegistryClientProviders.CachedSchemaRegistryClientProvider
                schemaRegistryClientProvider =
                        ProtobufConfluentFormatFactoryUtils
                                .createCachedSchemaRegistryClientProvider(formatOptions);

        return new ProjectableDecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType producedDataType,
                    int[][] projections) {
                ProtoRegistryDynamicDeserializationSchema wrappedDeser =
                        ProtobufConfluentFormatFactoryUtils.createDynamicDeserializationSchema(
                                context,
                                producedDataType,
                                projections,
                                schemaRegistryClientProvider,
                                schemaRegistryURL,
                                formatOptions);
                return new ProtobufConfluentDebeziumDeserializationSchema(wrappedDeser);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_BEFORE)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        ProtobufConfluentFormatFactoryUtils.validateDynamicEncodingOptions(
                formatOptions, IDENTIFIER);

        String schemaRegistryURL = formatOptions.get(URL);
        SchemaRegistryClientProviders.CachedSchemaRegistryClientProvider
                schemaRegistryClientProvider =
                        ProtobufConfluentFormatFactoryUtils
                                .createCachedSchemaRegistryClientProvider(formatOptions);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                ProtoRegistryDynamicSerializationSchema wrappedSer =
                        ProtobufConfluentFormatFactoryUtils.createDynamicSerializationSchema(
                                consumedDataType,
                                schemaRegistryClientProvider,
                                schemaRegistryURL,
                                formatOptions);
                return new ProtobufConfluentDebeziumSerializationSchema(wrappedSer);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_BEFORE)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return ProtobufConfluentFormatFactoryUtils.requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return ProtobufConfluentFormatFactoryUtils.optionalOptions();
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return ProtobufConfluentFormatFactoryUtils.forwardOptions();
    }
}
