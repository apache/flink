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

package org.apache.flink.connector.pulsar.table.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.table.source.impl.PulsarAppendMetadataSupport;
import org.apache.flink.connector.pulsar.table.source.impl.PulsarDeserializationSchemaFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.pulsar.client.api.SubscriptionType;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PulsarDynamicTableSource implements ScanTableSource, SupportsReadingMetadata {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    protected List<String> connectorMetadataKeys;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    private static final String FORMAT_METADATA_PREFIX = "value.";

    protected final PulsarDeserializationSchemaFactory deserializationSchemaFactory;

    protected final DecodingFormat<DeserializationSchema<RowData>>
            decodingFormatForMetadataPushdown;

    // --------------------------------------------------------------------------------------------
    // Pulsar-specific attributes
    // --------------------------------------------------------------------------------------------

    protected final List<String> topics;

    protected final Properties properties;

    protected final StartCursor startCursor;

    // TODO all streaming config options should be supported in Table API as well.
    public PulsarDynamicTableSource(
            PulsarDeserializationSchemaFactory deserializationSchemaFactory,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormatForMetadataPushdown,
            List<String> topics,
            Properties properties,
            StartCursor startCursor) {
        // Format attributes
        this.deserializationSchemaFactory = deserializationSchemaFactory;
        this.decodingFormatForMetadataPushdown =
                Preconditions.checkNotNull(
                        decodingFormatForMetadataPushdown,
                        "Value decoding format must not be null.");
        // Mutable attributes
        this.topics = topics;
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.startCursor = startCursor;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormatForMetadataPushdown.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {

        PulsarDeserializationSchema<RowData> deserializationSchema =
                deserializationSchemaFactory.createPulsarDeserialization(context);

        // values not exposed to users
        final String subscriptionName = "default-subscription";
        PulsarSource<RowData> source =
                createDefaultPulsarSourceBuilder()
                        .setTopics(topics)
                        .setStartCursor(startCursor)
                        .setDeserializationSchema(deserializationSchema)
                        .setSubscriptionName(subscriptionName)
                        .setProperties(properties)
                        .build();
        // TODO the boundedness should be supported
        return SourceProvider.of(source);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> allMetadataMap = new LinkedHashMap<>();

        // according to convention, the order of the final row must be
        // PHYSICAL + FORMAT METADATA + CONNECTOR METADATA
        // where the format metadata has highest precedence

        // add value format metadata with prefix
        decodingFormatForMetadataPushdown
                .listReadableMetadata()
                .forEach((key, value) -> allMetadataMap.put(FORMAT_METADATA_PREFIX + key, value));

        // add connector metadata
        Stream.of(PulsarAppendMetadataSupport.ReadableMetadata.values())
                .forEachOrdered(m -> allMetadataMap.putIfAbsent(m.key, m.dataType));

        return allMetadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> allMetadataKeys, DataType producedDataType) {
        // separate connector and format metadata
        final List<String> formatMetadataKeys =
                allMetadataKeys.stream()
                        .filter(k -> k.startsWith(FORMAT_METADATA_PREFIX))
                        .collect(Collectors.toList());

        final List<String> connectorMetadataKeys = new ArrayList<>(allMetadataKeys);
        connectorMetadataKeys.removeAll(formatMetadataKeys);

        // push down format metadata
        final Map<String, DataType> formatMetadata =
                decodingFormatForMetadataPushdown.listReadableMetadata();
        if (formatMetadata.size() > 0) {
            final List<String> requestedFormatMetadataKeys =
                    formatMetadataKeys.stream()
                            .map(k -> k.substring(FORMAT_METADATA_PREFIX.length()))
                            .collect(Collectors.toList());
            decodingFormatForMetadataPushdown.applyReadableMetadata(requestedFormatMetadataKeys);
        }

        // TODO it will be updated here as well
        deserializationSchemaFactory.setConnectorMetadataKeys(connectorMetadataKeys);
        deserializationSchemaFactory.setProducedDataType(producedDataType);
    }

    private PulsarSourceBuilder<RowData> createDefaultPulsarSourceBuilder() {
        PulsarSourceBuilder<RowData> builder = PulsarSource.builder();
        builder.setSubscriptionType(SubscriptionType.Exclusive);
        return builder;
    }

    @Override
    public DynamicTableSource copy() {
        final PulsarDynamicTableSource copy =
                new PulsarDynamicTableSource(
                        deserializationSchemaFactory,
                        decodingFormatForMetadataPushdown,
                        topics,
                        properties,
                        startCursor);
        copy.connectorMetadataKeys = connectorMetadataKeys;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Pulsar universal table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PulsarDynamicTableSource that = (PulsarDynamicTableSource) o;
        return Objects.equals(connectorMetadataKeys, that.connectorMetadataKeys)
                && Objects.equals(
                        decodingFormatForMetadataPushdown, that.decodingFormatForMetadataPushdown)
                && Objects.equals(topics, that.topics)
                && Objects.equals(properties, that.properties)
                && Objects.equals(startCursor, that.startCursor);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        connectorMetadataKeys,
                        decodingFormatForMetadataPushdown,
                        topics,
                        properties,
                        startCursor);
        return result;
    }
}
