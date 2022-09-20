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
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.api.SubscriptionType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ScanTableSource} implementation for Pulsar SQL Connector. It uses a {@link
 * SourceProvider} so it doesn't need to support {@link
 * org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown} interface.
 *
 * <p>{@link PulsarTableSource}
 */
public class PulsarTableSource implements ScanTableSource, SupportsReadingMetadata {
    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    private static final String FORMAT_METADATA_PREFIX = "value.";

    private final PulsarTableDeserializationSchemaFactory deserializationSchemaFactory;

    /**
     * Usually it is the same as the valueDecodingFormat, but use a different naming to show that it
     * is used to list all the format metadata keys.
     */
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormatForReadingMetadata;

    private final ChangelogMode changelogMode;

    // --------------------------------------------------------------------------------------------
    // PulsarSource needed attributes
    // --------------------------------------------------------------------------------------------

    private final List<String> topics;

    private final Properties properties;

    private final StartCursor startCursor;

    private final StopCursor stopCursor;

    private final SubscriptionType subscriptionType;

    public PulsarTableSource(
            PulsarTableDeserializationSchemaFactory deserializationSchemaFactory,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormatForReadingMetadata,
            ChangelogMode changelogMode,
            List<String> topics,
            Properties properties,
            StartCursor startCursor,
            StopCursor stopCursor,
            SubscriptionType subscriptionType) {
        // Format attributes
        this.deserializationSchemaFactory = checkNotNull(deserializationSchemaFactory);
        this.decodingFormatForReadingMetadata = checkNotNull(decodingFormatForReadingMetadata);
        this.changelogMode = changelogMode;
        // DataStream connector attributes
        this.topics = topics;
        this.properties = checkNotNull(properties);
        this.startCursor = checkNotNull(startCursor);
        this.stopCursor = checkNotNull(stopCursor);
        this.subscriptionType = checkNotNull(subscriptionType);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return changelogMode;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        PulsarDeserializationSchema<RowData> deserializationSchema =
                deserializationSchemaFactory.createPulsarDeserialization(context);
        PulsarSource<RowData> source =
                PulsarSource.builder()
                        .setTopics(topics)
                        .setStartCursor(startCursor)
                        .setUnboundedStopCursor(stopCursor)
                        .setDeserializationSchema(deserializationSchema)
                        .setSubscriptionType(subscriptionType)
                        .setProperties(properties)
                        .build();
        return SourceProvider.of(source);
    }

    /**
     * According to convention, the order of the final row must be PHYSICAL + FORMAT METADATA +
     * CONNECTOR METADATA where the format metadata has the highest precedence.
     *
     * @return
     */
    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> allMetadataMap = new LinkedHashMap<>();

        // add value format metadata with prefix
        decodingFormatForReadingMetadata
                .listReadableMetadata()
                .forEach((key, value) -> allMetadataMap.put(FORMAT_METADATA_PREFIX + key, value));
        // add connector metadata
        Stream.of(PulsarReadableMetadata.ReadableMetadata.values())
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
                decodingFormatForReadingMetadata.listReadableMetadata();
        if (formatMetadata.size() > 0) {
            final List<String> requestedFormatMetadataKeys =
                    formatMetadataKeys.stream()
                            .map(k -> k.substring(FORMAT_METADATA_PREFIX.length()))
                            .collect(Collectors.toList());
            decodingFormatForReadingMetadata.applyReadableMetadata(requestedFormatMetadataKeys);
        }

        // update the factory attributes.
        deserializationSchemaFactory.setConnectorMetadataKeys(connectorMetadataKeys);
        deserializationSchemaFactory.setProducedDataType(producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Pulsar table source";
    }

    @Override
    public DynamicTableSource copy() {
        final PulsarTableSource copy =
                new PulsarTableSource(
                        deserializationSchemaFactory,
                        decodingFormatForReadingMetadata,
                        changelogMode,
                        topics,
                        properties,
                        startCursor,
                        stopCursor,
                        subscriptionType);
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PulsarTableSource that = (PulsarTableSource) o;
        return Objects.equals(deserializationSchemaFactory, that.deserializationSchemaFactory)
                && Objects.equals(
                        decodingFormatForReadingMetadata, that.decodingFormatForReadingMetadata)
                && Objects.equals(changelogMode, that.changelogMode)
                && Objects.equals(topics, that.topics)
                && Objects.equals(properties, that.properties)
                && Objects.equals(startCursor, that.startCursor)
                && Objects.equals(stopCursor, that.stopCursor)
                && subscriptionType == that.subscriptionType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                deserializationSchemaFactory,
                decodingFormatForReadingMetadata,
                changelogMode,
                topics,
                properties,
                startCursor,
                stopCursor,
                subscriptionType);
    }
}
