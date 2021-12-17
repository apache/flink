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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kinesis.table.RowDataKinesisDeserializationSchema.Metadata;

/** Kinesis-backed {@link ScanTableSource}. */
@Internal
public class KinesisDynamicSource implements ScanTableSource, SupportsReadingMetadata {

    /** List of read-only metadata fields that the source can provide upstream upon request. */
    private static final Map<String, DataType> READABLE_METADATA =
            new HashMap<String, DataType>() {
                {
                    for (Metadata metadata : Metadata.values()) {
                        put(metadata.getFieldName(), metadata.getDataType());
                    }
                }
            };

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    private DataType producedDataType;

    /** Metadata that is requested to be appended at the end of a physical source row. */
    private List<Metadata> requestedMetadataFields;

    // --------------------------------------------------------------------------------------------
    // Scan format attributes
    // --------------------------------------------------------------------------------------------

    /** Data type to configure the format. */
    private final DataType physicalDataType;

    /** Scan format for decoding records from Kinesis. */
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    // --------------------------------------------------------------------------------------------
    // Kinesis-specific attributes
    // --------------------------------------------------------------------------------------------

    /** The Kinesis stream to consume. */
    private final String stream;

    /** Properties for the Kinesis consumer. */
    private final Properties consumerProperties;

    public KinesisDynamicSource(
            DataType physicalDataType,
            String stream,
            Properties consumerProperties,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        this(
                physicalDataType,
                stream,
                consumerProperties,
                decodingFormat,
                physicalDataType,
                Collections.emptyList());
    }

    public KinesisDynamicSource(
            DataType physicalDataType,
            String stream,
            Properties consumerProperties,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType producedDataType,
            List<Metadata> requestedMetadataFields) {

        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.stream = Preconditions.checkNotNull(stream, "Stream must not be null.");
        this.consumerProperties =
                Preconditions.checkNotNull(
                        consumerProperties,
                        "Properties for the Flink Kinesis consumer must not be null.");
        this.decodingFormat =
                Preconditions.checkNotNull(decodingFormat, "Decoding format must not be null.");
        this.producedDataType =
                Preconditions.checkNotNull(
                        producedDataType, "Produced data type must not be null.");
        this.requestedMetadataFields =
                Preconditions.checkNotNull(
                        requestedMetadataFields, "Requested metadata fields must not be null.");
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        KinesisDeserializationSchema<RowData> deserializationSchema;

        if (requestedMetadataFields.size() > 0) {
            deserializationSchema =
                    new RowDataKinesisDeserializationSchema(
                            decodingFormat.createRuntimeDecoder(
                                    runtimeProviderContext, physicalDataType),
                            runtimeProviderContext.createTypeInformation(producedDataType),
                            requestedMetadataFields);
        } else {
            deserializationSchema =
                    new KinesisDeserializationSchemaWrapper<>(
                            decodingFormat.createRuntimeDecoder(
                                    runtimeProviderContext, physicalDataType));
        }

        FlinkKinesisConsumer<RowData> kinesisConsumer =
                new FlinkKinesisConsumer<>(stream, deserializationSchema, consumerProperties);

        return SourceFunctionProvider.of(kinesisConsumer, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new KinesisDynamicSource(
                physicalDataType,
                stream,
                consumerProperties,
                decodingFormat,
                producedDataType,
                requestedMetadataFields);
    }

    @Override
    public String asSummaryString() {
        return "Kinesis";
    }

    // --------------------------------------------------------------------------------------------
    // SupportsReadingMetadata
    // --------------------------------------------------------------------------------------------

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return READABLE_METADATA;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.requestedMetadataFields =
                metadataKeys.stream().map(Metadata::of).collect(Collectors.toList());
        this.producedDataType = producedDataType;
    }

    // --------------------------------------------------------------------------------------------
    // Value semantics for equals and hashCode
    // --------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KinesisDynamicSource that = (KinesisDynamicSource) o;
        return Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(requestedMetadataFields, that.requestedMetadataFields)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(stream, that.stream)
                && Objects.equals(consumerProperties, that.consumerProperties)
                && Objects.equals(decodingFormat, that.decodingFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                requestedMetadataFields,
                producedDataType,
                physicalDataType,
                stream,
                consumerProperties,
                decodingFormat);
    }
}
