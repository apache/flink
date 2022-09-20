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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Contains key, value projection and format information, and use such information to create a
 * {@link PulsarTableDeserializationSchema} instance used by runtime {@link
 * org.apache.flink.connector.pulsar.source.PulsarSource} instance.
 *
 * <p>A Flink row fields has a strict order: Physical Fields (Key + value) + Format Metadata Fields
 * Connector Metadata Fields. Physical Fields are fields come directly from Pulsar message body;
 * Format Metadata Fields are from the extra information from the decoding format. Connector
 * metadata fields are the ones most Pulsar messages have, such as publish time, message size and
 * producer name.
 *
 * <p>In general, Physical fields + Format Metadata fields are contained in the RowData decoded
 * using valueDecodingFormat. Only Connector Metadata fields needs to be appended to the decoded
 * RowData. The tricky part is to put format metadata and connector metadata in the right location.
 * This requires an explicit adjustment process.
 *
 * <p>For example, suppose Physical Fields (Key + value) + Format Metadata Fields + Connector
 * Metadata Fields. has arity of 11, key projection is [0, 6], and physical value projection is [1,
 * 2, 3, 4, 5], Then after the adjustment, key projection should be [0, 6], physical value
 * projection should be [1, 2, 3, 4, 5] and format metadata projection should be [7], connector
 * metadata projection should be [8, 9, 10].
 */
public class PulsarTableDeserializationSchemaFactory implements Serializable {

    private static final long serialVersionUID = 6091562041940740434L;

    private final DataType physicalDataType;

    @Nullable private final DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat;

    private final int[] keyProjection;

    private final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    private final int[] valueProjection;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes. Will be updated after the applyReadableMetadata()
    // --------------------------------------------------------------------------------------------
    private DataType producedDataType;

    private List<String> connectorMetadataKeys;

    private final boolean upsertMode;

    public PulsarTableDeserializationSchemaFactory(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            int[] keyProjection,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] valueProjection,
            boolean upsertMode) {
        this.physicalDataType =
                checkNotNull(physicalDataType, "field physicalDataType must not be null.");
        this.keyDecodingFormat = keyDecodingFormat;
        this.keyProjection = checkNotNull(keyProjection);
        this.valueDecodingFormat =
                checkNotNull(valueDecodingFormat, "field valueDecodingFormat must not be null.");
        this.valueProjection =
                checkNotNull(valueProjection, "field valueProjection must not be null.");

        this.producedDataType = physicalDataType;
        this.connectorMetadataKeys = Collections.emptyList();
        this.upsertMode = upsertMode;
    }

    private @Nullable DeserializationSchema<RowData> createDeserialization(
            DynamicTableSource.Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }

        DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    public PulsarDeserializationSchema<RowData> createPulsarDeserialization(
            ScanTableSource.ScanContext context) {
        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(context, keyDecodingFormat, keyProjection, "");
        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(context, valueDecodingFormat, valueProjection, "");

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final PulsarReadableMetadata readableMetadata =
                new PulsarReadableMetadata(connectorMetadataKeys);

        // Get Physical Fields (key + value) + Format Metadata arity
        final int physicalPlusFormatMetadataArity =
                DataType.getFieldDataTypes(producedDataType).size()
                        - readableMetadata.getConnectorMetadataArity();
        final int[] physicalValuePlusFormatMetadataProjection =
                adjustValueProjectionByAppendConnectorMetadata(physicalPlusFormatMetadataArity);

        final PulsarRowDataConverter rowDataConverter =
                new PulsarRowDataConverter(
                        physicalPlusFormatMetadataArity,
                        keyProjection,
                        physicalValuePlusFormatMetadataProjection,
                        readableMetadata,
                        upsertMode);

        return new PulsarTableDeserializationSchema(
                keyDeserialization,
                valueDeserialization,
                producedTypeInfo,
                rowDataConverter,
                upsertMode);
    }

    public void setProducedDataType(DataType producedDataType) {
        this.producedDataType = producedDataType;
    }

    public void setConnectorMetadataKeys(List<String> metadataKeys) {
        this.connectorMetadataKeys = metadataKeys;
    }

    private int[] adjustValueProjectionByAppendConnectorMetadata(
            int physicalValuePlusFormatMetadataArity) {
        // Concat the Physical Fields (value only) with Format metadata projection.
        final int[] physicalValuePlusFormatMetadataProjection =
                IntStream.concat(
                                IntStream.of(valueProjection),
                                IntStream.range(
                                        keyProjection.length + valueProjection.length,
                                        physicalValuePlusFormatMetadataArity))
                        .toArray();
        return physicalValuePlusFormatMetadataProjection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PulsarTableDeserializationSchemaFactory that = (PulsarTableDeserializationSchemaFactory) o;
        return Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyDecodingFormat, that.keyDecodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Objects.equals(valueDecodingFormat, that.valueDecodingFormat)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(connectorMetadataKeys, that.connectorMetadataKeys)
                && Objects.equals(upsertMode, that.upsertMode);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        physicalDataType,
                        keyDecodingFormat,
                        valueDecodingFormat,
                        producedDataType,
                        connectorMetadataKeys,
                        upsertMode);
        result = 31 * result + Arrays.hashCode(keyProjection);
        result = 31 * result + Arrays.hashCode(valueProjection);
        return result;
    }
}
