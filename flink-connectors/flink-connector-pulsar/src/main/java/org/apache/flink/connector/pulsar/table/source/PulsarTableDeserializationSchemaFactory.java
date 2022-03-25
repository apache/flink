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
import org.apache.flink.connector.pulsar.table.source.impl.PulsarProjectProducedRowSupport;
import org.apache.flink.connector.pulsar.table.source.impl.PulsarReadableMetadata;
import org.apache.flink.connector.pulsar.table.source.impl.PulsarUpsertSupport;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Contains key, value projection and format information, and use these information to create a
 * {@link org.apache.flink.connector.pulsar.table.source.PulsarTableDeserializationSchema} instance
 * used by runtime {@link org.apache.flink.connector.pulsar.source.PulsarSource} instance.
 */
public class PulsarTableDeserializationSchemaFactory implements Serializable {

    private static final long serialVersionUID = 6091562041940740434L;

    private final DataType physicalDataType;

    @Nullable private final DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat;

    private final int[] keyProjection;

    private final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    private final int[] valueProjection;

    // TODO mutable datatypes
    private DataType producedDataType;

    private List<String> connectorMetadataKeys;

    public PulsarTableDeserializationSchemaFactory(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            int[] keyProjection,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] valueProjection) {
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.keyDecodingFormat = keyDecodingFormat;
        this.keyProjection = keyProjection;
        this.valueDecodingFormat = valueDecodingFormat;
        this.valueProjection = valueProjection;
        // Mutable Data
        this.producedDataType = physicalDataType;
        this.connectorMetadataKeys = Collections.emptyList();
    }

    private @Nullable DeserializationSchema<RowData> createDeserialization(
            DynamicTableSource.Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        // TODO what does this dataType do ? Get the logical SQL dataType
        // TODO equals to the produced data type
        DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    public PulsarDeserializationSchema<RowData> createPulsarDeserialization(
            ScanTableSource.ScanContext context) {

        // TODO currerently we do not support the keyDeserialization yet.
        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(context, keyDecodingFormat, keyProjection, "");
        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(context, valueDecodingFormat, valueProjection, "");

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final PulsarUpsertSupport upsertSupport = new PulsarUpsertSupport(false);
        final PulsarReadableMetadata readableMetadata =
                new PulsarReadableMetadata(connectorMetadataKeys);

        // TODO can we make this part more clear?
        // adjust physical arity with value format's metadata
        final int physicalPlusFormatMetadataArity =
                producedDataType.getChildren().size() - readableMetadata.getMetadataArity();
        final int[] physicalPlusFormatMetadataProjection =
                adjustValueProjectionByAppendConnectorMetadata(physicalPlusFormatMetadataArity);
        final PulsarProjectProducedRowSupport projectionSupport =
                new PulsarProjectProducedRowSupport(
                        physicalPlusFormatMetadataArity,
                        keyProjection,
                        physicalPlusFormatMetadataProjection,
                        readableMetadata,
                        upsertSupport);
        // TODO add the projection support here.
        return new PulsarTableDeserializationSchema(
                keyDeserialization, valueDeserialization, producedTypeInfo, projectionSupport);
    }

    // TODO what is the multithread implications here
    public void setProducedDataType(DataType producedDataType) {
        this.producedDataType = producedDataType;
    }

    public void setConnectorMetadataKeys(List<String> metadataKeys) {
        this.connectorMetadataKeys = metadataKeys;
    }

    private int[] adjustValueProjectionByAppendConnectorMetadata(
            int physicalPlusFormatMetadataArity) {
        // adjust value format projection to include value format's metadata columns at the end
        final int[] physicalPlusFormatMetadataProjection =
                IntStream.concat(
                                IntStream.of(valueProjection),
                                IntStream.range(
                                        keyProjection.length + valueProjection.length,
                                        physicalPlusFormatMetadataArity))
                        .toArray();
        return physicalPlusFormatMetadataProjection;
    }
}
