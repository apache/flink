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

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link DecodingFormat} for Avro Variant that reads Avro records with dynamic writer schemas and
 * converts them to Variant. Supports format-level metadata columns (e.g., Avro schema string).
 */
@Internal
public class AvroVariantDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    private static final String SCHEMA_METADATA_KEY = "schema";
    private static final DataType SCHEMA_METADATA_TYPE = DataTypes.STRING();

    private final SchemaCoder.SchemaCoderProvider schemaCoderProvider;
    private final int maxCacheSize;
    private List<String> metadataKeys = Collections.emptyList();

    public AvroVariantDecodingFormat(
            SchemaCoder.SchemaCoderProvider schemaCoderProvider, int maxCacheSize) {
        this.schemaCoderProvider = schemaCoderProvider;
        this.maxCacheSize = maxCacheSize;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context, DataType physicalDataType) {

        validatePhysicalType((RowType) physicalDataType.getLogicalType());

        final boolean includeSchemaMetadata = metadataKeys.contains(SCHEMA_METADATA_KEY);
        final DataType producedDataType;
        if (includeSchemaMetadata) {
            producedDataType =
                    DataTypeUtils.appendRowFields(
                            physicalDataType,
                            Collections.singletonList(
                                    DataTypes.FIELD(SCHEMA_METADATA_KEY, SCHEMA_METADATA_TYPE)));
        } else {
            producedDataType = physicalDataType;
        }

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        return new AvroVariantDeserializationSchema(
                new RegistryWriterAvroDeserializationSchema(schemaCoderProvider),
                includeSchemaMetadata,
                maxCacheSize,
                producedTypeInfo);
    }

    private void validatePhysicalType(RowType rowType) {
        Preconditions.checkArgument(
                rowType.getFieldCount() == 1,
                "avro-variant format requires exactly one physical column of type VARIANT, "
                        + "but found %s columns",
                rowType.getFieldCount());

        LogicalType fieldType = rowType.getTypeAt(0);
        Preconditions.checkArgument(
                fieldType instanceof VariantType,
                "avro-variant format requires the physical column to be VARIANT type, "
                        + "but found %s",
                fieldType);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Collections.singletonMap(SCHEMA_METADATA_KEY, SCHEMA_METADATA_TYPE);
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
