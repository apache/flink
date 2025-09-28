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

package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.deserialize.PbRowDataDeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

/** {@link DecodingFormat} for protobuf decoding. */
public class PbDecodingFormat implements ProjectableDecodingFormat<DeserializationSchema<RowData>> {
    private final PbFormatConfig formatConfig;

    public PbDecodingFormat(PbFormatConfig formatConfig) {
        this.formatConfig = formatConfig;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context, DataType physicalDataType, int[][] projections) {
        final DataType producedDataType = Projection.of(projections).project(physicalDataType);
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> rowDataTypeInfo =
                context.createTypeInformation(producedDataType);

        return new PbRowDataDeserializationSchema(
                rowType,
                rowDataTypeInfo,
                formatConfig,
                toProjectedNames((RowType) physicalDataType.getLogicalType(), projections));
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    private String[][] toProjectedNames(RowType type, int[][] projectedFields) {
        String[][] projectedNames = new String[projectedFields.length][];
        for (int i = 0; i < projectedNames.length; i++) {
            int[] fieldIndices = projectedFields[i];
            String[] fieldNames = new String[fieldIndices.length];
            projectedNames[i] = fieldNames;

            // convert fieldIndices to fieldNames
            RowType currentType = type;
            for (int j = 0; j < fieldIndices.length; j++) {
                int index = fieldIndices[j];
                fieldNames[j] = currentType.getFieldNames().get(index);
                if (j != fieldIndices.length - 1) {
                    currentType = (RowType) currentType.getTypeAt(index);
                }
            }
        }
        return projectedNames;
    }
}
