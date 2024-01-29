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

package org.apache.flink.formats.json.shareplex;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.shareplex.SharePlexJsonDeserializationSchema.MetadataConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link DecodingFormat} for shareplex using JSON encoding.
 */
public class SharePlexJsonDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /**
     * The requested metadata keys.
     */
    private List<String> metadataKeys;

    private final boolean ignoreParseErrors;

    private final TimestampFormat timestampFormat;

    private final String defaultTable;

    public SharePlexJsonDecodingFormat(boolean ignoreParseErrors, TimestampFormat timestampFormat, String defaultTable) {
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
        this.defaultTable = defaultTable;
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context, DataType physicalDataType) {
        final List<ReadableMetadata> readableMetadata =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalStateException(
                                                                        String.format(
                                                                                "Could not find the requested metadata key: %s",
                                                                                k))))
                        .collect(Collectors.toList());
        final List<DataTypes.Field> metadataFields =
                readableMetadata.stream()
                        .map(m -> DataTypes.FIELD(m.key, m.dataType))
                        .collect(Collectors.toList());
        final DataType producedDataType =
                DataTypeUtils.appendRowFields(physicalDataType, metadataFields);
        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        return new SharePlexJsonDeserializationSchema(
                physicalDataType,
                readableMetadata,
                producedTypeInfo,
                ignoreParseErrors,
                timestampFormat, defaultTable);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
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

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    /**
     * List of metadata that can be read with this format.
     */
    enum ReadableMetadata {
        OP(
                "op",
                DataTypes.STRING().nullable(),
                DataTypes.FIELD("op", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(RowData row, int pos) {
                        return row.getString(pos);
                    }
                }),
        TABLE(
                "table",
                DataTypes.STRING().nullable(),
                DataTypes.FIELD("table", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(RowData row, int pos) {
                        return row.getString(pos);
                    }
                }),
        TIME(
                "time",
                DataTypes.STRING().nullable(),
                DataTypes.FIELD("time", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(RowData row, int pos) {
                        return row.getString(pos);
                    }
                }),
        POST_TIME(
                "posttime",
                DataTypes.STRING().nullable(),
                DataTypes.FIELD("posttime", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(RowData row, int pos) {
                        return row.getString(pos);
                    }
                });

        // userid,scn,rowid,trans,seq,size,idx

        final String key;

        final DataType dataType;

        final DataTypes.Field requiredJsonField;

        final MetadataConverter converter;

        ReadableMetadata(
                String key,
                DataType dataType,
                DataTypes.Field requiredJsonField,
                MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.requiredJsonField = requiredJsonField;
            this.converter = converter;
        }
    }
}
