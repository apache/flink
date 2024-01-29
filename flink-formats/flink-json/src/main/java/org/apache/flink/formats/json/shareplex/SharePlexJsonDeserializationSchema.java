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
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.shareplex.SharePlexJsonDecodingFormat.ReadableMetadata;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Deserialization schema from shareplex JSON to Flink Table/SQL internal data structure {@link
 * RowData}. The deserialization schema knows shareplex's schema definition and can extract the
 * database data and convert into {@link RowData} with {@link RowKind}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 *
 * @see <a href="https://www.quest.com/products/shareplex/">shareplex</a>
 */
public class SharePlexJsonDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 2L;
    private static Logger LOG = LoggerFactory.getLogger(SharePlexJsonDeserializationSchema.class);

    private static final String FIELD_NEW = "data";
    private static final String OP_INSERT = "ins";
    private static final String OP_UPDATE = "upd";
    private static final String OP_DELETE = "del";


    /**
     * The deserializer to deserialize shareplex JSON data.
     */
    private final JsonRowDataDeserializationSchema jsonDeserializer;

    /**
     * Flag that indicates that an additional projection is required for metadata.
     */
    private final boolean hasMetadata;

    /**
     * Metadata to be extracted for every record.
     */
    private final MetadataConverter[] metadataConverters;

    /**
     * {@link TypeInformation} of the produced {@link RowData} (physical + meta data).
     */
    private final TypeInformation<RowData> producedTypeInfo;

    /**
     * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
     */
    private final boolean ignoreParseErrors;

    /**
     * Names of physical fields.
     */
    private final List<String> fieldNames;

    /**
     * Number of physical fields.
     */
    private final int fieldCount;
    private final String defaultTable;

    private boolean isInit = false;
    private static int metaSize;

    public SharePlexJsonDeserializationSchema(
            DataType physicalDataType,
            List<ReadableMetadata> requestedMetadata,
            TypeInformation<RowData> producedTypeInfo,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat, String defaultTable) {


        this.defaultTable = defaultTable;
        if (!this.defaultTable.isEmpty()) {
            LOG.info(" filter table " + defaultTable);
        }

        final RowType jsonRowType = createJsonRowType(physicalDataType, requestedMetadata);
        this.jsonDeserializer =
                new JsonRowDataDeserializationSchema(
                        jsonRowType,
                        // the result type is never used, so it's fine to pass in the produced type
                        // info
                        producedTypeInfo,
                        // ignoreParseErrors already contains the functionality of
                        // failOnMissingField
                        false,
                        ignoreParseErrors,
                        timestampFormat);
        this.hasMetadata = requestedMetadata.size() > 0;
        this.metadataConverters = createMetadataConverters(requestedMetadata);
        this.producedTypeInfo = producedTypeInfo;
        this.ignoreParseErrors = ignoreParseErrors;
        final RowType physicalRowType = ((RowType) physicalDataType.getLogicalType());
        this.fieldNames = physicalRowType.getFieldNames();
        this.fieldCount = physicalRowType.getFieldCount();
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        if (message == null || message.length == 0) {
            return;
        }

        try {
            // share-plex --> shareplex
            final JsonNode root = jsonDeserializer.deserializeToJsonNode(message);
            final GenericRowData row = (GenericRowData) jsonDeserializer.convertToRowData(root);

            if (!filterTable(row)) {
                return;
            }
            String type = getType(row);
            if (OP_INSERT.equals(type)) {
                // "data" field is a row, contains inserted rows
                GenericRowData insert = (GenericRowData) row.getRow(0, fieldCount);
                insert.setRowKind(RowKind.INSERT);
                emitRow(row, insert, out);
            } else if (OP_UPDATE.equals(type)) {
                // "data" field is a row, contains new rows
                // "old" field is a row, contains old values
                // the underlying JSON deserialization schema always produce GenericRowData.
                // "data":{"id":106,"name":"hammer","description":"18oz carpenter hammer","weight":1.0}, // new
                // "key":{"description":"16oz carpenter's hammer"} // old

                //             "key":{"id":106,"name":"hammer","description":"16oz carpenter's hammer","weight":1.0},  // old
                //              "data":{"description":"18oz carpenter hammer"} // new
                GenericRowData after = (GenericRowData) row.getRow(0, fieldCount); // "data" field
                GenericRowData before = (GenericRowData) row.getRow(1, fieldCount); // "old" field
                final JsonNode newFiled = root.get(FIELD_NEW);
                for (int f = 0; f < fieldCount; f++) {
                    if (after.isNullAt(f) && newFiled.findValue(fieldNames.get(f)) == null) {
                        // not null fields in "old" (before) means the fields are changed
                        // null/empty fields in "old" (before) means the fields are not changed
                        // so we just copy the not changed fields into before
                        after.setField(f, before.getField(f));
                    }
                }
                before.setRowKind(RowKind.UPDATE_BEFORE);
                after.setRowKind(RowKind.UPDATE_AFTER);
                emitRow(row, before, out);
                emitRow(row, after, out);
            } else if (OP_DELETE.equals(type)) {
                // "data" field is a row, contains deleted rows
                GenericRowData delete = (GenericRowData) row.getRow(0, fieldCount);
                delete.setRowKind(RowKind.DELETE);
                emitRow(row, delete, out);
            } else {
                if (!ignoreParseErrors) {
                    throw new IOException(
                            format(
                                    "Unknown \"type\" value \"%s\". The shareplex JSON message is '%s'",
                                    type, new String(message)));
                }
            }
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw new IOException(
                        format("Corrupt shareplex JSON message '%s'.", new String(message)), t);
            }
        }
    }

    private String getType(GenericRowData row) {
        RowData metaData = row.getRow(2, metaSize);
        return metaData.getString(0).toString();
    }

    // 留下为true
    private boolean filterTable(GenericRowData row) {
        if (this.defaultTable.isEmpty()) {
            return false;
        }
        RowData metaData = row.getRow(2, metaSize);
        String table = metaData.getString(1).toString();
        String configTable = this.defaultTable.toLowerCase();

        return configTable.equals(table.toLowerCase()) || table.toLowerCase().endsWith("." + configTable);
    }

    private void emitRow(
            GenericRowData rootRow, GenericRowData physicalRow, Collector<RowData> out) {
        // shortcut in case no output projection is required
        if (!hasMetadata) {
            out.collect(physicalRow);
            return;
        }
        RowData metaData = rootRow.getRow(2, metaSize);

        final int metadataArity = metadataConverters.length;
        final GenericRowData producedRow =
                new GenericRowData(physicalRow.getRowKind(), fieldCount + metadataArity);
        for (int physicalPos = 0; physicalPos < fieldCount; physicalPos++) {
            producedRow.setField(physicalPos, physicalRow.getField(physicalPos));
        }
        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
            producedRow.setField(
                    fieldCount + metadataPos, metadataConverters[metadataPos].convert(metaData));
        }
        out.collect(producedRow);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SharePlexJsonDeserializationSchema that = (SharePlexJsonDeserializationSchema) o;
        return Objects.equals(jsonDeserializer, that.jsonDeserializer)
                && hasMetadata == that.hasMetadata
                && Objects.equals(producedTypeInfo, that.producedTypeInfo)
                && ignoreParseErrors == that.ignoreParseErrors
                && fieldCount == that.fieldCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jsonDeserializer, hasMetadata, producedTypeInfo, ignoreParseErrors, fieldCount);
    }

    // --------------------------------------------------------------------------------------------


    private static RowType createJsonRowType(
            DataType physicalDataType, List<ReadableMetadata> readableMetadata) {

        ResolvedSchema SCHEMA =
                ResolvedSchema.of(
                        Arrays.stream(ReadableMetadata.values()).map(m ->
                                Column.physical(m.requiredJsonField.getName(),
                                        DataTypes.STRING())).toArray(Column[]::new)
                );

        DataType META_DATA_TYPE = SCHEMA.toPhysicalRowDataType();
        metaSize = META_DATA_TYPE.getChildren().size();

        DataType root =
                DataTypes.ROW(
                        DataTypes.FIELD("data", physicalDataType),
                        DataTypes.FIELD("key", physicalDataType),
                        DataTypes.FIELD("meta", META_DATA_TYPE));
        // append fields that are required for reading metadata in the root
        final List<DataTypes.Field> rootMetadataFields =
                readableMetadata.stream()
                        .map(m -> m.requiredJsonField)
                        .distinct()
                        .collect(Collectors.toList());
//        return (RowType) DataTypeUtils.appendRowFields(root, rootMetadataFields).getLogicalType();
        return (RowType) root.getLogicalType();
    }

    private static MetadataConverter[] createMetadataConverters(
            List<ReadableMetadata> requestedMetadata) {
        List<String> metaKeys =
                Arrays.stream(ReadableMetadata.values()).map(m -> m.key).collect(Collectors.toList());

        return requestedMetadata.stream()
                .map(m -> convert(metaKeys, m))
                .toArray(MetadataConverter[]::new);
    }

    private static MetadataConverter convert(List<String> metaKeys, ReadableMetadata metadata) {
        final int pos = metaKeys.indexOf(metadata.requiredJsonField.getName());
        return new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(RowData root, int unused) {
                return metadata.converter.convert(root, pos);
            }
        };
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Converter that extracts a metadata field from the row that comes out of the JSON schema and
     * converts it to the desired data type.
     */
    interface MetadataConverter extends Serializable {

        // Method for top-level access.
        default Object convert(RowData row) {
            return convert(row, -1);
        }

        Object convert(RowData row, int pos);
    }
}
