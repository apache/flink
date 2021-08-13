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

package org.apache.flink.formats.json.maxwell;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.maxwell.MaxwellJsonDecodingFormat.ReadableMetadata;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Deserialization schema from Maxwell JSON to Flink Table/SQL internal data structure {@link
 * RowData}. The deserialization schema knows Maxwell's schema definition and can extract the
 * database data and convert into {@link RowData} with {@link RowKind}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 *
 * @see <a href="http://maxwells-daemon.io/">Maxwell</a>
 */
public class MaxwellJsonDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 2L;

    private static final String FIELD_OLD = "old";
    private static final String OP_INSERT = "insert";
    private static final String OP_UPDATE = "update";
    private static final String OP_DELETE = "delete";

    /** The deserializer to deserialize Maxwell JSON data. */
    private final JsonRowDataDeserializationSchema jsonDeserializer;

    /** Flag that indicates that an additional projection is required for metadata. */
    private final boolean hasMetadata;

    /** Metadata to be extracted for every record. */
    private final MetadataConverter[] metadataConverters;

    /** {@link TypeInformation} of the produced {@link RowData} (physical + meta data). */
    private final TypeInformation<RowData> producedTypeInfo;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Flag indicating whether to parse non-numeric number fields (default: throw an exception). */
    private final boolean allowNonNumericNumbers;

    /** Names of physical fields. */
    private final List<String> fieldNames;

    /** Number of physical fields. */
    private final int fieldCount;

    public MaxwellJsonDeserializationSchema(
            DataType physicalDataType,
            List<ReadableMetadata> requestedMetadata,
            TypeInformation<RowData> producedTypeInfo,
            boolean ignoreParseErrors,
            boolean allowNonNumericNumbers,
            TimestampFormat timestampFormat) {
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
                        allowNonNumericNumbers,
                        timestampFormat);
        this.hasMetadata = requestedMetadata.size() > 0;
        this.metadataConverters = createMetadataConverters(jsonRowType, requestedMetadata);
        this.producedTypeInfo = producedTypeInfo;
        this.ignoreParseErrors = ignoreParseErrors;
        this.allowNonNumericNumbers = allowNonNumericNumbers;
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
            final JsonNode root = jsonDeserializer.deserializeToJsonNode(message);
            final GenericRowData row = (GenericRowData) jsonDeserializer.convertToRowData(root);
            String type = row.getString(2).toString(); // "type" field
            if (OP_INSERT.equals(type)) {
                // "data" field is a row, contains inserted rows
                GenericRowData insert = (GenericRowData) row.getRow(0, fieldCount);
                insert.setRowKind(RowKind.INSERT);
                emitRow(row, insert, out);
            } else if (OP_UPDATE.equals(type)) {
                // "data" field is a row, contains new rows
                // "old" field is a row, contains old values
                // the underlying JSON deserialization schema always produce GenericRowData.
                GenericRowData after = (GenericRowData) row.getRow(0, fieldCount); // "data" field
                GenericRowData before = (GenericRowData) row.getRow(1, fieldCount); // "old" field
                final JsonNode oldField = root.get(FIELD_OLD);
                for (int f = 0; f < fieldCount; f++) {
                    if (before.isNullAt(f) && oldField.findValue(fieldNames.get(f)) == null) {
                        // not null fields in "old" (before) means the fields are changed
                        // null/empty fields in "old" (before) means the fields are not changed
                        // so we just copy the not changed fields into before
                        before.setField(f, after.getField(f));
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
                                    "Unknown \"type\" value \"%s\". The Maxwell JSON message is '%s'",
                                    type, new String(message)));
                }
            }
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw new IOException(
                        format("Corrupt Maxwell JSON message '%s'.", new String(message)), t);
            }
        }
    }

    private void emitRow(
            GenericRowData rootRow, GenericRowData physicalRow, Collector<RowData> out) {
        // shortcut in case no output projection is required
        if (!hasMetadata) {
            out.collect(physicalRow);
            return;
        }
        final int metadataArity = metadataConverters.length;
        final GenericRowData producedRow =
                new GenericRowData(physicalRow.getRowKind(), fieldCount + metadataArity);
        for (int physicalPos = 0; physicalPos < fieldCount; physicalPos++) {
            producedRow.setField(physicalPos, physicalRow.getField(physicalPos));
        }
        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
            producedRow.setField(
                    fieldCount + metadataPos, metadataConverters[metadataPos].convert(rootRow));
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
        MaxwellJsonDeserializationSchema that = (MaxwellJsonDeserializationSchema) o;
        return Objects.equals(jsonDeserializer, that.jsonDeserializer)
                && hasMetadata == that.hasMetadata
                && Objects.equals(producedTypeInfo, that.producedTypeInfo)
                && ignoreParseErrors == that.ignoreParseErrors
                && allowNonNumericNumbers == that.allowNonNumericNumbers
                && fieldCount == that.fieldCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jsonDeserializer,
                hasMetadata,
                producedTypeInfo,
                ignoreParseErrors,
                allowNonNumericNumbers,
                fieldCount);
    }

    // --------------------------------------------------------------------------------------------

    private static RowType createJsonRowType(
            DataType physicalDataType, List<ReadableMetadata> readableMetadata) {
        DataType root =
                DataTypes.ROW(
                        DataTypes.FIELD("data", physicalDataType),
                        DataTypes.FIELD("old", physicalDataType),
                        DataTypes.FIELD("type", DataTypes.STRING()));
        // append fields that are required for reading metadata in the root
        final List<DataTypes.Field> rootMetadataFields =
                readableMetadata.stream()
                        .map(m -> m.requiredJsonField)
                        .distinct()
                        .collect(Collectors.toList());
        return (RowType) DataTypeUtils.appendRowFields(root, rootMetadataFields).getLogicalType();
    }

    private static MetadataConverter[] createMetadataConverters(
            RowType jsonRowType, List<ReadableMetadata> requestedMetadata) {
        return requestedMetadata.stream()
                .map(m -> convert(jsonRowType, m))
                .toArray(MetadataConverter[]::new);
    }

    private static MetadataConverter convert(RowType jsonRowType, ReadableMetadata metadata) {
        final int pos = jsonRowType.getFieldNames().indexOf(metadata.requiredJsonField.getName());
        return new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(GenericRowData root, int unused) {
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
        default Object convert(GenericRowData row) {
            return convert(row, -1);
        }

        Object convert(GenericRowData row, int pos);
    }
}
