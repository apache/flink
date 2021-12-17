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

package org.apache.flink.formats.json.canal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.canal.CanalJsonDecodingFormat.ReadableMetadata;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Deserialization schema from Canal JSON to Flink Table/SQL internal data structure {@link
 * RowData}. The deserialization schema knows Canal's schema definition and can extract the database
 * data and convert into {@link RowData} with {@link RowKind}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 *
 * @see <a href="https://github.com/alibaba/canal">Alibaba Canal</a>
 */
public final class CanalJsonDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    private static final String FIELD_OLD = "old";
    private static final String OP_INSERT = "INSERT";
    private static final String OP_UPDATE = "UPDATE";
    private static final String OP_DELETE = "DELETE";
    private static final String OP_CREATE = "CREATE";

    /** The deserializer to deserialize Canal JSON data. */
    private final JsonRowDataDeserializationSchema jsonDeserializer;

    /** Flag that indicates that an additional projection is required for metadata. */
    private final boolean hasMetadata;

    /** Metadata to be extracted for every record. */
    private final MetadataConverter[] metadataConverters;

    /** {@link TypeInformation} of the produced {@link RowData} (physical + meta data). */
    private final TypeInformation<RowData> producedTypeInfo;

    /** Only read changelogs from the specific database. */
    private final @Nullable String database;

    /** Only read changelogs from the specific table. */
    private final @Nullable String table;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Names of fields. */
    private final List<String> fieldNames;

    /** Number of fields. */
    private final int fieldCount;

    /** Pattern of the specific database. */
    private final Pattern databasePattern;

    /** Pattern of the specific table. */
    private final Pattern tablePattern;

    private CanalJsonDeserializationSchema(
            DataType physicalDataType,
            List<ReadableMetadata> requestedMetadata,
            TypeInformation<RowData> producedTypeInfo,
            @Nullable String database,
            @Nullable String table,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        final RowType jsonRowType = createJsonRowType(physicalDataType, requestedMetadata);
        this.jsonDeserializer =
                new JsonRowDataDeserializationSchema(
                        jsonRowType,
                        // the result type is never used, so it's fine to pass in the produced type
                        // info
                        producedTypeInfo,
                        false, // ignoreParseErrors already contains the functionality of
                        // failOnMissingField
                        ignoreParseErrors,
                        timestampFormat);
        this.hasMetadata = requestedMetadata.size() > 0;
        this.metadataConverters = createMetadataConverters(jsonRowType, requestedMetadata);
        this.producedTypeInfo = producedTypeInfo;
        this.database = database;
        this.table = table;
        this.ignoreParseErrors = ignoreParseErrors;
        final RowType physicalRowType = ((RowType) physicalDataType.getLogicalType());
        this.fieldNames = physicalRowType.getFieldNames();
        this.fieldCount = physicalRowType.getFieldCount();
        this.databasePattern = database == null ? null : Pattern.compile(database);
        this.tablePattern = table == null ? null : Pattern.compile(table);
    }

    // ------------------------------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------------------------------

    /** Creates A builder for building a {@link CanalJsonDeserializationSchema}. */
    public static Builder builder(
            DataType physicalDataType,
            List<ReadableMetadata> requestedMetadata,
            TypeInformation<RowData> producedTypeInfo) {
        return new Builder(physicalDataType, requestedMetadata, producedTypeInfo);
    }

    /** A builder for creating a {@link CanalJsonDeserializationSchema}. */
    @Internal
    public static final class Builder {
        private final DataType physicalDataType;
        private final List<ReadableMetadata> requestedMetadata;
        private final TypeInformation<RowData> producedTypeInfo;
        private String database = null;
        private String table = null;
        private boolean ignoreParseErrors = false;
        private TimestampFormat timestampFormat = TimestampFormat.SQL;

        private Builder(
                DataType physicalDataType,
                List<ReadableMetadata> requestedMetadata,
                TypeInformation<RowData> producedTypeInfo) {
            this.physicalDataType = physicalDataType;
            this.requestedMetadata = requestedMetadata;
            this.producedTypeInfo = producedTypeInfo;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public Builder setTimestampFormat(TimestampFormat timestampFormat) {
            this.timestampFormat = timestampFormat;
            return this;
        }

        public CanalJsonDeserializationSchema build() {
            return new CanalJsonDeserializationSchema(
                    physicalDataType,
                    requestedMetadata,
                    producedTypeInfo,
                    database,
                    table,
                    ignoreParseErrors,
                    timestampFormat);
        }
    }

    // ------------------------------------------------------------------------------------------

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(@Nullable byte[] message, Collector<RowData> out) throws IOException {
        if (message == null || message.length == 0) {
            return;
        }
        try {
            final JsonNode root = jsonDeserializer.deserializeToJsonNode(message);
            if (database != null) {
                if (!databasePattern
                        .matcher(root.get(ReadableMetadata.DATABASE.key).asText())
                        .matches()) {
                    return;
                }
            }
            if (table != null) {
                if (!tablePattern
                        .matcher(root.get(ReadableMetadata.TABLE.key).asText())
                        .matches()) {
                    return;
                }
            }
            final GenericRowData row = (GenericRowData) jsonDeserializer.convertToRowData(root);
            String type = row.getString(2).toString(); // "type" field
            if (OP_INSERT.equals(type)) {
                // "data" field is an array of row, contains inserted rows
                ArrayData data = row.getArray(0);
                for (int i = 0; i < data.size(); i++) {
                    GenericRowData insert = (GenericRowData) data.getRow(i, fieldCount);
                    insert.setRowKind(RowKind.INSERT);
                    emitRow(row, insert, out);
                }
            } else if (OP_UPDATE.equals(type)) {
                // "data" field is an array of row, contains new rows
                ArrayData data = row.getArray(0);
                // "old" field is an array of row, contains old values
                ArrayData old = row.getArray(1);
                for (int i = 0; i < data.size(); i++) {
                    // the underlying JSON deserialization schema always produce GenericRowData.
                    GenericRowData after = (GenericRowData) data.getRow(i, fieldCount);
                    GenericRowData before = (GenericRowData) old.getRow(i, fieldCount);
                    final JsonNode oldField = root.get(FIELD_OLD);
                    for (int f = 0; f < fieldCount; f++) {
                        if (before.isNullAt(f) && oldField.findValue(fieldNames.get(f)) == null) {
                            // fields in "old" (before) means the fields are changed
                            // fields not in "old" (before) means the fields are not changed
                            // so we just copy the not changed fields into before
                            before.setField(f, after.getField(f));
                        }
                    }
                    before.setRowKind(RowKind.UPDATE_BEFORE);
                    after.setRowKind(RowKind.UPDATE_AFTER);
                    emitRow(row, before, out);
                    emitRow(row, after, out);
                }
            } else if (OP_DELETE.equals(type)) {
                // "data" field is an array of row, contains deleted rows
                ArrayData data = row.getArray(0);
                for (int i = 0; i < data.size(); i++) {
                    GenericRowData insert = (GenericRowData) data.getRow(i, fieldCount);
                    insert.setRowKind(RowKind.DELETE);
                    emitRow(row, insert, out);
                }
            } else if (OP_CREATE.equals(type)) {
                // "data" field is null and "type" is "CREATE" which means
                // this is a DDL change event, and we should skip it.
                return;
            } else {
                if (!ignoreParseErrors) {
                    throw new IOException(
                            format(
                                    "Unknown \"type\" value \"%s\". The Canal JSON message is '%s'",
                                    type, new String(message)));
                }
            }
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw new IOException(
                        format("Corrupt Canal JSON message '%s'.", new String(message)), t);
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
        final int physicalArity = physicalRow.getArity();
        final int metadataArity = metadataConverters.length;
        final GenericRowData producedRow =
                new GenericRowData(physicalRow.getRowKind(), physicalArity + metadataArity);
        for (int physicalPos = 0; physicalPos < physicalArity; physicalPos++) {
            producedRow.setField(physicalPos, physicalRow.getField(physicalPos));
        }
        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
            producedRow.setField(
                    physicalArity + metadataPos, metadataConverters[metadataPos].convert(rootRow));
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
        CanalJsonDeserializationSchema that = (CanalJsonDeserializationSchema) o;
        return Objects.equals(jsonDeserializer, that.jsonDeserializer)
                && hasMetadata == that.hasMetadata
                && Objects.equals(producedTypeInfo, that.producedTypeInfo)
                && Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && ignoreParseErrors == that.ignoreParseErrors
                && fieldCount == that.fieldCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jsonDeserializer,
                hasMetadata,
                producedTypeInfo,
                database,
                table,
                ignoreParseErrors,
                fieldCount);
    }

    // --------------------------------------------------------------------------------------------

    private static RowType createJsonRowType(
            DataType physicalDataType, List<ReadableMetadata> readableMetadata) {
        // Canal JSON contains other information, e.g. "ts", "sql", but we don't need them
        DataType root =
                DataTypes.ROW(
                        DataTypes.FIELD("data", DataTypes.ARRAY(physicalDataType)),
                        DataTypes.FIELD("old", DataTypes.ARRAY(physicalDataType)),
                        DataTypes.FIELD("type", DataTypes.STRING()),
                        ReadableMetadata.DATABASE.requiredJsonField,
                        ReadableMetadata.TABLE.requiredJsonField);
        // append fields that are required for reading metadata in the root
        final List<DataTypes.Field> rootMetadataFields =
                readableMetadata.stream()
                        .filter(m -> m != ReadableMetadata.DATABASE && m != ReadableMetadata.TABLE)
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
