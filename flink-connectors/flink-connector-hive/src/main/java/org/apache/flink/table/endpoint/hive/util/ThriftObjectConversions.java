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

package org.apache.flink.table.endpoint.hive.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.results.FetchOrientation;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.rpc.thrift.TBinaryColumn;
import org.apache.hive.service.rpc.thrift.TBoolColumn;
import org.apache.hive.service.rpc.thrift.TBoolValue;
import org.apache.hive.service.rpc.thrift.TByteColumn;
import org.apache.hive.service.rpc.thrift.TByteValue;
import org.apache.hive.service.rpc.thrift.TCLIServiceConstants;
import org.apache.hive.service.rpc.thrift.TColumn;
import org.apache.hive.service.rpc.thrift.TColumnDesc;
import org.apache.hive.service.rpc.thrift.TColumnValue;
import org.apache.hive.service.rpc.thrift.TDoubleColumn;
import org.apache.hive.service.rpc.thrift.TDoubleValue;
import org.apache.hive.service.rpc.thrift.TFetchOrientation;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.apache.hive.service.rpc.thrift.TI16Column;
import org.apache.hive.service.rpc.thrift.TI16Value;
import org.apache.hive.service.rpc.thrift.TI32Column;
import org.apache.hive.service.rpc.thrift.TI32Value;
import org.apache.hive.service.rpc.thrift.TI64Column;
import org.apache.hive.service.rpc.thrift.TI64Value;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationState;
import org.apache.hive.service.rpc.thrift.TOperationType;
import org.apache.hive.service.rpc.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TRow;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.hive.service.rpc.thrift.TStringColumn;
import org.apache.hive.service.rpc.thrift.TStringValue;
import org.apache.hive.service.rpc.thrift.TTableSchema;
import org.apache.hive.service.rpc.thrift.TTypeDesc;
import org.apache.hive.service.rpc.thrift.TTypeEntry;
import org.apache.hive.service.rpc.thrift.TTypeQualifierValue;
import org.apache.hive.service.rpc.thrift.TTypeQualifiers;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.endpoint.hive.HiveServer2EndpointVersion.HIVE_CLI_SERVICE_PROTOCOL_V6;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.CONSTRUCTED;

/** Conversion between thrift object and flink object. */
public class ThriftObjectConversions {

    private static final UUID SECRET_ID = UUID.fromString("b06fa16a-3d16-475f-b510-6c64abb9b173");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final RowDataToJsonConverters TO_JSON_CONVERTERS =
            new RowDataToJsonConverters(
                    TimestampFormat.SQL, JsonFormatOptions.MapNullKeyMode.LITERAL, "null");
    private static final Map<String, TableKind> TABLE_TYPE_MAPPINGS = buildTableTypeMapping();

    // --------------------------------------------------------------------------------------------
    // Flink SessionHandle from/to Hive SessionHandle
    // --------------------------------------------------------------------------------------------

    public static TSessionHandle toTSessionHandle(SessionHandle sessionHandle) {
        return new TSessionHandle(toTHandleIdentifier(sessionHandle.getIdentifier(), SECRET_ID));
    }

    public static SessionHandle toSessionHandle(TSessionHandle tSessionHandle) {
        ByteBuffer bb = ByteBuffer.wrap(tSessionHandle.getSessionId().getGuid());
        return new SessionHandle(new UUID(bb.getLong(), bb.getLong()));
    }

    // --------------------------------------------------------------------------------------------
    // Flink SessionHandle && OperationHandle from/to Hive OperationHandle
    // --------------------------------------------------------------------------------------------

    /**
     * Convert {@link SessionHandle} and {@link OperationHandle} to {@link TOperationHandle}.
     *
     * <p>Hive uses {@link TOperationHandle} to retrieve the {@code Operation} related information.
     * However, SqlGateway uses {@link SessionHandle} and {@link OperationHandle} to determine.
     * Therefore, the {@link TOperationHandle} needs to contain both {@link SessionHandle} and
     * {@link OperationHandle}.
     *
     * <p>Currently all operations in the {@link SqlGatewayService} has data. Therefore, set the
     * {@code TOperationHandle#hasResultSet} true.
     */
    public static TOperationHandle toTOperationHandle(
            SessionHandle sessionHandle,
            OperationHandle operationHandle,
            TOperationType operationType) {
        return new TOperationHandle(
                toTHandleIdentifier(operationHandle.getIdentifier(), sessionHandle.getIdentifier()),
                operationType,
                true);
    }

    public static SessionHandle toSessionHandle(TOperationHandle tOperationHandle) {
        ByteBuffer bb = ByteBuffer.wrap(tOperationHandle.getOperationId().getSecret());
        return new SessionHandle(new UUID(bb.getLong(), bb.getLong()));
    }

    public static OperationHandle toOperationHandle(TOperationHandle tOperationHandle) {
        ByteBuffer bb = ByteBuffer.wrap(tOperationHandle.getOperationId().getGuid());
        return new OperationHandle(new UUID(bb.getLong(), bb.getLong()));
    }

    // --------------------------------------------------------------------------------------------
    // Operation related conversions
    // --------------------------------------------------------------------------------------------

    public static TOperationState toTOperationState(OperationStatus operationStatus) {
        switch (operationStatus) {
            case INITIALIZED:
                return TOperationState.INITIALIZED_STATE;
            case PENDING:
                return TOperationState.PENDING_STATE;
            case RUNNING:
                return TOperationState.RUNNING_STATE;
            case FINISHED:
                return TOperationState.FINISHED_STATE;
            case ERROR:
                return TOperationState.ERROR_STATE;
            case TIMEOUT:
                return TOperationState.TIMEDOUT_STATE;
            case CANCELED:
                return TOperationState.CANCELED_STATE;
            case CLOSED:
                return TOperationState.CLOSED_STATE;
            default:
                throw new IllegalArgumentException(
                        String.format("Unknown operation status: %s.", operationStatus));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Statement related conversions
    // --------------------------------------------------------------------------------------------

    public static FetchOrientation toFetchOrientation(int fetchOrientation) {
        if (fetchOrientation == TFetchOrientation.FETCH_NEXT.getValue()) {
            return FetchOrientation.FETCH_NEXT;
        } else if (fetchOrientation == TFetchOrientation.FETCH_PRIOR.getValue()) {
            return FetchOrientation.FETCH_PRIOR;
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported fetch orientation: %s.", fetchOrientation));
        }
    }

    /** Similar logic in the {@code org.apache.hive.service.cli.ColumnDescriptor}. */
    public static TTableSchema toTTableSchema(ResolvedSchema schema) {
        TTableSchema tSchema = new TTableSchema();

        for (int i = 0; i < schema.getColumnCount(); i++) {
            Column column = schema.getColumns().get(i);
            TColumnDesc desc = new TColumnDesc();
            desc.setColumnName(column.getName());
            column.getComment().ifPresent(desc::setComment);
            desc.setPosition(i);

            TTypeDesc typeDesc = new TTypeDesc();

            // Hive uses the TPrimitiveTypeEntry only. Please refer to TypeDescriptor#toTTypeDesc.
            DataType columnType = column.getDataType();
            TPrimitiveTypeEntry typeEntry =
                    new TPrimitiveTypeEntry(
                            Type.getType(HiveTypeUtil.toHiveTypeInfo(columnType, false)).toTType());

            if (hasTypeQualifiers(columnType.getLogicalType())) {
                typeEntry.setTypeQualifiers(toTTypeQualifiers(columnType.getLogicalType()));
            }
            typeDesc.addToTypes(TTypeEntry.primitiveEntry(typeEntry));

            desc.setTypeDesc(typeDesc);
            tSchema.addToColumns(desc);
        }
        return tSchema;
    }

    /**
     * Similar to {@link SerDeUtils#toThriftPayload(Object, ObjectInspector, int)} that converts the
     * returned Rows to JSON string. The only difference is the current implementation also keep the
     * type for primitive type.
     */
    public static TRowSet toTRowSet(
            TProtocolVersion version, ResolvedSchema schema, List<RowData> data) {
        for (RowData row : data) {
            if (row.getRowKind() != RowKind.INSERT) {
                throw new UnsupportedOperationException(
                        "HiveServer2 Endpoint only supports to serialize the INSERT-ONLY RowData.");
            }
        }

        List<RowData.FieldGetter> fieldGetters = new ArrayList<>();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            fieldGetters.add(
                    RowData.createFieldGetter(
                            schema.getColumnDataTypes().get(i).getLogicalType(), i));
        }

        List<LogicalType> fieldTypes =
                schema.getColumnDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList());
        if (version.getValue() < HIVE_CLI_SERVICE_PROTOCOL_V6.getVersion().getValue()) {
            return toRowBasedSet(fieldTypes, fieldGetters, data);
        } else {
            return toColumnBasedSet(fieldTypes, fieldGetters, data);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Catalog API related conversions
    // --------------------------------------------------------------------------------------------

    /** Counterpart of the {@code org.apache.hive.service.cli.operation.TableTypeMapping}. */
    public static Set<TableKind> toFlinkTableKinds(@Nullable List<String> tableTypes) {
        Set<TableKind> tableKinds = new HashSet<>();
        if (tableTypes == null || tableTypes.isEmpty()) {
            tableKinds.addAll(Arrays.asList(TableKind.values()));
            return tableKinds;
        }

        for (String tableType : tableTypes) {
            if (!TABLE_TYPE_MAPPINGS.containsKey(tableType)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Can not find the mapping from the TableType '%s' to the Flink TableKind. Please remove it from the specified tableTypes.",
                                tableType));
            }
            tableKinds.add(TABLE_TYPE_MAPPINGS.get(tableType));
        }
        return tableKinds;
    }

    public static TStatus toTStatus(Throwable t) {
        TStatus tStatus = new TStatus(TStatusCode.ERROR_STATUS);
        tStatus.setErrorMessage(t.getMessage());
        tStatus.setInfoMessages(toString(t));
        return tStatus;
    }

    // --------------------------------------------------------------------------------------------

    private static THandleIdentifier toTHandleIdentifier(UUID publicId, UUID secretId) {
        byte[] guid = new byte[16];
        byte[] secret = new byte[16];
        ByteBuffer guidBB = ByteBuffer.wrap(guid);
        ByteBuffer secretBB = ByteBuffer.wrap(secret);

        guidBB.putLong(publicId.getMostSignificantBits());
        guidBB.putLong(publicId.getLeastSignificantBits());
        secretBB.putLong(secretId.getMostSignificantBits());
        secretBB.putLong(secretId.getLeastSignificantBits());
        return new THandleIdentifier(ByteBuffer.wrap(guid), ByteBuffer.wrap(secret));
    }

    @VisibleForTesting
    public static TRowSet toColumnBasedSet(
            List<LogicalType> fieldTypes,
            List<RowData.FieldGetter> fieldGetters,
            List<RowData> rows) {
        int rowNum = rows.size();
        // TODO: Support accurate start offset
        TRowSet rowSet = new TRowSet(0, new ArrayList<>(rowNum));
        for (int i = 0; i < fieldTypes.size(); i++) {
            RowData.FieldGetter fieldGetter = fieldGetters.get(i);
            RowDataToJsonConverters.RowDataToJsonConverter converter =
                    TO_JSON_CONVERTERS.createConverter(fieldTypes.get(i));
            rowSet.addToColumns(
                    toTColumn(
                            fieldTypes.get(i),
                            fieldGetter,
                            buildJsonValueConverter(converter),
                            rows));
        }
        return rowSet;
    }

    private static TColumn toTColumn(
            LogicalType fieldType,
            RowData.FieldGetter fieldGetter,
            Function<Object, String> jsonValue,
            List<RowData> rows) {
        BitSet nulls = new BitSet();
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return TColumn.boolVal(
                        new TBoolColumn(
                                getValues(rows, fieldGetter::getFieldOrNull, false, nulls),
                                ByteBuffer.wrap(nulls.toByteArray())));
            case TINYINT:
                return TColumn.byteVal(
                        new TByteColumn(
                                getValues(rows, fieldGetter::getFieldOrNull, (byte) 0, nulls),
                                ByteBuffer.wrap(nulls.toByteArray())));
            case SMALLINT:
                return TColumn.i16Val(
                        new TI16Column(
                                (getValues(rows, fieldGetter::getFieldOrNull, (short) 0, nulls)),
                                ByteBuffer.wrap(nulls.toByteArray())));

            case INTEGER:
                return TColumn.i32Val(
                        new TI32Column(
                                getValues(rows, fieldGetter::getFieldOrNull, 0, nulls),
                                ByteBuffer.wrap(nulls.toByteArray())));
            case BIGINT:
                return TColumn.i64Val(
                        new TI64Column(
                                getValues(rows, fieldGetter::getFieldOrNull, 0L, nulls),
                                ByteBuffer.wrap(nulls.toByteArray())));
            case FLOAT:
                return TColumn.doubleVal(
                        new TDoubleColumn(
                                getValues(rows, fieldGetter::getFieldOrNull, 0.0f, nulls).stream()
                                        .map(Float::doubleValue)
                                        .collect(Collectors.toList()),
                                ByteBuffer.wrap(nulls.toByteArray())));
            case DOUBLE:
                return TColumn.doubleVal(
                        new TDoubleColumn(
                                getValues(rows, fieldGetter::getFieldOrNull, 0.0, nulls),
                                ByteBuffer.wrap(nulls.toByteArray())));
            case BINARY:
            case VARBINARY:
                return TColumn.binaryVal(
                        new TBinaryColumn(
                                getValues(rows, fieldGetter::getFieldOrNull, new byte[0], nulls)
                                        .stream()
                                        .map(ByteBuffer::wrap)
                                        .collect(Collectors.toList()),
                                ByteBuffer.wrap(nulls.toByteArray())));
            default:
                List<String> jsonValues =
                        getValues(
                                rows,
                                row -> {
                                    Object fieldOrNull = fieldGetter.getFieldOrNull(row);
                                    if (!fieldType.is(CONSTRUCTED) && fieldOrNull == null) {
                                        return null;
                                    } else {
                                        return jsonValue.apply(fieldOrNull);
                                    }
                                },
                                "",
                                nulls);
                return TColumn.stringVal(
                        new TStringColumn(jsonValues, ByteBuffer.wrap(nulls.toByteArray())));
        }
    }

    @VisibleForTesting
    public static TRowSet toRowBasedSet(
            List<LogicalType> fieldTypes,
            List<RowData.FieldGetter> fieldGetters,
            List<RowData> rows) {
        TRowSet rowSet = new TRowSet();
        List<RowDataToJsonConverters.RowDataToJsonConverter> converters =
                fieldTypes.stream()
                        .map(TO_JSON_CONVERTERS::createConverter)
                        .collect(Collectors.toList());
        for (RowData row : rows) {
            TRow convertedRow = new TRow();
            for (int i = 0; i < row.getArity(); i++) {
                Object field = fieldGetters.get(i).getFieldOrNull(row);
                RowDataToJsonConverters.RowDataToJsonConverter converter = converters.get(i);
                convertedRow.addToColVals(
                        toTColumnValue(
                                fieldTypes.get(i), field, buildJsonValueConverter(converter)));
            }
            rowSet.addToRows(convertedRow);
        }
        return rowSet;
    }

    private static TColumnValue toTColumnValue(
            LogicalType fieldType, Object value, Function<Object, String> jsonValue) {
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                {
                    TBoolValue boolValue = new TBoolValue();
                    if (value != null) {
                        boolValue.setValue((Boolean) value);
                    }
                    return TColumnValue.boolVal(boolValue);
                }
            case TINYINT:
                {
                    TByteValue byteValue = new TByteValue();
                    if (value != null) {
                        byteValue.setValue((byte) value);
                    }
                    return TColumnValue.byteVal(byteValue);
                }
            case SMALLINT:
                {
                    TI16Value tI16Value = new TI16Value();
                    if (value != null) {
                        tI16Value.setValue((Short) value);
                    }
                    return TColumnValue.i16Val(tI16Value);
                }
            case INTEGER:
                {
                    TI32Value tI32Value = new TI32Value();
                    if (value != null) {
                        tI32Value.setValue((Integer) value);
                    }
                    return TColumnValue.i32Val(tI32Value);
                }
            case BIGINT:
                {
                    TI64Value tI64Value = new TI64Value();
                    if (value != null) {
                        tI64Value.setValue((Long) value);
                    }
                    return TColumnValue.i64Val(tI64Value);
                }
            case FLOAT:
            case DOUBLE:
                {
                    TDoubleValue tDoubleValue = new TDoubleValue();
                    if (value != null) {
                        if (value instanceof Double) {
                            tDoubleValue.setValue((Double) value);
                        } else if (value instanceof Float) {
                            tDoubleValue.setValue((Float) value);
                        }
                    }
                    return TColumnValue.doubleVal(tDoubleValue);
                }
            case BINARY:
            case VARBINARY:
                // For BINARY type, convert to STRING. Please refer to SerDeUtil#toThriftPayload
                {
                    TStringValue tStringValue = new TStringValue();
                    if (value != null) {
                        tStringValue.setValue(new String((byte[]) value));
                    }
                    return TColumnValue.stringVal(tStringValue);
                }
            default:
                {
                    TStringValue jsonString = new TStringValue();
                    if (fieldType.is(CONSTRUCTED) || value != null) {
                        // If field is ARRAY/MAP/MULTISET, convert to "null".
                        jsonString.setValue(jsonValue.apply(value));
                    }
                    return TColumnValue.stringVal(jsonString);
                }
        }
    }

    /** Only the type that has length, precision or scale has {@link TTypeQualifiers}. */
    private static boolean hasTypeQualifiers(LogicalType type) {
        switch (type.getTypeRoot()) {
            case DECIMAL:
            case CHAR:
            case VARCHAR:
                return true;
            default:
                return false;
        }
    }

    /**
     * Create {@link TTypeQualifiers} from {@link LogicalType}. The logic is almost same in the
     * {@code org.apache.hive.service.cli#toTTypeQualifiers}.
     */
    private static TTypeQualifiers toTTypeQualifiers(LogicalType type) {
        Map<String, TTypeQualifierValue> qualifiers = new HashMap<>();

        switch (type.getTypeRoot()) {
            case DECIMAL:
                qualifiers.put(
                        TCLIServiceConstants.PRECISION,
                        TTypeQualifierValue.i32Value(((DecimalType) type).getPrecision()));
                qualifiers.put(
                        TCLIServiceConstants.SCALE,
                        TTypeQualifierValue.i32Value(((DecimalType) type).getScale()));
                break;
            case VARCHAR:
                qualifiers.put(
                        TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH,
                        TTypeQualifierValue.i32Value(((VarCharType) type).getLength()));
                break;
            case CHAR:
                qualifiers.put(
                        TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH,
                        TTypeQualifierValue.i32Value(((CharType) type).getLength()));
                break;
        }

        return new TTypeQualifiers(qualifiers);
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> getValues(
            List<RowData> rows,
            Function<RowData, Object> fieldGetter,
            T defaultValue,
            BitSet isNulls) {
        List<T> columnValues = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            T value = (T) fieldGetter.apply(rows.get(i));
            if (value == null) {
                isNulls.set(i, true);
                columnValues.add(defaultValue);
            } else {
                columnValues.add(value);
            }
        }
        return columnValues;
    }

    private static Function<Object, String> buildJsonValueConverter(
            RowDataToJsonConverters.RowDataToJsonConverter converter) {
        return field -> {
            JsonNode node = converter.convert(OBJECT_MAPPER, null, field);
            // The difference below is the primitive value return the actual value, e.g. STRING type
            // will get "Hello World" rather than "\"Hello World\"" but the nested type return the
            // json value.
            if (node.isValueNode()) {
                return node.asText();
            } else {
                return node.toString();
            }
        };
    }

    private static Map<String, TableKind> buildTableTypeMapping() {
        Map<String, TableKind> mappings = new ConcurrentHashMap<>();
        // Add {@code org.apache.hive.service.cli.operation.ClassicTableTypeMapping }
        mappings.put("TABLE", TableKind.TABLE);
        mappings.put("VIEW", TableKind.VIEW);
        // Add {@code org.apache.hive.service.cli.operation.HiveTableTypeMapping }
        // Use literal string instead of org.apache.hadoop.hive.metastore.TableType because hive may
        // remove some members of this enum class
        mappings.put("MANAGED_TABLE", TableKind.TABLE);
        mappings.put("EXTERNAL_TABLE", TableKind.TABLE);
        mappings.put("INDEX_TABLE", TableKind.TABLE);
        mappings.put("VIRTUAL_VIEW", TableKind.VIEW);
        return mappings;
    }

    /**
     * Converts a {@link Throwable} object into a flattened list of texts including its stack trace
     * and the stack traces of the nested causes.
     *
     * @param ex a {@link Throwable} object
     * @return a flattened list of texts including the {@link Throwable} object's stack trace and
     *     the stack traces of the nested causes.
     */
    private static List<String> toString(Throwable ex) {
        return toString(ex, null);
    }

    private static List<String> toString(Throwable cause, StackTraceElement[] parent) {
        StackTraceElement[] trace = cause.getStackTrace();
        int m = trace.length - 1;
        if (parent != null) {
            int n = parent.length - 1;
            while (m >= 0 && n >= 0 && trace[m].equals(parent[n])) {
                m--;
                n--;
            }
        }
        List<String> detail = enroll(cause, trace, m);
        cause = cause.getCause();
        if (cause != null) {
            detail.addAll(toString(cause, trace));
        }
        return detail;
    }

    private static List<String> enroll(Throwable ex, StackTraceElement[] trace, int max) {
        List<String> details = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        builder.append('*').append(ex.getClass().getName()).append(':');
        builder.append(ex.getMessage()).append(':');
        builder.append(trace.length).append(':').append(max);
        details.add(builder.toString());
        for (int i = 0; i <= max; i++) {
            builder.setLength(0);
            builder.append(trace[i].getClassName()).append(':');
            builder.append(trace[i].getMethodName()).append(':');
            String fileName = trace[i].getFileName();
            builder.append(fileName == null ? "" : fileName).append(':');
            builder.append(trace[i].getLineNumber());
            details.add(builder.toString());
        }
        return details;
    }
}
