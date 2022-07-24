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

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.operation.OperationType;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.rpc.thrift.TCLIServiceConstants;
import org.apache.hive.service.rpc.thrift.TColumnDesc;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationState;
import org.apache.hive.service.rpc.thrift.TOperationType;
import org.apache.hive.service.rpc.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.hive.service.rpc.thrift.TTableSchema;
import org.apache.hive.service.rpc.thrift.TTypeDesc;
import org.apache.hive.service.rpc.thrift.TTypeEntry;
import org.apache.hive.service.rpc.thrift.TTypeQualifierValue;
import org.apache.hive.service.rpc.thrift.TTypeQualifiers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Conversion between thrift object and flink object. */
public class ThriftObjectConversions {

    private static final UUID SECRET_ID = UUID.fromString("b06fa16a-3d16-475f-b510-6c64abb9b173");

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
            OperationType operationType) {
        return new TOperationHandle(
                toTHandleIdentifier(operationHandle.getIdentifier(), sessionHandle.getIdentifier()),
                toTOperationType(operationType),
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

    public static TOperationType toTOperationType(OperationType type) {
        switch (type) {
            case EXECUTE_STATEMENT:
                return TOperationType.EXECUTE_STATEMENT;
            case UNKNOWN:
                return TOperationType.UNKNOWN;
            default:
                throw new IllegalArgumentException(
                        String.format("Unknown operation type: %s.", type));
        }
    }

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
