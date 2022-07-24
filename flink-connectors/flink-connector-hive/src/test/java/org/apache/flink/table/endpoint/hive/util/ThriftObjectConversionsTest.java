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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.operation.OperationType;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationState;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toOperationHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toSessionHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTOperationHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTOperationState;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTSessionHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTTableSchema;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.CANCELED;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.CLOSED;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.ERROR;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.FINISHED;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.INITIALIZED;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.PENDING;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.RUNNING;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link ThriftObjectConversions}. */
class ThriftObjectConversionsTest {

    @Test
    public void testConvertSessionHandle() {
        SessionHandle originSessionHandle = SessionHandle.create();
        assertEquals(toSessionHandle(toTSessionHandle(originSessionHandle)), originSessionHandle);
    }

    @Test
    public void testConvertSessionHandleAndOperationHandle() {
        SessionHandle originSessionHandle = SessionHandle.create();
        OperationHandle originOperationHandle = OperationHandle.create();
        TOperationHandle tOperationHandle =
                toTOperationHandle(
                        originSessionHandle, originOperationHandle, OperationType.UNKNOWN);

        assertEquals(toSessionHandle(tOperationHandle), originSessionHandle);
        assertEquals(toOperationHandle(tOperationHandle), originOperationHandle);
    }

    @Test
    public void testConvertOperationStatus() {
        Map<OperationStatus, TOperationState> expectedMappings = new HashMap<>();
        expectedMappings.put(INITIALIZED, TOperationState.INITIALIZED_STATE);
        expectedMappings.put(PENDING, TOperationState.PENDING_STATE);
        expectedMappings.put(RUNNING, TOperationState.RUNNING_STATE);
        expectedMappings.put(FINISHED, TOperationState.FINISHED_STATE);
        expectedMappings.put(CANCELED, TOperationState.CANCELED_STATE);
        expectedMappings.put(CLOSED, TOperationState.CLOSED_STATE);
        expectedMappings.put(ERROR, TOperationState.ERROR_STATE);
        expectedMappings.put(TIMEOUT, TOperationState.TIMEDOUT_STATE);

        for (OperationStatus status : expectedMappings.keySet()) {
            assertEquals(expectedMappings.get(status), toTOperationState(status));
        }
    }

    @Test
    public void testToTTableSchema() {
        for (DataTypeSpec spec : getDataTypeSpecs()) {
            TableSchema actual =
                    new TableSchema(
                            toTTableSchema(
                                    DataTypeUtils.expandCompositeTypeToSchema(spec.flinkType)));
            List<Integer> javaSqlTypes =
                    Arrays.stream(actual.toTypeDescriptors())
                            .map(desc -> desc.getType().toJavaSQLType())
                            .collect(Collectors.toList());

            assertEquals(Collections.singletonList(spec.sqlType), javaSqlTypes);
        }
    }

    // --------------------------------------------------------------------------------------------

    private List<DataTypeSpec> getDataTypeSpecs() {
        return Arrays.asList(
                DataTypeSpec.newSpec().withType(BOOLEAN()).expectSqlType(Types.BOOLEAN),
                DataTypeSpec.newSpec()
                        .withType(TINYINT())
                        // TINYINT is the alias of the BYTE in Hive.
                        .expectSqlType(Types.BINARY),
                DataTypeSpec.newSpec().withType(SMALLINT()).expectSqlType(Types.SMALLINT),
                DataTypeSpec.newSpec().withType(INT()).expectSqlType(Types.INTEGER),
                DataTypeSpec.newSpec().withType(BIGINT()).expectSqlType(Types.BIGINT),
                DataTypeSpec.newSpec().withType(FLOAT()).expectSqlType(Types.FLOAT),
                DataTypeSpec.newSpec().withType(DOUBLE()).expectSqlType(Types.DOUBLE),
                DataTypeSpec.newSpec().withType(DECIMAL(9, 6)).expectSqlType(Types.DECIMAL),
                DataTypeSpec.newSpec().withType(STRING()).expectSqlType(Types.VARCHAR),
                DataTypeSpec.newSpec().withType(BYTES()).expectSqlType(Types.BINARY),
                DataTypeSpec.newSpec().withType(DATE()).expectSqlType(Types.DATE),
                DataTypeSpec.newSpec().withType(TIMESTAMP(4)).expectSqlType(Types.TIMESTAMP),
                DataTypeSpec.newSpec()
                        .withType(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        .expectSqlType(Types.JAVA_OBJECT),
                DataTypeSpec.newSpec()
                        .withType(
                                DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())))
                        // Hive uses STRING type
                        .expectSqlType(Types.VARCHAR));
    }

    private static class DataTypeSpec {
        DataType flinkType;
        Integer sqlType;
        RowData flinkValue;

        public static DataTypeSpec newSpec() {
            DataTypeSpec spec = new DataTypeSpec();
            spec.flinkValue = new GenericRowData(1);
            return spec;
        }

        public DataTypeSpec withType(DataType flinkType) {
            this.flinkType = DataTypes.ROW(flinkType);
            return this;
        }

        public DataTypeSpec expectSqlType(int sqlType) {
            this.sqlType = sqlType;
            return this;
        }
    }
}
