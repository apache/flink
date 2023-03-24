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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;

import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.operation.ClassicTableTypeMapping.ClassicTableTypes;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationState;
import org.apache.hive.service.rpc.thrift.TOperationType;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
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
import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toColumnBasedSet;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toFlinkTableKinds;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toOperationHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toRowBasedSet;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toSessionHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTOperationHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTOperationState;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTRowSet;
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
import static org.apache.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10;
import static org.apache.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V3;
import static org.apache.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ThriftObjectConversions}. */
class ThriftObjectConversionsTest {

    @Test
    public void testConvertSessionHandle() {
        SessionHandle originSessionHandle = SessionHandle.create();
        assertThat(toSessionHandle(toTSessionHandle(originSessionHandle)))
                .isEqualTo(originSessionHandle);
    }

    @Test
    public void testConvertSessionHandleAndOperationHandle() {
        SessionHandle originSessionHandle = SessionHandle.create();
        OperationHandle originOperationHandle = OperationHandle.create();
        TOperationHandle tOperationHandle =
                toTOperationHandle(
                        originSessionHandle, originOperationHandle, TOperationType.UNKNOWN);

        assertThat(toSessionHandle(tOperationHandle)).isEqualTo(originSessionHandle);
        assertThat(toOperationHandle(tOperationHandle)).isEqualTo(originOperationHandle);
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
            assertThat(expectedMappings.get(status)).isEqualTo(toTOperationState(status));
        }
    }

    @ParameterizedTest
    @MethodSource("getDataTypeSpecs")
    public void testToTTableSchema(DataTypeSpec spec) {
        TableSchema actual =
                new TableSchema(
                        toTTableSchema(DataTypeUtils.expandCompositeTypeToSchema(spec.flinkType)));
        List<Integer> javaSqlTypes =
                Arrays.stream(actual.toTypeDescriptors())
                        .map(desc -> desc.getType().toJavaSQLType())
                        .collect(Collectors.toList());

        assertThat(Collections.singletonList(spec.sqlType)).isEqualTo(javaSqlTypes);
    }

    @ParameterizedTest
    @MethodSource("getDataTypeSpecs")
    public void testResultSetToColumnBasedRowSet(DataTypeSpec spec) throws Exception {
        List<LogicalType> fieldTypes = spec.flinkType.getLogicalType().getChildren();
        TRowSet tRowSet =
                toColumnBasedSet(
                        fieldTypes,
                        IntStream.range(0, fieldTypes.size())
                                .mapToObj(
                                        pos -> RowData.createFieldGetter(fieldTypes.get(pos), pos))
                                .collect(Collectors.toList()),
                        Arrays.asList(spec.flinkValue, new GenericRowData(1)));
        RowSet rowSet = RowSetFactory.create(tRowSet, HIVE_CLI_SERVICE_PROTOCOL_V10);
        Iterator<Object[]> iterator = rowSet.iterator();

        assertThat(spec.convertedColumnBasedValue).isEqualTo(iterator.next()[0]);
        assertThat(spec.convertedNullValue).isEqualTo(iterator.next()[0]);
    }

    @ParameterizedTest
    @MethodSource("getDataTypeSpecs")
    public void testResultSetToRowBasedRowSet(DataTypeSpec spec) throws Exception {
        List<LogicalType> fieldTypes = spec.flinkType.getLogicalType().getChildren();
        TRowSet tRowSet =
                toRowBasedSet(
                        fieldTypes,
                        IntStream.range(0, fieldTypes.size())
                                .mapToObj(
                                        pos -> RowData.createFieldGetter(fieldTypes.get(pos), pos))
                                .collect(Collectors.toList()),
                        Arrays.asList(spec.flinkValue, new GenericRowData(1)));
        RowSet rowSet = RowSetFactory.create(tRowSet, HIVE_CLI_SERVICE_PROTOCOL_V3);
        Iterator<Object[]> iterator = rowSet.iterator();
        assertThat(spec.convertedRowBasedValue).isEqualTo(iterator.next()[0]);
        assertThat(spec.convertedNullValue).isEqualTo(iterator.next()[0]);
    }

    @Test
    public void testClassicTableTypeToFlinkTableKind() {
        assertHiveTableTypeToFlinkTableKind(ClassicTableTypes.TABLE.name(), TableKind.TABLE);
        assertHiveTableTypeToFlinkTableKind(ClassicTableTypes.VIEW.name(), TableKind.VIEW);
    }

    @Test
    public void testHiveTableTypeToFlinkTableKind() {
        assertHiveTableTypeToFlinkTableKind("MANAGED_TABLE", TableKind.TABLE);
        assertHiveTableTypeToFlinkTableKind("EXTERNAL_TABLE", TableKind.TABLE);
        assertHiveTableTypeToFlinkTableKind("INDEX_TABLE", TableKind.TABLE);
        assertHiveTableTypeToFlinkTableKind("VIRTUAL_VIEW", TableKind.VIEW);
    }

    // --------------------------------------------------------------------------------------------
    // Negative tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testSerializeRowDataWithRowKind() {
        for (RowKind kind :
                Arrays.asList(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
            assertThatThrownBy(
                            () ->
                                    toTRowSet(
                                            HIVE_CLI_SERVICE_PROTOCOL_V5,
                                            ResolvedSchema.of(
                                                    Column.physical("f0", DataTypes.INT())),
                                            Collections.singletonList(
                                                    GenericRowData.ofKind(kind, 1))))
                    .satisfies(
                            anyCauseMatches(
                                    UnsupportedOperationException.class,
                                    "HiveServer2 Endpoint only supports to serialize the INSERT-ONLY RowData."));
        }
    }

    @Test
    public void testUnsupportedHiveTableTypeToFlinkTableKind() {
        assertThatThrownBy(
                        () ->
                                toFlinkTableKinds(
                                        Collections.singletonList(
                                                ClassicTableTypes.MATERIALIZED_VIEW.name())))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Can not find the mapping from the TableType 'MATERIALIZED_VIEW' to the Flink TableKind."
                                        + " Please remove it from the specified tableTypes."));
    }

    // --------------------------------------------------------------------------------------------

    private static List<DataTypeSpec> getDataTypeSpecs() {
        Map<StringData, StringData> map = new HashMap<>();
        map.put(StringData.fromString("World"), StringData.fromString("Hello"));
        map.put(StringData.fromString("Hello"), StringData.fromString("World"));
        MapData mapData = new GenericMapData(map);
        return Arrays.asList(
                DataTypeSpec.newSpec()
                        .forType(BOOLEAN())
                        .forValue(Boolean.TRUE)
                        .expectSqlType(Types.BOOLEAN),
                DataTypeSpec.newSpec()
                        .forType(TINYINT())
                        .forValue((byte) 3)
                        // TINYINT is the alias of the BYTE in Hive.
                        .expectSqlType(Types.BINARY),
                DataTypeSpec.newSpec()
                        .forType(SMALLINT())
                        .forValue((short) 255)
                        .expectSqlType(Types.SMALLINT),
                DataTypeSpec.newSpec().forType(INT()).forValue(1994).expectSqlType(Types.INTEGER),
                DataTypeSpec.newSpec()
                        .forType(BIGINT())
                        .forValue(13214991L)
                        .expectSqlType(Types.BIGINT),
                DataTypeSpec.newSpec()
                        .forType(FLOAT())
                        .forValue(1024.0f)
                        .expectSqlType(Types.FLOAT)
                        .expectValue(1024.0),
                DataTypeSpec.newSpec()
                        .forType(DOUBLE())
                        .forValue(2048.1024)
                        .expectSqlType(Types.DOUBLE),
                DataTypeSpec.newSpec()
                        .forType(DECIMAL(9, 6))
                        .forValue(DecimalData.fromBigDecimal(new BigDecimal("123.456789"), 9, 6))
                        .expectSqlType(Types.DECIMAL)
                        .expectValue("123.456789"),
                DataTypeSpec.newSpec()
                        .forType(STRING())
                        .forValue(StringData.fromString("Hello World"))
                        .expectSqlType(Types.VARCHAR)
                        .expectValue("Hello World"),
                DataTypeSpec.newSpec()
                        .forType(BYTES())
                        .forValue("Flink SQL Gateway".getBytes(StandardCharsets.UTF_8))
                        .expectSqlType(Types.BINARY)
                        .expectValue(
                                new String("Flink SQL Gateway".getBytes(StandardCharsets.UTF_8)),
                                "Flink SQL Gateway".getBytes(StandardCharsets.UTF_8)),
                DataTypeSpec.newSpec()
                        .forType(DATE())
                        .forValue((int) LocalDate.parse("2022-02-22").toEpochDay())
                        .expectSqlType(Types.DATE)
                        .expectValue("2022-02-22"),
                DataTypeSpec.newSpec()
                        .forType(TIMESTAMP(4))
                        .forValue(
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.parse("2020-05-11T12:00:12.1234")))
                        .expectSqlType(Types.TIMESTAMP)
                        .expectValue("2020-05-11 12:00:12.1234"),
                DataTypeSpec.newSpec()
                        .forType(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .forValue(mapData)
                        .expectSqlType(Types.JAVA_OBJECT)
                        .expectValue("{\"Hello\":\"World\",\"World\":\"Hello\"}")
                        .expectNullValue("null"),
                DataTypeSpec.newSpec()
                        .forType(
                                DataTypes.ARRAY(
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())))
                        .forValue(new GenericArrayData(new Object[] {mapData, mapData}))
                        // Hive uses STRING type
                        .expectSqlType(Types.VARCHAR)
                        .expectValue(
                                "[{\"Hello\":\"World\",\"World\":\"Hello\"},{\"Hello\":\"World\",\"World\":\"Hello\"}]")
                        .expectNullValue("null"));
    }

    private void assertHiveTableTypeToFlinkTableKind(String tableType, TableKind kind) {
        assertThat(toFlinkTableKinds(Collections.singletonList(tableType)))
                .isEqualTo(Collections.singleton(kind));
    }

    private static class DataTypeSpec {
        DataType flinkType;
        Integer sqlType;
        RowData flinkValue;
        Object convertedColumnBasedValue;
        Object convertedRowBasedValue;
        Object convertedNullValue;

        public static DataTypeSpec newSpec() {
            DataTypeSpec spec = new DataTypeSpec();
            spec.flinkValue = new GenericRowData(1);
            return spec;
        }

        public DataTypeSpec forType(DataType flinkType) {
            this.flinkType = DataTypes.ROW(flinkType);
            return this;
        }

        public DataTypeSpec expectSqlType(int sqlType) {
            this.sqlType = sqlType;
            return this;
        }

        public DataTypeSpec forValue(Object flinkValue) {
            this.flinkValue = GenericRowData.of(flinkValue);
            this.convertedColumnBasedValue = flinkValue;
            this.convertedRowBasedValue = flinkValue;
            return this;
        }

        public DataTypeSpec expectNullValue(Object convertedNullValue) {
            this.convertedNullValue = convertedNullValue;
            return this;
        }

        public DataTypeSpec expectValue(Object rowAndColumnBasedValue) {
            this.convertedColumnBasedValue = rowAndColumnBasedValue;
            this.convertedRowBasedValue = rowAndColumnBasedValue;
            return this;
        }

        public DataTypeSpec expectValue(
                Object convertedRowBasedValue, Object convertedColumnBasedValue) {
            this.convertedRowBasedValue = convertedRowBasedValue;
            this.convertedColumnBasedValue = convertedColumnBasedValue;
            return this;
        }

        @Override
        public String toString() {
            // Only print flink type only for clear.
            return "DataTypeSpec{" + "flinkType=" + flinkType + '}';
        }
    }
}
