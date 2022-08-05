/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.endpoint.hive;

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.endpoint.hive.util.HiveServer2EndpointExtension;
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.operation.OperationType;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.session.SessionManager;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hive.jdbc.JdbcColumn;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCancelOperationReq;
import org.apache.hive.service.rpc.thrift.TCancelOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseOperationReq;
import org.apache.hive.service.rpc.thrift.TCloseOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TCloseSessionResp;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.api.config.TableConfigOptions.MAX_LENGTH_GENERATED_CODE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTOperationHandle;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link HiveServer2Endpoint}. */
public class HiveServer2EndpointITCase extends TestLogger {

    @RegisterExtension
    @Order(1)
    public static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    @RegisterExtension
    @Order(2)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    @RegisterExtension
    @Order(3)
    public static final HiveServer2EndpointExtension ENDPOINT_EXTENSION =
            new HiveServer2EndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    @Test
    public void testOpenCloseJdbcConnection() throws Exception {
        SessionManager sessionManager = SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager();
        int originSessionCount = sessionManager.currentSessionCount();
        try (Connection ignore = ENDPOINT_EXTENSION.getConnection()) {
            assertThat(1 + originSessionCount).isEqualTo(sessionManager.currentSessionCount());
        }
        assertThat(sessionManager.currentSessionCount()).isEqualTo(originSessionCount);
    }

    @Test
    public void testOpenSessionWithConfig() throws Exception {
        TCLIService.Client client = createClient();
        TOpenSessionReq openSessionReq = new TOpenSessionReq();

        Map<String, String> configs = new HashMap<>();
        configs.put(MAX_LENGTH_GENERATED_CODE.key(), "-1");
        // simulate to set config using hive jdbc
        configs.put("set:hiveconf:key", "value");
        // TODO: set hivevar when FLINK-28096 is fixed
        openSessionReq.setConfiguration(configs);
        TOpenSessionResp openSessionResp = client.OpenSession(openSessionReq);
        SessionHandle sessionHandle =
                ThriftObjectConversions.toSessionHandle(openSessionResp.getSessionHandle());

        Map<String, String> actualConfig =
                SQL_GATEWAY_SERVICE_EXTENSION.getService().getSessionConfig(sessionHandle);
        assertThat(actualConfig.entrySet())
                .contains(
                        new AbstractMap.SimpleEntry<>(
                                TableConfigOptions.TABLE_SQL_DIALECT.key(), SqlDialect.HIVE.name()),
                        new AbstractMap.SimpleEntry<>(TABLE_DML_SYNC.key(), "true"),
                        new AbstractMap.SimpleEntry<>(RUNTIME_MODE.key(), BATCH.name()),
                        new AbstractMap.SimpleEntry<>(MAX_LENGTH_GENERATED_CODE.key(), "-1"),
                        new AbstractMap.SimpleEntry<>("key", "value"));
    }

    @Test
    public void testGetException() throws Exception {
        TCLIService.Client client = createClient();
        TCloseSessionReq closeSessionReq = new TCloseSessionReq();
        SessionHandle sessionHandle = SessionHandle.create();
        closeSessionReq.setSessionHandle(ThriftObjectConversions.toTSessionHandle(sessionHandle));
        TCloseSessionResp resp = client.CloseSession(closeSessionReq);
        assertThat(resp.getStatus().getStatusCode()).isEqualTo(TStatusCode.ERROR_STATUS);
        assertThat(resp.getStatus().getInfoMessages())
                .anyMatch(
                        error ->
                                error.contains(
                                        String.format(
                                                "Session '%s' does not exist", sessionHandle)));
    }

    @Test
    public void testCancelOperation() throws Exception {
        runOperationRequest(
                tOperationHandle -> {
                    TCancelOperationResp tCancelOperationResp =
                            ENDPOINT_EXTENSION
                                    .getEndpoint()
                                    .CancelOperation(new TCancelOperationReq(tOperationHandle));
                    assertThat(tCancelOperationResp.getStatus().getStatusCode())
                            .isEqualTo(TStatusCode.SUCCESS_STATUS);
                },
                ((sessionHandle, operationHandle) ->
                        assertThat(
                                        SQL_GATEWAY_SERVICE_EXTENSION
                                                .getService()
                                                .getOperationInfo(sessionHandle, operationHandle)
                                                .getStatus())
                                .isEqualTo(OperationStatus.CANCELED)));
    }

    @Test
    public void testCloseOperation() throws Exception {
        runOperationRequest(
                tOperationHandle -> {
                    TCloseOperationResp resp =
                            ENDPOINT_EXTENSION
                                    .getEndpoint()
                                    .CloseOperation(new TCloseOperationReq(tOperationHandle));
                    assertThat(resp.getStatus().getStatusCode())
                            .isEqualTo(TStatusCode.SUCCESS_STATUS);
                },
                ((sessionHandle, operationHandle) ->
                        assertThatThrownBy(
                                        () ->
                                                SQL_GATEWAY_SERVICE_EXTENSION
                                                        .getService()
                                                        .getOperationInfo(
                                                                sessionHandle, operationHandle))
                                .satisfies(
                                        FlinkAssertions.anyCauseMatches(
                                                SqlGatewayException.class,
                                                String.format(
                                                        "Can not find the submitted operation in the OperationManager with the %s",
                                                        operationHandle)))));
    }

    @Test
    public void testGetCatalogs() throws Exception {
        runGetObjectTest(
                connection -> connection.getMetaData().getCatalogs(),
                ResolvedSchema.of(Column.physical("TABLE_CAT", DataTypes.STRING())),
                Arrays.asList(
                        Collections.singletonList("hive"),
                        Collections.singletonList("default_catalog")));
    }

    @Test
    public void testGetSchemas() throws Exception {
        runGetObjectTest(
                connection -> connection.getMetaData().getSchemas("default_catalog", null),
                ResolvedSchema.of(
                        Column.physical("TABLE_SCHEMA", DataTypes.STRING()),
                        Column.physical("TABLE_CAT", DataTypes.STRING())),
                Arrays.asList(
                        Arrays.asList("default_database", "default_catalog"),
                        Arrays.asList("db_test1", "default_catalog"),
                        Arrays.asList("db_test2", "default_catalog"),
                        Arrays.asList("db_diff", "default_catalog")));
    }

    @Test
    public void testGetSchemasWithPattern() throws Exception {
        runGetObjectTest(
                connection -> connection.getMetaData().getSchemas(null, "db\\_test%"),
                ResolvedSchema.of(
                        Column.physical("TABLE_SCHEMA", DataTypes.STRING()),
                        Column.physical("TABLE_CAT", DataTypes.STRING())),
                Arrays.asList(
                        Arrays.asList("db_test1", "default_catalog"),
                        Arrays.asList("db_test2", "default_catalog")));
    }

    // --------------------------------------------------------------------------------------------

    private Connection getInitializedConnection() throws Exception {
        Connection connection = ENDPOINT_EXTENSION.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("SET table.sql-dialect=default");
        statement.execute("USE CATALOG `default_catalog`");

        // default_catalog: db_test1 | db_test2 | db_diff | default
        //     db_test1: temporary table tb_1, table tb_2, temporary view tb_3, view tb_4
        //     db_test2: table tb_1, table diff_1, view tb_2, view diff_2
        //     db_diff:  table tb_1, view tb_2

        statement.execute("CREATE DATABASE db_test1");
        statement.execute("CREATE DATABASE db_test2");
        statement.execute("CREATE DATABASE db_diff");

        statement.execute("CREATE TEMPORARY TABLE db_test1.tb_1 COMMENT 'temporary table tb_1'");
        statement.execute("CREATE TABLE db_test1.tb_2 COMMENT 'table tb_2'");
        statement.execute(
                "CREATE TEMPORARY VIEW db_test1.tb_3 COMMENT 'temporary view tb_3' AS SELECT 1");
        statement.execute("CREATE VIEW db_test1.tb_4 COMMENT 'view tb_4' AS SELECT 1");

        statement.execute("CREATE TABLE db_test2.tb_1 COMMENT 'table tb_1'");
        statement.execute("CREATE TABLE db_test2.diff_1 COMMENT 'table diff_1'");
        statement.execute("CREATE VIEW db_test2.tb_2 COMMENT 'view tb_2' AS SELECT 1");
        statement.execute("CREATE VIEW db_test2.diff_2 COMMENT 'view diff_2' AS SELECT 1");

        statement.execute("CREATE TABLE db_diff.tb_1 COMMENT 'table tb_1'");
        statement.execute("CREATE VIEW db_diff.tb_2 COMMENT 'view tb_2' AS SELECT 1");

        statement.close();
        return connection;
    }

    private void runGetObjectTest(
            FunctionWithException<Connection, java.sql.ResultSet, Exception> resultSetSupplier,
            ResolvedSchema expectedSchema,
            List<List<Object>> expectedResults)
            throws Exception {
        try (Connection connection = getInitializedConnection();
                java.sql.ResultSet result = resultSetSupplier.apply(connection)) {
            assertSchemaEquals(expectedSchema, result.getMetaData());
            assertThat(new HashSet<>(collect(result, expectedSchema.getColumnCount())))
                    .isEqualTo(new HashSet<>(expectedResults));
        }
    }

    private void runOperationRequest(
            ThrowingConsumer<TOperationHandle, Exception> manipulateOp,
            BiConsumerWithException<SessionHandle, OperationHandle, Exception> operationValidator)
            throws Exception {
        SessionHandle sessionHandle =
                SQL_GATEWAY_SERVICE_EXTENSION
                        .getService()
                        .openSession(
                                SessionEnvironment.newBuilder()
                                        .setSessionEndpointVersion(
                                                HiveServer2EndpointVersion
                                                        .HIVE_CLI_SERVICE_PROTOCOL_V10)
                                        .build());
        CountDownLatch latch = new CountDownLatch(1);
        OperationHandle operationHandle =
                SQL_GATEWAY_SERVICE_EXTENSION
                        .getService()
                        .submitOperation(
                                sessionHandle,
                                OperationType.UNKNOWN,
                                () -> {
                                    latch.await();
                                    return ResultSet.NOT_READY_RESULTS;
                                });
        manipulateOp.accept(
                toTOperationHandle(sessionHandle, operationHandle, OperationType.UNKNOWN));
        operationValidator.accept(sessionHandle, operationHandle);
        SQL_GATEWAY_SERVICE_EXTENSION.getService().closeSession(sessionHandle);
    }

    private TCLIService.Client createClient() throws Exception {
        TTransport transport =
                HiveAuthUtils.getSocketTransport(
                        InetAddress.getLocalHost().getHostAddress(),
                        ENDPOINT_EXTENSION.getPort(),
                        0);
        transport.open();
        return new TCLIService.Client(new TBinaryProtocol(transport));
    }

    private void assertSchemaEquals(ResolvedSchema expected, ResultSetMetaData metaData)
            throws Exception {
        assertThat(metaData.getColumnCount()).isEqualTo(expected.getColumnCount());
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            Column column =
                    expected.getColumn(i - 1)
                            .orElseThrow(() -> new RuntimeException("Can not get column."));
            assertThat(metaData.getColumnName(i)).isEqualTo(column.getName());
            int jdbcType =
                    JdbcColumn.hiveTypeToSqlType(
                            HiveTypeUtil.toHiveTypeInfo(column.getDataType(), false).getTypeName());
            assertThat(metaData.getColumnType(i)).isEqualTo(jdbcType);
        }
    }

    private List<List<Object>> collect(java.sql.ResultSet result, int columnCount)
            throws Exception {
        List<List<Object>> actual = new ArrayList<>();
        while (result.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(result.getObject(i));
            }
            actual.add(row);
        }
        return actual;
    }
}
