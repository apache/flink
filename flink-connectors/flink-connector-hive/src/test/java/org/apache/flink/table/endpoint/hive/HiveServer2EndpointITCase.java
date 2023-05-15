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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.endpoint.hive.util.HiveServer2EndpointExtension;
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.result.NotReadyResult;
import org.apache.flink.table.gateway.service.session.SessionManagerImpl;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc0;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.FutureTaskWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCancelOperationReq;
import org.apache.hive.service.rpc.thrift.TCancelOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseOperationReq;
import org.apache.hive.service.rpc.thrift.TCloseOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TCloseSessionResp;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TFetchOrientation;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TGetInfoReq;
import org.apache.hive.service.rpc.thrift.TGetInfoResp;
import org.apache.hive.service.rpc.thrift.TGetInfoType;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationState;
import org.apache.hive.service.rpc.thrift.TOperationType;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.configuration.PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID;
import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.config.TableConfigOptions.MAX_LENGTH_GENERATED_CODE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_SQL_DIALECT;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toSessionHandle;
import static org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions.toTOperationHandle;
import static org.apache.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10;
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

    @BeforeAll
    public static void setup() throws Exception {
        initializeEnvironment();
    }

    @Test
    public void testOpenCloseJdbcConnection() throws Exception {
        SessionManagerImpl sessionManager =
                (SessionManagerImpl) SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager();
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
        configs.put("set:system:ks", "vs");
        configs.put("set:key1", "value1");
        configs.put("set:hivevar:key2", "${hiveconf:common-key}");
        // enable native hive agg function
        configs.put(TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED.key(), "true");
        openSessionReq.setConfiguration(configs);
        TOpenSessionResp openSessionResp = client.OpenSession(openSessionReq);
        SessionHandle sessionHandle =
                ThriftObjectConversions.toSessionHandle(openSessionResp.getSessionHandle());

        Map<String, String> actualConfig =
                SQL_GATEWAY_SERVICE_EXTENSION.getService().getSessionConfig(sessionHandle);
        assertThat(actualConfig.entrySet())
                .contains(
                        new AbstractMap.SimpleEntry<>(
                                TABLE_SQL_DIALECT.key(), SqlDialect.HIVE.name()),
                        new AbstractMap.SimpleEntry<>(TABLE_DML_SYNC.key(), "true"),
                        new AbstractMap.SimpleEntry<>(RUNTIME_MODE.key(), BATCH.name()),
                        new AbstractMap.SimpleEntry<>(MAX_LENGTH_GENERATED_CODE.key(), "-1"),
                        new AbstractMap.SimpleEntry<>("key1", "value1"),
                        new AbstractMap.SimpleEntry<>("key2", "common-val"),
                        new AbstractMap.SimpleEntry<>(
                                TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED.key(), "true"));
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
    public void testGetUnsupportedException() throws Exception {
        try (HiveConnection connection = (HiveConnection) ENDPOINT_EXTENSION.getConnection();
                HiveStatement statement = (HiveStatement) connection.createStatement()) {
            assertThatThrownBy(() -> connection.renewDelegationToken("TokenMessage"))
                    .satisfies(
                            anyCauseMatches(
                                    "The HiveServer2 Endpoint currently doesn't support to RenewDelegationToken."));
            assertThatThrownBy(() -> connection.cancelDelegationToken("TokenMessage"))
                    .satisfies(
                            anyCauseMatches(
                                    "The HiveServer2 Endpoint currently doesn't support to CancelDelegationToken."));
            assertThatThrownBy(() -> connection.getDelegationToken("Flink", "TokenMessage"))
                    .satisfies(
                            anyCauseMatches(
                                    "The HiveServer2 Endpoint currently doesn't support to GetDelegationToken."));
            assertThatThrownBy(
                            () ->
                                    connection
                                            .getMetaData()
                                            .getCrossReference(
                                                    "hive",
                                                    "schema",
                                                    "table",
                                                    "default_catalog",
                                                    "default_database",
                                                    "table"))
                    .satisfies(
                            anyCauseMatches(
                                    "The HiveServer2 Endpoint currently doesn't support to GetCrossReference."));
            assertThatThrownBy(
                            () -> {
                                statement.execute("SHOW TABLES");
                                statement.getQueryLog();
                            })
                    .satisfies(
                            anyCauseMatches(
                                    "The HiveServer2 endpoint currently doesn't support to fetch logs."));
        }
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
                                        anyCauseMatches(
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
                Collections.singletonList(Collections.singletonList("hive")));
    }

    @Test
    public void testGetSchemas() throws Exception {
        runGetObjectTest(
                connection -> connection.getMetaData().getSchemas("hive", null),
                getExpectedGetSchemasOperationSchema(),
                Arrays.asList(
                        Arrays.asList("db_diff", "hive"),
                        Arrays.asList("db_test1", "hive"),
                        Arrays.asList("db_test2", "hive"),
                        Arrays.asList("default", "hive")));
    }

    @Test
    public void testGetSchemasWithPattern() throws Exception {
        runGetObjectTest(
                connection -> connection.getMetaData().getSchemas(null, "db\\_test%"),
                getExpectedGetSchemasOperationSchema(),
                Arrays.asList(
                        Arrays.asList("db_test1", "hive"), Arrays.asList("db_test2", "hive")));
    }

    @Test
    public void testGetTables() throws Exception {
        runGetObjectTest(
                connection ->
                        connection
                                .getMetaData()
                                .getTables(
                                        null,
                                        null,
                                        null,
                                        new String[] {"MANAGED_TABLE", "VIRTUAL_VIEW"}),
                getExpectedGetTablesOperationSchema(),
                Arrays.asList(
                        Arrays.asList("hive", "db_diff", "tbl_1", "TABLE"),
                        Arrays.asList("hive", "db_test1", "tbl_1", "TABLE"),
                        Arrays.asList("hive", "db_test1", "tbl_2", "TABLE"),
                        Arrays.asList("hive", "db_test2", "diff_1", "TABLE"),
                        Arrays.asList("hive", "db_test2", "tbl_1", "TABLE"),
                        Arrays.asList("hive", "db_diff", "tbl_2", "VIEW"),
                        Arrays.asList("hive", "db_test1", "tbl_3", "VIEW"),
                        Arrays.asList("hive", "db_test1", "tbl_4", "VIEW"),
                        Arrays.asList("hive", "db_test2", "diff_2", "VIEW"),
                        Arrays.asList("hive", "db_test2", "tbl_2", "VIEW")));
    }

    @Test
    public void testGetTablesWithPattern() throws Exception {
        runGetObjectTest(
                connection ->
                        connection
                                .getMetaData()
                                .getTables(
                                        "hive",
                                        "db\\_test_",
                                        "tbl%",
                                        new String[] {"VIRTUAL_VIEW"}),
                getExpectedGetTablesOperationSchema(),
                Arrays.asList(
                        Arrays.asList("hive", "db_test1", "tbl_3", "VIEW"),
                        Arrays.asList("hive", "db_test1", "tbl_4", "VIEW"),
                        Arrays.asList("hive", "db_test2", "tbl_2", "VIEW")));
    }

    @Test
    public void testGetTableTypes() throws Exception {
        runGetObjectTest(
                connection -> connection.getMetaData().getTableTypes(),
                ResolvedSchema.of(Column.physical("TABLE_TYPE", DataTypes.STRING())),
                Arrays.stream(TableKind.values())
                        .map(kind -> Collections.singletonList((Object) kind.name()))
                        .collect(Collectors.toList()));
    }

    @Test
    void testGetColumns() throws Exception {
        runGetObjectTest(
                connection -> connection.getMetaData().getColumns(null, null, null, null),
                getExpectedGetColumnsOperationSchema(),
                rows ->
                        assertThat(
                                        rows.stream()
                                                .map(
                                                        row ->
                                                                Arrays.asList(
                                                                        row.get(0), // CATALOG NAME
                                                                        row.get(1), // SCHEMA NAME
                                                                        row.get(2), // TABLE NAME
                                                                        row.get(3), // COLUMN NAME
                                                                        row.get(5))) // TYPE NAME
                                                .collect(Collectors.toList()))
                                .isEqualTo(
                                        Arrays.asList(
                                                Arrays.asList(
                                                        "hive", "db_diff", "tbl_2", "EXPR$0",
                                                        "INT"),
                                                Arrays.asList(
                                                        "hive",
                                                        "db_test1",
                                                        "tbl_1",
                                                        "user",
                                                        "BIGINT"),
                                                Arrays.asList(
                                                        "hive",
                                                        "db_test1",
                                                        "tbl_1",
                                                        "product",
                                                        "STRING"),
                                                Arrays.asList(
                                                        "hive",
                                                        "db_test1",
                                                        "tbl_1",
                                                        "amount",
                                                        "INT"),
                                                Arrays.asList(
                                                        "hive",
                                                        "db_test1",
                                                        "tbl_2",
                                                        "user",
                                                        "STRING"),
                                                Arrays.asList(
                                                        "hive",
                                                        "db_test1",
                                                        "tbl_2",
                                                        "id",
                                                        "BIGINT"),
                                                Arrays.asList(
                                                        "hive",
                                                        "db_test1",
                                                        "tbl_2",
                                                        "timestamp",
                                                        "TIMESTAMP"),
                                                Arrays.asList(
                                                        "hive",
                                                        "db_test1",
                                                        "tbl_3",
                                                        "EXPR$0",
                                                        "INT"),
                                                Arrays.asList(
                                                        "hive",
                                                        "db_test1",
                                                        "tbl_4",
                                                        "EXPR$0",
                                                        "INT"),
                                                Arrays.asList(
                                                        "hive",
                                                        "db_test2",
                                                        "diff_2",
                                                        "EXPR$0",
                                                        "INT"),
                                                Arrays.asList(
                                                        "hive",
                                                        "db_test2",
                                                        "tbl_2",
                                                        "EXPR$0",
                                                        "INT"))));
    }

    @Test
    public void testGetColumnsWithPattern() throws Exception {
        runGetObjectTest(
                connection ->
                        connection
                                .getMetaData()
                                .getColumns("hive", "db\\_test_", "tbl\\_1", "user"),
                getExpectedGetColumnsOperationSchema(),
                Collections.singletonList(
                        Arrays.asList(
                                "hive",
                                "db_test1",
                                "tbl_1",
                                "user",
                                Types.BIGINT,
                                "BIGINT",
                                String.valueOf(Long.MAX_VALUE).length(),
                                0, // digits number
                                10, // radix
                                0, // nullable
                                "user id.", // comment
                                1, // position
                                "NO", // isNullable
                                "NO"))); // isAutoIncrement
    }

    @Test
    public void testGetPrimaryKey() throws Exception {
        runGetObjectTest(
                connection -> connection.getMetaData().getPrimaryKeys(null, null, null),
                getExpectedGetPrimaryKeysOperationSchema(),
                Arrays.asList(
                        Arrays.asList("hive", "db_test1", "tbl_1", "user", 1, "pk"),
                        Arrays.asList("hive", "db_test1", "tbl_2", "user", 1, "pk"),
                        Arrays.asList("hive", "db_test1", "tbl_2", "id", 2, "pk")));
    }

    @Test
    public void testGetPrimaryKeyWithPattern() throws Exception {
        runGetObjectTest(
                connection -> connection.getMetaData().getPrimaryKeys(null, null, "tbl_2"),
                getExpectedGetPrimaryKeysOperationSchema(),
                Arrays.asList(
                        Arrays.asList("hive", "db_test1", "tbl_2", "user", 1, "pk"),
                        Arrays.asList("hive", "db_test1", "tbl_2", "id", 2, "pk")));
    }

    @Test
    public void testGetTypeInfo() throws Exception {
        runGetObjectTest(
                connection -> connection.getMetaData().getTypeInfo(),
                getExpectedGetTypeInfoSchema(),
                types ->
                        assertThat(
                                        types.stream()
                                                .map(type -> type.get(0))
                                                .collect(Collectors.toList()))
                                .isEqualTo(
                                        Arrays.asList(
                                                "VOID",
                                                "BOOLEAN",
                                                "STRING",
                                                "BINARY",
                                                "TINYINT",
                                                "SMALLINT",
                                                "INT",
                                                "BIGINT",
                                                "FLOAT",
                                                "DOUBLE",
                                                "DECIMAL",
                                                "DATE",
                                                "TIMESTAMP",
                                                "ARRAY",
                                                "MAP",
                                                "STRUCT",
                                                "CHAR",
                                                "VARCHAR",
                                                "INTERVAL_YEAR_MONTH",
                                                "INTERVAL_DAY_TIME")));
    }

    @Test
    public void testGetFunctions() throws Exception {
        runGetObjectTest(
                connection -> connection.getMetaData().getFunctions(null, null, ".*"),
                ResolvedSchema.of(
                        Column.physical("FUNCTION_CAT", DataTypes.STRING()),
                        Column.physical("FUNCTION_SCHEM", DataTypes.STRING()),
                        Column.physical("FUNCTION_NAME", DataTypes.STRING()),
                        Column.physical("REMARKS", DataTypes.STRING()),
                        Column.physical("FUNCTION_TYPE", DataTypes.INT()),
                        Column.physical("SPECIFIC_NAME", DataTypes.STRING())),
                compactedResult ->
                        assertThat(compactedResult)
                                .contains(
                                        Arrays.asList(
                                                "withColumns",
                                                "",
                                                0,
                                                "org.apache.flink.table.functions.BuiltInFunctionDefinition"),
                                        Arrays.asList(
                                                "bin",
                                                "",
                                                1,
                                                "org.apache.hadoop.hive.ql.udf.UDFBin"),
                                        Arrays.asList(
                                                "parse_url_tuple",
                                                "",
                                                2,
                                                "org.apache.hadoop.hive.ql.udf.generic.GenericUDTFParseUrlTuple")));
    }

    @Test
    public void testGetFunctionWithPattern() throws Exception {
        runGetObjectTest(
                connection -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.execute(
                                String.format(
                                        "CREATE FUNCTION `db_test2`.`my_abs` as '%s'",
                                        JavaFunc0.class.getName()));
                        statement.execute(
                                String.format(
                                        "CREATE FUNCTION `db_diff`.`your_abs` as '%s'",
                                        JavaFunc0.class.getName()));
                    }
                    return connection.getMetaData().getFunctions("hive", "db.*", "my.*");
                },
                ResolvedSchema.of(
                        Column.physical("FUNCTION_CAT", DataTypes.STRING()),
                        Column.physical("FUNCTION_SCHEM", DataTypes.STRING()),
                        Column.physical("FUNCTION_NAME", DataTypes.STRING()),
                        Column.physical("REMARKS", DataTypes.STRING()),
                        Column.physical("FUNCTION_TYPE", DataTypes.INT()),
                        Column.physical("SPECIFIC_NAME", DataTypes.STRING())),
                Collections.singletonList(
                        Arrays.asList(
                                "hive", "db_test2", "my_abs", "", 0, JavaFunc0.class.getName())));
    }

    @Test
    public void testGetInfo() throws Exception {
        try (Connection connection = ENDPOINT_EXTENSION.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getDatabaseProductName()).isEqualTo("Apache Flink");
            assertThat(metaData.getDatabaseProductVersion())
                    .isEqualTo(FlinkVersion.current().toString());
        }
    }

    @Test
    public void testUnknownGetInfoType() throws Exception {
        TCLIService.Client client = createClient();
        TOpenSessionReq openSessionReq = new TOpenSessionReq();
        TOpenSessionResp openSessionResp = client.OpenSession(openSessionReq);
        TSessionHandle tSessionHandle = openSessionResp.getSessionHandle();

        // send GetInfoReq using a GetInfoType which is unknown to HiveServer2 endpoint
        TGetInfoReq getInfoReq =
                new TGetInfoReq(tSessionHandle, TGetInfoType.CLI_MAX_IDENTIFIER_LEN);
        TGetInfoResp getInfoResp = client.GetInfo(getInfoReq);
        assertThat(getInfoResp.getStatus().getStatusCode()).isEqualTo(TStatusCode.ERROR_STATUS);

        try (Connection connection = ENDPOINT_EXTENSION.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            connection.createStatement().execute("CREATE SCHEMA test;");

            assertThat(collectAndCompact(metaData.getSchemas("hive", null), 2))
                    .contains(Arrays.asList("test", "hive"));
        }
    }

    @Test
    public void testExecuteStatementInSyncMode() throws Exception {
        TCLIService.Client client = createClient();
        TSessionHandle sessionHandle = client.OpenSession(new TOpenSessionReq()).getSessionHandle();
        TOperationHandle operationHandle =
                client.ExecuteStatement(new TExecuteStatementReq(sessionHandle, "SHOW DATABASES"))
                        .getOperationHandle();

        assertThat(
                        client.GetOperationStatus(new TGetOperationStatusReq(operationHandle))
                                .getOperationState())
                .isEqualTo(TOperationState.FINISHED_STATE);

        RowSet rowSet =
                RowSetFactory.create(
                        client.FetchResults(
                                        new TFetchResultsReq(
                                                operationHandle,
                                                TFetchOrientation.FETCH_NEXT,
                                                Integer.MAX_VALUE))
                                .getResults(),
                        HIVE_CLI_SERVICE_PROTOCOL_V10);
        Iterator<Object[]> iterator = rowSet.iterator();
        List<List<Object>> actual = new ArrayList<>();
        while (iterator.hasNext()) {
            actual.add(new ArrayList<>(Arrays.asList(iterator.next())));
        }
        List<List<String>> expected = new ArrayList<>();
        for (String s : Arrays.asList("db_diff", "db_test1", "db_test2", "default")) {
            expected.add(Collections.singletonList(s));
        }
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testExecuteStatementInSyncModeWithCompileException() throws Exception {
        TCLIService.Client client = createClient();
        TSessionHandle tSessionHandle =
                client.OpenSession(new TOpenSessionReq()).getSessionHandle();
        TExecuteStatementReq req =
                new TExecuteStatementReq(tSessionHandle, "SELECT * FROM `non_exist_table`");
        TExecuteStatementResp resp = client.ExecuteStatement(req);
        assertThat(resp.getStatus().getInfoMessages())
                .matches(
                        causes ->
                                causes.stream()
                                        .anyMatch(
                                                cause ->
                                                        cause.contains(
                                                                "Table not found 'non_exist_table'")));
        assertThat(
                        ((SqlGatewayServiceImpl) (SQL_GATEWAY_SERVICE_EXTENSION.getService()))
                                .getSession(toSessionHandle(tSessionHandle))
                                .getOperationManager()
                                .getOperationCount())
                .isEqualTo(0);
    }

    @Test
    public void testExecuteStatementInSyncModeWithRuntimeException1() throws Exception {
        runExecuteStatementInSyncModeWithRuntimeException(
                (tSessionHandle, future) -> {
                    createClient().CloseSession(new TCloseSessionReq(tSessionHandle));

                    TExecuteStatementResp resp = future.get(10, TimeUnit.SECONDS);
                    assertThat(resp.getStatus().getInfoMessages())
                            .matches(
                                    causes ->
                                            causes.stream()
                                                    .anyMatch(
                                                            cause ->
                                                                    // Close the session before or
                                                                    // after
                                                                    // submitting the job
                                                                    cause.contains(
                                                                                    "Failed to execute statement.")
                                                                            || cause.contains(
                                                                                    "Failed to getOperationInfo")));
                });
    }

    @Test
    public void testExecuteStatementInSyncModeWithRuntimeException2(
            @InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {
        runExecuteStatementInSyncModeWithRuntimeException(
                (tSessionHandle, future) -> {
                    waitUntilJobIsRunning(restClusterClient);
                    JobID jobID =
                            JobID.fromHexString(
                                    SQL_GATEWAY_SERVICE_EXTENSION
                                            .getService()
                                            .getSessionConfig(toSessionHandle(tSessionHandle))
                                            .get(PIPELINE_FIXED_JOB_ID.key()));

                    restClusterClient.cancel(jobID).get();

                    TExecuteStatementResp resp = future.get(10, TimeUnit.SECONDS);
                    assertThat(resp.getStatus().getInfoMessages())
                            .matches(
                                    causes ->
                                            causes.stream()
                                                    .anyMatch(
                                                            cause ->
                                                                    cause.contains(
                                                                            String.format(
                                                                                    "Job failed (JobID: %s)",
                                                                                    jobID))));
                });
    }

    // --------------------------------------------------------------------------------------------

    private static void initializeEnvironment() throws Exception {
        try (Connection connection = ENDPOINT_EXTENSION.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET table.sql-dialect=default");

            // hive: db_test1 | db_test2 | db_diff | default
            //     db_test1: temporary table tbl_1, table tbl_2, temporary view tbl_3, view tbl_4
            //     db_test2: table tbl_1, table diff_1, view tbl_2, view diff_2
            //     db_diff:  table tbl_1, view tbl_2

            statement.execute("CREATE DATABASE db_test1");
            statement.execute("CREATE DATABASE db_test2");
            statement.execute("CREATE DATABASE db_diff");

            statement.execute(
                    "CREATE TABLE db_test1.tbl_1(\n"
                            + "`user` BIGINT CONSTRAINT `pk` PRIMARY KEY NOT ENFORCED COMMENT 'user id.',\n"
                            + "`product` STRING NOT NULL,\n"
                            + "`amount`  INT) COMMENT 'temporary table tbl_1'");
            statement.execute(
                    "CREATE TABLE db_test1.tbl_2(\n"
                            + "`user` STRING COMMENT 'user name.',\n"
                            + "`id` BIGINT COMMENT 'user id.',\n"
                            + "`timestamp` TIMESTAMP,"
                            + "CONSTRAINT `pk` PRIMARY KEY(`user`, `id`) NOT ENFORCED) COMMENT 'table tbl_2'");
            statement.execute(
                    "CREATE VIEW db_test1.tbl_3 COMMENT 'temporary view tbl_3' AS SELECT 1");
            statement.execute("CREATE VIEW db_test1.tbl_4 COMMENT 'view tbl_4' AS SELECT 1");

            statement.execute("CREATE TABLE db_test2.tbl_1 COMMENT 'table tbl_1'");
            statement.execute("CREATE TABLE db_test2.diff_1 COMMENT 'table diff_1'");
            statement.execute("CREATE VIEW db_test2.tbl_2 COMMENT 'view tbl_2' AS SELECT 1");
            statement.execute("CREATE VIEW db_test2.diff_2 COMMENT 'view diff_2' AS SELECT 1");

            statement.execute("CREATE TABLE db_diff.tbl_1 COMMENT 'table tbl_1'");
            statement.execute("CREATE VIEW db_diff.tbl_2 COMMENT 'view tbl_2' AS SELECT 1");
        }
    }

    private void runGetObjectTest(
            FunctionWithException<Connection, java.sql.ResultSet, Exception> resultSetSupplier,
            ResolvedSchema expectedSchema,
            List<List<Object>> expectedResults)
            throws Exception {
        runGetObjectTest(
                resultSetSupplier,
                expectedSchema,
                result -> assertThat(result).isEqualTo(expectedResults));
    }

    private void runGetObjectTest(
            FunctionWithException<Connection, java.sql.ResultSet, Exception> resultSetSupplier,
            ResolvedSchema expectedSchema,
            Consumer<List<List<Object>>> validator)
            throws Exception {
        try (Connection connection = ENDPOINT_EXTENSION.getConnection();
                java.sql.ResultSet result = resultSetSupplier.apply(connection)) {
            assertSchemaEquals(expectedSchema, result.getMetaData());
            validator.accept(collectAndCompact(result, expectedSchema.getColumnCount()));
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
                                () -> {
                                    latch.await();
                                    return NotReadyResult.INSTANCE;
                                });
        manipulateOp.accept(
                toTOperationHandle(sessionHandle, operationHandle, TOperationType.UNKNOWN));
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

    private ResolvedSchema getExpectedGetSchemasOperationSchema() {
        return ResolvedSchema.of(
                Column.physical("TABLE_SCHEM", DataTypes.STRING()),
                Column.physical("TABLE_CATALOG", DataTypes.STRING()));
    }

    private ResolvedSchema getExpectedGetTablesOperationSchema() {
        return ResolvedSchema.of(
                Column.physical("TABLE_CAT", DataTypes.STRING()),
                Column.physical("TABLE_SCHEM", DataTypes.STRING()),
                Column.physical("TABLE_NAME", DataTypes.STRING()),
                Column.physical("TABLE_TYPE", DataTypes.STRING()),
                Column.physical("REMARKS", DataTypes.STRING()),
                Column.physical("TYPE_CAT", DataTypes.STRING()),
                Column.physical("TYPE_SCHEM", DataTypes.STRING()),
                Column.physical("TYPE_NAME", DataTypes.STRING()),
                Column.physical("SELF_REFERENCING_COL_NAME", DataTypes.STRING()),
                Column.physical("REF_GENERATION", DataTypes.STRING()));
    }

    private ResolvedSchema getExpectedGetColumnsOperationSchema() {
        return ResolvedSchema.of(
                Column.physical("TABLE_CAT", DataTypes.STRING()),
                Column.physical("TABLE_SCHEM", DataTypes.STRING()),
                Column.physical("TABLE_NAME", DataTypes.STRING()),
                Column.physical("COLUMN_NAME", DataTypes.STRING()),
                Column.physical("DATA_TYPE", DataTypes.INT()),
                Column.physical("TYPE_NAME", DataTypes.STRING()),
                Column.physical("COLUMN_SIZE", DataTypes.INT()),
                Column.physical("BUFFER_LENGTH", DataTypes.TINYINT()),
                Column.physical("DECIMAL_DIGITS", DataTypes.INT()),
                Column.physical("NUM_PREC_RADIX", DataTypes.INT()),
                Column.physical("NULLABLE", DataTypes.INT()),
                Column.physical("REMARKS", DataTypes.STRING()),
                Column.physical("COLUMN_DEF", DataTypes.STRING()),
                Column.physical("SQL_DATA_TYPE", DataTypes.INT()),
                Column.physical("SQL_DATETIME_SUB", DataTypes.INT()),
                Column.physical("CHAR_OCTET_LENGTH", DataTypes.INT()),
                Column.physical("ORDINAL_POSITION", DataTypes.INT()),
                Column.physical("IS_NULLABLE", DataTypes.STRING()),
                Column.physical("SCOPE_CATALOG", DataTypes.STRING()),
                Column.physical("SCOPE_SCHEMA", DataTypes.STRING()),
                Column.physical("SCOPE_TABLE", DataTypes.STRING()),
                Column.physical("SOURCE_DATA_TYPE", DataTypes.SMALLINT()),
                Column.physical("IS_AUTO_INCREMENT", DataTypes.STRING()));
    }

    private ResolvedSchema getExpectedGetPrimaryKeysOperationSchema() {
        return ResolvedSchema.of(
                Column.physical("TABLE_CAT", DataTypes.STRING()),
                Column.physical("TABLE_SCHEM", DataTypes.STRING()),
                Column.physical("TABLE_NAME", DataTypes.STRING()),
                Column.physical("COLUMN_NAME", DataTypes.STRING()),
                Column.physical("KEY_SEQ", DataTypes.INT()),
                Column.physical("PK_NAME", DataTypes.STRING()));
    }

    private ResolvedSchema getExpectedGetTypeInfoSchema() {
        return ResolvedSchema.of(
                Column.physical("TYPE_NAME", DataTypes.STRING()),
                Column.physical("DATA_TYPE", DataTypes.INT()),
                Column.physical("PRECISION", DataTypes.INT()),
                Column.physical("LITERAL_PREFIX", DataTypes.STRING()),
                Column.physical("LITERAL_SUFFIX", DataTypes.STRING()),
                Column.physical("CREATE_PARAMS", DataTypes.STRING()),
                Column.physical("NULLABLE", DataTypes.SMALLINT()),
                Column.physical("CASE_SENSITIVE", DataTypes.BOOLEAN()),
                Column.physical("SEARCHABLE", DataTypes.SMALLINT()),
                Column.physical("UNSIGNED_ATTRIBUTE", DataTypes.BOOLEAN()),
                Column.physical("FIXED_PREC_SCALE", DataTypes.BOOLEAN()),
                Column.physical("AUTO_INCREMENT", DataTypes.BOOLEAN()),
                Column.physical("LOCAL_TYPE_NAME", DataTypes.STRING()),
                Column.physical("MINIMUM_SCALE", DataTypes.SMALLINT()),
                Column.physical("MAXIMUM_SCALE", DataTypes.SMALLINT()),
                Column.physical("SQL_DATA_TYPE", DataTypes.INT()),
                Column.physical("SQL_DATETIME_SUB", DataTypes.INT()),
                Column.physical("NUM_PREC_RADIX", DataTypes.INT()));
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
                    Type.getType(HiveTypeUtil.toHiveTypeInfo(column.getDataType(), false))
                            .toJavaSQLType();
            assertThat(metaData.getColumnType(i)).isEqualTo(jdbcType);
        }
    }

    private List<List<Object>> collectAndCompact(java.sql.ResultSet result, int columnCount)
            throws Exception {
        List<List<Object>> actual = new ArrayList<>();
        while (result.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                Object value = result.getObject(i);
                // ignore the null value for better presentation
                if (value == null) {
                    continue;
                }
                row.add(value);
            }
            actual.add(row);
        }
        return actual;
    }

    private void runExecuteStatementInSyncModeWithRuntimeException(
            BiConsumerWithException<
                            TSessionHandle,
                            FutureTaskWithException<TExecuteStatementResp>,
                            Exception>
                    checker)
            throws Exception {
        TCLIService.Client client = createClient();
        TOpenSessionReq openSessionReq = new TOpenSessionReq();
        openSessionReq.putToConfiguration(
                RUNTIME_MODE.key(), RuntimeExecutionMode.STREAMING.name());
        openSessionReq.putToConfiguration(TABLE_SQL_DIALECT.key(), SqlDialect.DEFAULT.name());
        openSessionReq.putToConfiguration(PIPELINE_FIXED_JOB_ID.key(), JobID.generate().toString());
        TSessionHandle tSessionHandle = client.OpenSession(openSessionReq).getSessionHandle();

        List<String> initSql =
                Arrays.asList(
                        "CREATE TEMPORARY TABLE source(\n"
                                + "  a INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'datagen'"
                                + ")",
                        "CREATE TEMPORARY TABLE sink(\n"
                                + "  a INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'blackhole'"
                                + ")");

        for (String sql : initSql) {
            TExecuteStatementReq statementReq = new TExecuteStatementReq(tSessionHandle, sql);
            client.ExecuteStatement(statementReq);
        }

        CountDownLatch countDownLatch = new CountDownLatch(1);
        FutureTaskWithException<TExecuteStatementResp> future =
                new FutureTaskWithException<>(
                        () -> {
                            countDownLatch.countDown();
                            // Thrift client is not thread-safe.
                            return createClient()
                                    .ExecuteStatement(
                                            new TExecuteStatementReq(
                                                    tSessionHandle,
                                                    "INSERT INTO sink SELECT * FROM source"));
                        });
        Thread submitter = new Thread(future);
        submitter.start();
        countDownLatch.await();

        checker.accept(tSessionHandle, future);
    }

    private void waitUntilJobIsRunning(ClusterClient<?> client) throws Exception {
        while (getRunningJobs(client).isEmpty()) {
            Thread.sleep(50);
        }
    }

    private List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
        Collection<JobStatusMessage> statusMessages = client.listJobs().get();
        return statusMessages.stream()
                .filter(status -> !status.getJobState().isGloballyTerminalState())
                .map(JobStatusMessage::getJobId)
                .collect(Collectors.toList());
    }
}
