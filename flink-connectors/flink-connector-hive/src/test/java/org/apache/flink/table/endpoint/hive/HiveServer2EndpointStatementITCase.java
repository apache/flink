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

package org.apache.flink.table.endpoint.hive;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.endpoint.hive.util.HiveServer2EndpointExtension;
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions;
import org.apache.flink.table.gateway.AbstractSqlGatewayStatementITCase;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.api.internal.StaticResultProvider.SIMPLE_ROW_DATA_TO_STRING_CONVERTER;

/** ITCase to verify the statements. */
public class HiveServer2EndpointStatementITCase extends AbstractSqlGatewayStatementITCase {

    @RegisterExtension
    @Order(3)
    public static final HiveServer2EndpointExtension ENDPOINT_EXTENSION =
            new HiveServer2EndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    private Connection connection;
    private Statement statement;

    @BeforeEach
    @Override
    public void before(@TempDir Path temporaryFolder) throws Exception {
        super.before(temporaryFolder);
        connection = ENDPOINT_EXTENSION.getConnection();
        statement = connection.createStatement();
    }

    @AfterEach
    public void after() throws Exception {
        statement.close();
        connection.close();
    }

    public static Stream<String> listHiveSqlTests() throws Exception {
        return listTestSpecInTheSameModule("endpoint");
    }

    @ParameterizedTest
    @MethodSource("listHiveSqlTests")
    public void testHiveSqlStatements(String sqlPath) throws Exception {
        runTest(sqlPath);
    }

    @Override
    protected String runSingleStatement(String sql) throws Exception {
        statement.execute(sql);

        ResultSet resultSet = statement.getResultSet();
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnSize = metaData.getColumnCount();
        List<RowData> rows = new ArrayList<>();
        DataType type = toStringifiedType(metaData);
        while (resultSet.next()) {
            GenericRowData stringifiedRowData = new GenericRowData(columnSize);
            for (int i = 0; i < columnSize; i++) {
                Object field = resultSet.getObject(i + 1);
                // Similar to SIMPLE_ROW_DATA_TO_STRING_CONVERTER
                if (field != null) {
                    if (field instanceof Boolean) {
                        stringifiedRowData.setField(i, field);
                    } else if (field instanceof byte[]) {
                        stringifiedRowData.setField(
                                i,
                                StringData.fromString(
                                        new String((byte[]) field, StandardCharsets.UTF_8)));
                    } else {
                        stringifiedRowData.setField(i, StringData.fromString(field.toString()));
                    }
                }
            }
            rows.add(stringifiedRowData);
        }

        StatementType statementType = StatementType.match(sql);

        return toString(
                statementType,
                DataTypeUtils.expandCompositeTypeToSchema(type),
                SIMPLE_ROW_DATA_TO_STRING_CONVERTER,
                rows.iterator());
    }

    @Override
    protected String stringifyException(Throwable t) {
        return t.getMessage().trim();
    }

    @Override
    protected boolean isStreaming() throws Exception {
        Field sessHandleField = HiveConnection.class.getDeclaredField("sessHandle");
        // Set the accessibility as true
        sessHandleField.setAccessible(true);
        SessionHandle sessionHandle =
                ThriftObjectConversions.toSessionHandle(
                        (TSessionHandle) sessHandleField.get(connection));
        return Configuration.fromMap(service.getSessionConfig(sessionHandle))
                .get(ExecutionOptions.RUNTIME_MODE)
                .equals(RuntimeExecutionMode.STREAMING);
    }

    @Override
    protected void resetSessionForFlinkSqlStatements() throws Exception {
        for (String sql :
                Arrays.asList(
                        "RESET",
                        "USE CATALOG `default_catalog`",
                        "DROP CATALOG hive",
                        "UNLOAD MODULE hive")) {
            runSingleStatement(sql);
        }
    }

    private DataType toStringifiedType(ResultSetMetaData metaData) throws Exception {
        int columnCount = metaData.getColumnCount();

        List<DataTypes.Field> fields = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            int sqlType = metaData.getColumnType(i);

            if (sqlType == Types.BOOLEAN) {
                fields.add(DataTypes.FIELD(columnName, DataTypes.BOOLEAN()));
            } else {
                fields.add(DataTypes.FIELD(columnName, DataTypes.STRING()));
            }
        }

        return DataTypes.ROW(fields);
    }
}
