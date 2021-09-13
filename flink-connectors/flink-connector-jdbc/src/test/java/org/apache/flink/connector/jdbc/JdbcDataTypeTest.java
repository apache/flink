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

package org.apache.flink.connector.jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

/** Tests for all DataTypes and Dialects of JDBC connector. */
@RunWith(Parameterized.class)
public class JdbcDataTypeTest {

    private static final String DDL_FORMAT =
            "CREATE TABLE T(\n"
                    + "f0 %s\n"
                    + ") WITH (\n"
                    + "  'connector.type'='jdbc',\n"
                    + "  'connector.url'='"
                    + "jdbc:%s:memory:test"
                    + "',\n"
                    + "  'connector.table'='myTable'\n"
                    + ")";

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestItem> testData() {
        return Arrays.asList(
                createTestItem("derby", "CHAR"),
                createTestItem("derby", "VARCHAR"),
                createTestItem("derby", "BOOLEAN"),
                createTestItem("derby", "TINYINT"),
                createTestItem("derby", "SMALLINT"),
                createTestItem("derby", "INTEGER"),
                createTestItem("derby", "BIGINT"),
                createTestItem("derby", "FLOAT"),
                createTestItem("derby", "DOUBLE"),
                createTestItem("derby", "DECIMAL(10, 4)"),
                createTestItem("derby", "DATE"),
                createTestItem("derby", "TIME"),
                createTestItem("derby", "TIMESTAMP(3)"),
                createTestItem("derby", "TIMESTAMP WITHOUT TIME ZONE"),
                createTestItem("derby", "TIMESTAMP(9) WITHOUT TIME ZONE"),
                createTestItem("derby", "VARBINARY"),
                createTestItem("mysql", "CHAR"),
                createTestItem("mysql", "VARCHAR"),
                createTestItem("mysql", "BOOLEAN"),
                createTestItem("mysql", "TINYINT"),
                createTestItem("mysql", "SMALLINT"),
                createTestItem("mysql", "INTEGER"),
                createTestItem("mysql", "BIGINT"),
                createTestItem("mysql", "FLOAT"),
                createTestItem("mysql", "DOUBLE"),
                createTestItem("mysql", "DECIMAL(10, 4)"),
                createTestItem("mysql", "DECIMAL(38, 18)"),
                createTestItem("mysql", "DATE"),
                createTestItem("mysql", "TIME"),
                createTestItem("mysql", "TIMESTAMP(3)"),
                createTestItem("mysql", "TIMESTAMP WITHOUT TIME ZONE"),
                createTestItem("mysql", "VARBINARY"),
                createTestItem("postgresql", "CHAR"),
                createTestItem("postgresql", "VARCHAR"),
                createTestItem("postgresql", "BOOLEAN"),
                createTestItem("postgresql", "TINYINT"),
                createTestItem("postgresql", "SMALLINT"),
                createTestItem("postgresql", "INTEGER"),
                createTestItem("postgresql", "BIGINT"),
                createTestItem("postgresql", "FLOAT"),
                createTestItem("postgresql", "DOUBLE"),
                createTestItem("postgresql", "DECIMAL(10, 4)"),
                createTestItem("postgresql", "DECIMAL(38, 18)"),
                createTestItem("postgresql", "DATE"),
                createTestItem("postgresql", "TIME"),
                createTestItem("postgresql", "TIMESTAMP(3)"),
                createTestItem("postgresql", "TIMESTAMP WITHOUT TIME ZONE"),
                createTestItem("postgresql", "VARBINARY"),
                createTestItem("postgresql", "ARRAY<INTEGER>"),

                // Unsupported types throws errors.
                createTestItem(
                        "derby", "BINARY", "The Derby dialect doesn't support type: BINARY(1)."),
                createTestItem(
                        "derby",
                        "VARBINARY(10)",
                        "The Derby dialect doesn't support type: VARBINARY(10)."),
                createTestItem(
                        "derby",
                        "TIMESTAMP_LTZ(3)",
                        "The Derby dialect doesn't support type: TIMESTAMP_LTZ(3)."),
                createTestItem(
                        "derby",
                        "DECIMAL(38, 18)",
                        "The precision of field 'f0' is out of the DECIMAL precision range [1, 31] supported by Derby dialect."),
                createTestItem(
                        "mysql", "BINARY", "The MySQL dialect doesn't support type: BINARY(1)."),
                createTestItem(
                        "mysql",
                        "VARBINARY(10)",
                        "The MySQL dialect doesn't support type: VARBINARY(10)."),
                createTestItem(
                        "mysql",
                        "TIMESTAMP(9) WITHOUT TIME ZONE",
                        "The precision of field 'f0' is out of the TIMESTAMP precision range [1, 6] supported by MySQL dialect."),
                createTestItem(
                        "mysql",
                        "TIMESTAMP_LTZ(3)",
                        "The MySQL dialect doesn't support type: TIMESTAMP_LTZ(3)."),
                createTestItem(
                        "postgresql",
                        "BINARY",
                        "The PostgreSQL dialect doesn't support type: BINARY(1)."),
                createTestItem(
                        "postgresql",
                        "VARBINARY(10)",
                        "The PostgreSQL dialect doesn't support type: VARBINARY(10)."),
                createTestItem(
                        "postgresql",
                        "TIMESTAMP(9) WITHOUT TIME ZONE",
                        "The precision of field 'f0' is out of the TIMESTAMP precision range [1, 6] supported by PostgreSQL dialect."),
                createTestItem(
                        "postgresql",
                        "TIMESTAMP_LTZ(3)",
                        "The PostgreSQL dialect doesn't support type: TIMESTAMP_LTZ(3)."));
    }

    private static TestItem createTestItem(Object... args) {
        assert args.length >= 2;
        TestItem item = TestItem.fromDialectAndType((String) args[0], (String) args[1]);
        if (args.length == 3) {
            item.withExpectError((String) args[2]);
        }
        return item;
    }

    @Parameterized.Parameter public TestItem testItem;

    @Test
    public void testDataTypeValidate() {
        String sqlDDL = String.format(DDL_FORMAT, testItem.dataTypeExpr, testItem.dialect);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(sqlDDL);

        if (testItem.expectError != null) {
            try {
                tEnv.sqlQuery("SELECT * FROM T");
            } catch (Exception ex) {
                Assert.assertTrue(ex.getCause() instanceof ValidationException);
                Assert.assertEquals(testItem.expectError, ex.getCause().getMessage());
            }
        } else {
            tEnv.sqlQuery("SELECT * FROM T");
        }
    }

    // ~ Inner Class
    private static class TestItem {

        private final String dialect;

        private final String dataTypeExpr;

        @Nullable private String expectError;

        private TestItem(String dialect, String dataTypeExpr) {
            this.dialect = dialect;
            this.dataTypeExpr = dataTypeExpr;
        }

        static TestItem fromDialectAndType(String dialect, String dataTypeExpr) {
            return new TestItem(dialect, dataTypeExpr);
        }

        TestItem withExpectError(String expectError) {
            this.expectError = expectError;
            return this;
        }

        @Override
        public String toString() {
            return String.format("Dialect: %s, DataType: %s", dialect, dataTypeExpr);
        }
    }
}
