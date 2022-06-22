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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.plan.utils.RexNodeToExpressionConverter;
import org.apache.flink.table.planner.runtime.utils.StreamTestSink;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.calcite.rex.RexBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

/** Test for {@link JdbcFilterPushdownVisitor}. */
public class JdbcFilterPushdownVisitorTest extends AbstractTestBase {

    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    public static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
    public static final String DB_URL = "jdbc:derby:memory:test";
    public static final String INPUT_TABLE = "jdbDynamicTableSource";

    public static StreamExecutionEnvironment env;
    public static TableEnvironment tEnv;

    @Before
    public void before() throws ClassNotFoundException, SQLException {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);

        System.setProperty(
                "derby.stream.error.field", JdbcTestBase.class.getCanonicalName() + ".DEV_NULL");
        Class.forName(DRIVER_CLASS);

        try (Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
                Statement statement = conn.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE "
                            + INPUT_TABLE
                            + " ("
                            + "id BIGINT NOT NULL,"
                            + "description VARCHAR(200) NOT NULL,"
                            + "timestamp6_col TIMESTAMP, "
                            + "timestamp9_col TIMESTAMP, "
                            + "time_col TIME, "
                            + "real_col FLOAT(23), "
                            + // A precision of 23 or less makes FLOAT equivalent to REAL.
                            "double_col FLOAT(24),"
                            + // A precision of 24 or greater makes FLOAT equivalent to DOUBLE
                            // PRECISION.
                            "decimal_col DECIMAL(10, 4))");
        }
        // Create table in Flink, this can be reused across test cases
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "("
                        + "id BIGINT,"
                        + "description VARCHAR(200),"
                        + "timestamp6_col TIMESTAMP(6),"
                        + "timestamp9_col TIMESTAMP(9),"
                        + "time_col TIME,"
                        + "real_col FLOAT,"
                        + "double_col DOUBLE,"
                        + "decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + DB_URL
                        + "',"
                        + "  'table-name'='"
                        + INPUT_TABLE
                        + "'"
                        + ")");
    }

    @After
    public void clearOutputTable() throws Exception {
        Class.forName(DRIVER_CLASS);
        try (Connection conn = DriverManager.getConnection(DB_URL);
                Statement stat = conn.createStatement()) {
            stat.executeUpdate("DROP TABLE " + INPUT_TABLE);
        }
        StreamTestSink.clear();
    }

    @Test
    public void testSimpleExpressionPrimitiveType() {
        ResolvedSchema schema = tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE).getResolvedSchema();
        Arrays.asList(
                        "id = 6",
                        "id >= 6",
                        "id > 6",
                        "id < 6",
                        "id <= 5",
                        "description = 'Halo'",
                        "real_col > 0.5",
                        "double_col <= -0.3")
                .forEach(input -> assertSimpleInputExprEqualsOutExpr(input, schema));
    }

    @Test
    public void testComplexExpressionDatetime() {
        ResolvedSchema schema = tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE).getResolvedSchema();
        String andExpr = "id = 6 AND timestamp6_col = TIMESTAMP '2022-01-01 07:00:01.333'";
        assertGeneratedSQLString(
                andExpr, schema, "((id = 6) AND (timestamp6_col = '2022-01-01 07:00:01.333000'))");

        String orExpr =
                "timestamp9_col = TIMESTAMP '2022-01-01 07:00:01.333' OR description = 'Halo'";
        assertGeneratedSQLString(
                orExpr,
                schema,
                "((timestamp9_col = '2022-01-01 07:00:01.333000000') OR (description = 'Halo'))");
    }

    @Test
    public void testExpressionWithNull() {
        ResolvedSchema schema = tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE).getResolvedSchema();
        String andExpr = "id = NULL AND real_col <= 0.6";
        assertGeneratedSQLString(andExpr, schema, "((id = null) AND (real_col <= 0.6))");

        String orExpr = "id = 6 OR description = NULL";
        assertGeneratedSQLString(orExpr, schema, "((id = 6) OR (description = null))");
    }

    @Test
    public void testComplexExpressionPrimitiveType() {
        ResolvedSchema schema = tEnv.sqlQuery("SELECT * FROM " + INPUT_TABLE).getResolvedSchema();
        String andExpr = "id = NULL AND real_col <= 0.6";
        assertGeneratedSQLString(andExpr, schema, "((id = null) AND (real_col <= 0.6))");

        String orExpr = "id = 6 OR description = NULL";
        assertGeneratedSQLString(orExpr, schema, "((id = 6) OR (description = null))");
    }

    private void assertGeneratedSQLString(
            String inputExpr, ResolvedSchema schema, String expectedOutputExpr) {
        List<ResolvedExpression> resolved = resolveSQLFilterToExpression(inputExpr, schema);
        assertEquals(1, resolved.size());
        JdbcDialect dialect = JdbcDialects.get(DB_URL).get();
        JdbcFilterPushdownVisitor visitor = new JdbcFilterPushdownVisitor(dialect::quoteIdentifier);
        String generatedString = resolved.get(0).accept(visitor).get();
        // our visitor always wrap expression
        assertEquals(expectedOutputExpr, generatedString);
    }

    private void assertSimpleInputExprEqualsOutExpr(String inputExpr, ResolvedSchema schema) {
        // our visitor always wrap expression
        assertGeneratedSQLString(inputExpr, schema, "(" + inputExpr + ")");
    }

    /**
     * Resolve a SQL filter expression against a Schema, this method makes use of some
     * implementation details of Flink.
     */
    private List<ResolvedExpression> resolveSQLFilterToExpression(
            String sqlExp, ResolvedSchema schema) {
        StreamTableEnvironmentImpl tbImpl = (StreamTableEnvironmentImpl) tEnv;

        FlinkContext ctx = ((PlannerBase) tbImpl.getPlanner()).getFlinkContext();
        CatalogManager catMan = tbImpl.getCatalogManager();
        FunctionCatalog funCat = ctx.getFunctionCatalog();
        RowType sourceType = (RowType) schema.toSourceRowDataType().getLogicalType();

        RexNodeToExpressionConverter converter =
                new RexNodeToExpressionConverter(
                        new RexBuilder(FlinkTypeFactory.INSTANCE()),
                        sourceType.getFieldNames().toArray(new String[0]),
                        funCat,
                        catMan,
                        TimeZone.getTimeZone(tEnv.getConfig().getLocalTimeZone()));

        RexNodeExpression rexExp =
                (RexNodeExpression) tbImpl.getParser().parseSqlExpression(sqlExp, sourceType, null);
        ResolvedExpression resolvedExp = rexExp.getRexNode().accept(converter).get();
        ExpressionResolver resolver =
                ExpressionResolver.resolverFor(
                                tEnv.getConfig(),
                                name -> Optional.empty(),
                                funCat.asLookup(
                                        str -> {
                                            throw new TableException(
                                                    "We should not need to lookup any expressions at this point");
                                        }),
                                catMan.getDataTypeFactory(),
                                (sqlExpression, inputRowType, outputType) -> {
                                    throw new TableException(
                                            "SQL expression parsing is not supported at this location.");
                                })
                        .build();

        return resolver.resolve(Arrays.asList(resolvedExp));
    }
}
