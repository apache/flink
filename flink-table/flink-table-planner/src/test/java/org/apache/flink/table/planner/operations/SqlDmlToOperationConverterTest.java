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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.dql.SqlRichExplain;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.DeleteFromFilterOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.TruncateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.factories.TestUpdateDeleteTableFactory;
import org.apache.flink.table.planner.parse.CalciteParser;

import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/** Test cases for the DML statements for {@link SqlNodeToOperationConversion}. */
public class SqlDmlToOperationConverterTest extends SqlNodeToOperationConversionTestBase {

    @Test
    public void testExplainWithSelect() {
        final String sql = "explain select * from t1";
        checkExplainSql(sql);
    }

    @Test
    public void testExplainWithInsert() {
        final String sql = "explain insert into t2 select * from t1";
        checkExplainSql(sql);
    }

    @Test
    public void testExplainWithUnion() {
        final String sql = "explain select * from t1 union select * from t2";
        checkExplainSql(sql);
    }

    @Test
    public void testExplainWithExplainDetails() {
        String sql = "explain changelog_mode, estimated_cost, json_execution_plan select * from t1";
        checkExplainSql(sql);
    }

    @Test
    public void testSqlInsertWithStaticPartition() {
        final String sql = "insert into t1 partition(a=1) select b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(SinkModifyOperation.class);
        SinkModifyOperation sinkModifyOperation = (SinkModifyOperation) operation;
        final Map<String, String> expectedStaticPartitions = new HashMap<>();
        expectedStaticPartitions.put("a", "1");
        assertThat(sinkModifyOperation.getStaticPartitions()).isEqualTo(expectedStaticPartitions);
    }

    @Test
    public void testSqlInsertWithDynamicTableOptions() {
        final String sql =
                "insert into t1 /*+ OPTIONS('k1'='v1', 'k2'='v2') */\n"
                        + "select a, b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(SinkModifyOperation.class);
        SinkModifyOperation sinkModifyOperation = (SinkModifyOperation) operation;
        Map<String, String> dynamicOptions = sinkModifyOperation.getDynamicOptions();
        assertThat(dynamicOptions).isNotNull();
        assertThat(dynamicOptions.size()).isEqualTo(2);
        assertThat(dynamicOptions.toString()).isEqualTo("{k1=v1, k2=v2}");
    }

    @Test
    public void testDynamicTableWithInvalidOptions() {
        final String sql = "select * from t1 /*+ OPTIONS('opt1', 'opt2') */";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        assertThatThrownBy(() -> parse(sql, planner, parser))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(
                        "Hint [OPTIONS] only support " + "non empty key value options");
    }

    @Test
    public void testBeginStatementSet() {
        final String sql = "BEGIN STATEMENT SET";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(BeginStatementSetOperation.class);
        final BeginStatementSetOperation beginStatementSetOperation =
                (BeginStatementSetOperation) operation;

        assertThat(beginStatementSetOperation.asSummaryString()).isEqualTo("BEGIN STATEMENT SET");
    }

    @Test
    public void testEnd() {
        final String sql = "END";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(EndStatementSetOperation.class);
        final EndStatementSetOperation endStatementSetOperation =
                (EndStatementSetOperation) operation;

        assertThat(endStatementSetOperation.asSummaryString()).isEqualTo("END");
    }

    @Test
    public void testSqlRichExplainWithSelect() {
        final String sql = "explain plan for select a, b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(ExplainOperation.class);
    }

    @Test
    public void testSqlRichExplainWithInsert() {
        final String sql = "explain plan for insert into t1 select a, b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(ExplainOperation.class);
    }

    @Test
    public void testSqlRichExplainWithStatementSet() {
        final String sql =
                "explain plan for statement set begin "
                        + "insert into t1 select a, b, c, d from t2 where a > 1;"
                        + "insert into t1 select a, b, c, d from t2 where a > 2;"
                        + "end";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(ExplainOperation.class);
    }

    @Test
    public void testExplainDetailsWithSelect() {
        final String sql =
                "explain estimated_cost, changelog_mode, plan_advice select a, b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        assertExplainDetails(parse(sql, planner, parser));
    }

    @Test
    public void testExplainDetailsWithInsert() {
        final String sql =
                "explain estimated_cost, changelog_mode, plan_advice insert into t1 select a, b, c, d from t2";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        assertExplainDetails(parse(sql, planner, parser));
    }

    @Test
    public void testExplainDetailsWithStatementSet() {
        final String sql =
                "explain estimated_cost, changelog_mode, plan_advice statement set begin "
                        + "insert into t1 select a, b, c, d from t2 where a > 1;"
                        + "insert into t1 select a, b, c, d from t2 where a > 2;"
                        + "end";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        assertExplainDetails(parse(sql, planner, parser));
    }

    private void assertExplainDetails(Operation operation) {
        Set<String> expectedDetail = new HashSet<>();
        expectedDetail.add(ExplainDetail.ESTIMATED_COST.toString());
        expectedDetail.add(ExplainDetail.CHANGELOG_MODE.toString());
        expectedDetail.add(ExplainDetail.PLAN_ADVICE.toString());
        assertThat(operation)
                .asInstanceOf(type(ExplainOperation.class))
                .satisfies(
                        explain ->
                                assertThat(explain.getExplainDetails()).isEqualTo(expectedDetail));
    }

    @Test
    public void testSqlExecuteWithStatementSet() {
        final String sql =
                "execute statement set begin "
                        + "insert into t1 select a, b, c, d from t2 where a > 1;"
                        + "insert into t1 select a, b, c, d from t2 where a > 2;"
                        + "end";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(StatementSetOperation.class);
    }

    @Test
    public void testSqlExecuteWithInsert() {
        final String sql = "execute insert into t1 select a, b, c, d from t2 where a > 1";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(SinkModifyOperation.class);
    }

    @Test
    public void testSqlExecuteWithSelect() {
        final String sql = "execute select a, b, c, d from t2 where a > 1";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(QueryOperation.class);
    }

    @Test
    public void testDelete() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("connector", TestUpdateDeleteTableFactory.IDENTIFIER);
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder()
                                .column("a", DataTypes.INT().notNull())
                                .column("c", DataTypes.STRING().notNull())
                                .build(),
                        null,
                        Collections.emptyList(),
                        options);
        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("builtin", "default", "test_delete");
        catalogManager.createTable(catalogTable, tableIdentifier, false);

        // no filter in delete statement
        Operation operation = parse("DELETE FROM test_delete");
        checkDeleteFromFilterOperation(operation, "[]");

        // with filters in delete statement
        operation = parse("DELETE FROM test_delete where a = 1 and c = '123'");
        checkDeleteFromFilterOperation(operation, "[equals(a, 1), equals(c, '123')]");

        // with filter = false after reduced in delete statement
        operation = parse("DELETE FROM test_delete where a = 1 + 6 and a = 2");
        checkDeleteFromFilterOperation(operation, "[false]");

        operation = parse("DELETE FROM test_delete where a = (select count(*) from test_delete)");
        assertThat(operation).isInstanceOf(SinkModifyOperation.class);
        SinkModifyOperation modifyOperation = (SinkModifyOperation) operation;
        assertThat(modifyOperation.isDelete()).isTrue();
    }

    @Test
    public void testUpdate() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("connector", TestUpdateDeleteTableFactory.IDENTIFIER);
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder()
                                .column("a", DataTypes.INT().notNull())
                                .column("b", DataTypes.BIGINT().nullable())
                                .column("c", DataTypes.STRING().notNull())
                                .build(),
                        null,
                        Collections.emptyList(),
                        options);
        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("builtin", "default", "test_update");
        catalogManager.createTable(catalogTable, tableIdentifier, false);

        Operation operation = parse("UPDATE test_update SET a = 1, c = '123'");
        checkUpdateOperation(operation);

        operation = parse("UPDATE test_update SET a = 1, c = '123' WHERE a = 3");
        checkUpdateOperation(operation);

        operation =
                parse(
                        "UPDATE test_update SET a = 1, c = '123' WHERE b = 2 and a = (select count(*) from test_update)");
        checkUpdateOperation(operation);
    }

    @Test
    public void testTruncateTable() {
        String sql = "TRUNCATE TABLE t1";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        Operation operation = parse(sql, planner, parser);
        assertThat(operation).isInstanceOf(TruncateTableOperation.class);
        TruncateTableOperation truncateTableOperation = (TruncateTableOperation) operation;
        assertThat(truncateTableOperation.getTableIdentifier())
                .isEqualTo(ObjectIdentifier.of("builtin", "default", "t1"));
    }

    private void checkExplainSql(String sql) {
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        SqlNode node = parser.parse(sql);
        assertThat(node).isInstanceOf(SqlRichExplain.class);
        Operation operation =
                SqlNodeToOperationConversion.convert(planner, catalogManager, node).get();
        assertThat(operation).isInstanceOf(ExplainOperation.class);
    }

    private static void checkDeleteFromFilterOperation(
            Operation operation, String expectedFilters) {
        assertThat(operation).isInstanceOf(DeleteFromFilterOperation.class);
        DeleteFromFilterOperation deleteFromFiltersOperation =
                (DeleteFromFilterOperation) operation;
        List<ResolvedExpression> filters = deleteFromFiltersOperation.getFilters();
        assertThat(filters.toString()).isEqualTo(expectedFilters);
    }

    private static void checkUpdateOperation(Operation operation) {
        assertThat(operation).isInstanceOf(SinkModifyOperation.class);
        SinkModifyOperation sinkModifyOperation = (SinkModifyOperation) operation;
        assertThat(sinkModifyOperation.isUpdate()).isTrue();
    }
}
