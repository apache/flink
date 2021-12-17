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

package org.apache.flink.table.planner.plan.nodes.exec.operator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.planner.utils.TestLegacyFilterableTableSource;
import org.apache.flink.table.planner.utils.Top3;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/** Tests for verifying name and description of stream sql operator name. */
public class StreamOperatorNameTest extends OperatorNameTestBase {

    private StreamTableTestUtil util;

    @Override
    protected TableTestUtil getTableTestUtil() {
        return streamTestUtil(TableConfig.getDefault());
    }

    @Before
    public void setup() {
        super.setup();
        util = (StreamTableTestUtil) super.util;
    }

    /** Verify DropUpdateBefore. */
    @Test
    public void testDropUpdateBefore() throws Exception {

        util.getStreamEnv().setParallelism(2);

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int not null,\n"
                        + "  c varchar,\n"
                        + "  d bigint not null,\n"
                        + "  primary key(a, b) NOT ENFORCED\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'changelog-mode' = 'I,UA,UB,D',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);

        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  c varchar,\n"
                        + "  a bigint,\n"
                        + "  b int not null,\n"
                        + "  primary key(a, b) NOT ENFORCED\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'sink-changelog-mode-enforced' = 'I,UA,D',"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);

        verifyInsert("insert into MySink select c, a, b from MyTable");
    }

    /** Verify ChangelogNormalize and SinkMaterialize. */
    @Test
    public void testChangelogNormalize() throws Exception {

        util.getStreamEnv().setParallelism(2);

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int not null,\n"
                        + "  c varchar,\n"
                        + "  d bigint not null,\n"
                        + "  primary key(a, b) NOT ENFORCED\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'changelog-mode' = 'I,UA,D',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);

        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  c varchar,\n"
                        + "  a bigint,\n"
                        + "  b int not null,\n"
                        + "  primary key(a) NOT ENFORCED\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'sink-changelog-mode-enforced' = 'I,UA,D',"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);

        verifyInsert("insert into MySink select c, a, b from MyTable");
    }

    /** Verify Deduplicate. */
    @Test
    public void testDeduplicate() {
        createSourceWithTimeAttribute();
        verifyQuery(
                "SELECT a, b, c FROM "
                        + "(SELECT *, "
                        + "    ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime ASC) AS rk"
                        + " FROM MyTable) t "
                        + "WHERE rk = 1");
    }

    /**
     * Verify Expand, MiniBatchAssigner, LocalGroupAggregate, GlobalGroupAggregate,
     * IncrementalAggregate.
     */
    @Test
    public void testIncrementalAggregate() {
        util.enableMiniBatch();
        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true);
        createTestSource();
        verifyQuery("SELECT a, " + "count(distinct b) as b " + "FROM MyTable GROUP BY a");
    }

    /** Verify GroupAggregate. */
    @Test
    public void testGroupAggregate() {
        testGroupAggregateInternal();
    }

    /** Verify RowConversion, TableGroupAggregate. */
    @Test
    public void testTableGroupAggregate() {
        final DataStream<Integer> dataStream = util.getStreamEnv().fromElements(1, 2, 3, 4, 5);
        TableTestUtil.createTemporaryView(
                tEnv,
                "MySource",
                dataStream,
                JavaScalaConversionUtil.toScala(Optional.empty()),
                JavaScalaConversionUtil.toScala(Optional.empty()),
                JavaScalaConversionUtil.toScala(Optional.empty()));
        tEnv.createTemporaryFunction("top3", new Top3());
        tEnv.createTemporaryView(
                "MyTable",
                tEnv.from("MySource")
                        .flatAggregate(call(Top3.class, $("f0")))
                        .select($("f0"), $("f1")));
        verifyQuery("SELECT * FROM MyTable");
    }

    /** Verify IntervalJoin. */
    @Test
    public void testIntervalJoin() {
        createSourceWithTimeAttribute("A");
        createSourceWithTimeAttribute("B");
        verifyQuery(
                "SELECT t1.a, t2.b FROM A t1 JOIN B t2 ON\n"
                        + "    t1.a = t2.a AND \n"
                        + "    t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR");
    }

    /** Verify IntervalJoin. */
    @Test
    public void testIntervalJoinNegativeWindow() {
        createSourceWithTimeAttribute("A");
        createSourceWithTimeAttribute("B");
        verifyQuery(
                "SELECT t1.a, t2.b FROM A t1 LEFT JOIN B t2 ON\n"
                        + "    t1.a = t2.a AND \n"
                        + "    t1.proctime BETWEEN t2.proctime + INTERVAL '2' HOUR AND t2.proctime + INTERVAL '1' HOUR");
    }

    /** Verify Join. */
    @Test
    public void testJoin() {
        testJoinInternal();
    }

    @Test
    public void testMatch() {
        createSourceWithTimeAttribute();
        String sql =
                "SELECT T.aid, T.bid, T.cid\n"
                        + "     FROM MyTable MATCH_RECOGNIZE (\n"
                        + "             ORDER BY proctime\n"
                        + "             MEASURES\n"
                        + "             `A\"`.a AS aid,\n"
                        + "             \u006C.a AS bid,\n"
                        + "             C.a AS cid\n"
                        + "             PATTERN (`A\"` \u006C C)\n"
                        + "             DEFINE\n"
                        + "                 `A\"` AS a = 1,\n"
                        + "                 \u006C AS b = 2,\n"
                        + "                 C AS c = 'c'\n"
                        + "     ) AS T";
        verifyQuery(sql);
    }

    @Test
    public void testTemporalJoin() {
        tEnv.executeSql(
                "CREATE TABLE Orders (\n"
                        + " amount INT,\n"
                        + " currency STRING,\n"
                        + " rowtime TIMESTAMP(3),\n"
                        + " proctime AS PROCTIME(),\n"
                        + " WATERMARK FOR rowtime AS rowtime\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE RatesHistory (\n"
                        + " currency STRING,\n"
                        + " rate INT,\n"
                        + " rowtime TIMESTAMP(3),\n"
                        + " WATERMARK FOR rowtime AS rowtime,\n"
                        + " PRIMARY KEY(currency) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values'\n"
                        + ")");
        TemporalTableFunction ratesHistory =
                tEnv.from("RatesHistory").createTemporalTableFunction("rowtime", "currency");
        tEnv.createTemporarySystemFunction("Rates", ratesHistory);
        verifyQuery(
                "SELECT amount * r.rate "
                        + "FROM Orders AS o,  "
                        + "LATERAL TABLE (Rates(o.rowtime)) AS r "
                        + "WHERE o.currency = r.currency ");
    }

    @Test
    public void testTemporalSortOnProcTime() {
        createSourceWithTimeAttribute();
        verifyQuery("SELECT a FROM MyTable order by proctime, c");
    }

    @Test
    public void testTemporalSortOnEventTime() {
        createSourceWithTimeAttribute();
        verifyQuery("SELECT a FROM MyTable order by rowtime, c");
    }

    /** Verify WindowJoin, WindowRank, WindowAggregate, WindowDeduplicate. */
    @Test
    public void testWindowAggregate() {
        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "ONE_PHASE");
        createSourceWithTimeAttribute();
        verifyQuery(
                "SELECT\n"
                        + "  b,\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  COUNT(*),\n"
                        + "  SUM(a)\n"
                        + "FROM TABLE(\n"
                        + "   TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    /** Verify LocalWindowAggregate, GlobalWindowAggregate. */
    @Test
    public void testLocalGlobalWindowAggregate() {
        createSourceWithTimeAttribute();
        verifyQuery(
                "SELECT\n"
                        + "  b,\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  COUNT(*),\n"
                        + "  SUM(a)\n"
                        + "FROM TABLE(\n"
                        + "   TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    /** Verify WindowJoin. */
    @Test
    public void testWindowJoin() {
        createSourceWithTimeAttribute("MyTable");
        createSourceWithTimeAttribute("MyTable2");
        verifyQuery(
                "select\n"
                        + "  L.a,\n"
                        + "  L.window_start,\n"
                        + "  L.window_end,\n"
                        + "  L.cnt,\n"
                        + "  L.uv,\n"
                        + "  R.a,\n"
                        + "  R.cnt,\n"
                        + "  R.uv\n"
                        + "FROM (\n"
                        + "  SELECT\n"
                        + "    a,\n"
                        + "    window_start,\n"
                        + "    window_end,\n"
                        + "    count(*) as cnt,\n"
                        + "    count(distinct c) AS uv\n"
                        + "  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))\n"
                        + "  GROUP BY a, window_start, window_end, window_time\n"
                        + ") L\n"
                        + "JOIN (\n"
                        + "  SELECT\n"
                        + "    a,\n"
                        + "    window_start,\n"
                        + "    window_end,\n"
                        + "    count(*) as cnt,\n"
                        + "    count(distinct c) AS uv\n"
                        + "  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))\n"
                        + "  GROUP BY a, window_start, window_end, window_time\n"
                        + ") R\n"
                        + "ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a");
    }

    /** Verify WindowTableFunction and WindowRank. */
    @Test
    public void testWindowRank() {
        createSourceWithTimeAttribute();
        verifyQuery(
                "select\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  a,\n"
                        + "  b,\n"
                        + "  c\n"
                        + "FROM (\n"
                        + "  SELECT\n"
                        + "    *,\n"
                        + "   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY b DESC) as rownum\n"
                        + "  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE)))\n"
                        + "WHERE rownum <= 3");
    }

    /** Verify WindowDeduplicate. */
    @Test
    public void testWindowDeduplicate() {
        createSourceWithTimeAttribute();
        verifyQuery(
                "select\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  a,\n"
                        + "  b,\n"
                        + "  c\n"
                        + "FROM (\n"
                        + "  SELECT\n"
                        + "    *,\n"
                        + "   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY rowtime DESC) as rownum\n"
                        + "  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE)))\n"
                        + "WHERE rownum <= 1");
    }

    /** Verify LegacySource and LegacySink. */
    @Test
    public void testLegacySourceSink() {
        TableSchema schema = TestLegacyFilterableTableSource.defaultSchema();
        TestLegacyFilterableTableSource.createTemporaryTable(
                tEnv,
                schema,
                "MySource",
                true,
                TestLegacyFilterableTableSource.defaultRows().toList(),
                TestLegacyFilterableTableSource.defaultFilterableFields());
        TableSink<Row> sink =
                util.createAppendTableSink(
                        schema.getFieldNames(),
                        schema.getTableColumns().stream()
                                .map(col -> col.getType().getLogicalType())
                                .toArray(LogicalType[]::new));
        util.testingTableEnv().registerTableSinkInternal("MySink", sink);
        verifyInsert("insert into MySink select * from MySource");
    }
}
