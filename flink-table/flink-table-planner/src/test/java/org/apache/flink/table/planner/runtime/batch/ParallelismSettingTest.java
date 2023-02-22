/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * imitations under the License.
 */

package org.apache.flink.table.planner.runtime.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for parallelism setting when translating ExecNode to Transformation. */
public class ParallelismSettingTest extends TableTestBase {

    private BatchTableTestUtil util;

    @Before
    public void before() {
        util = batchTestUtil(TableConfig.getDefault());
        util.getTableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable (\n"
                                + "  a BIGINT,\n"
                                + "  b BIGINT,\n"
                                + "  c VARCHAR\n"
                                + ") WITH (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'format' = 'testcsv',\n"
                                + "  'path' = '/tmp')");
    }

    @Test
    public void testParallelismSettingAfterSingletonShuffleRemove() {
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink (\n"
                                + "  b bigint\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false',\n"
                                + "  'table-sink-class' = 'DEFAULT')");

        // the exec plan:
        // Sink(table=[default_catalog.default_database.MySink], fields=[EXPR$0])
        // +- HashAggregate(isMerge=[false], select=[MAX(b) AS EXPR$0])
        //   +- HashAggregate(isMerge=[true], select=[Final_SUM(sum$0) AS b])
        //      +- Exchange(distribution=[single])
        //         +- LocalHashAggregate(select=[Partial_SUM(b) AS sum$0])
        //            +- TableSourceScan(table=[[MyTable, project=[b])

        Transformation<?> sink =
                generateTransformation(
                        "INSERT INTO MySink SELECT MAX(b) FROM (SELECT SUM(b) AS b FROM MyTable)");
        Transformation<?> topAgg = sink.getInputs().get(0);
        assertThat(topAgg.getParallelism()).isEqualTo(1);
        assertThat(topAgg.getMaxParallelism()).isEqualTo(1);
        Transformation<?> bottomAgg = topAgg.getInputs().get(0);
        assertThat(bottomAgg.getParallelism()).isEqualTo(1);
        assertThat(bottomAgg.getMaxParallelism()).isEqualTo(1);
    }

    @Test
    public void testSortQuery() {
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink (\n"
                                + "  a bigint,\n"
                                + "  b bigint,\n"
                                + "  c varchar\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false',\n"
                                + "  'table-sink-class' = 'DEFAULT')");

        // the exec plan:
        // Sink(table=[default_catalog.default_database.MySink], fields=[EXPR$0])
        // +- Sort(orderBy=[a ASC])
        //    +- Exchange(distribution=[single])
        //       +- TableSourceScan(table=[[default_catalog, default_database, MyTable]], fields=[a,
        // b, c])

        Transformation<?> sink =
                generateTransformation("INSERT INTO MySink SELECT * FROM MyTable ORDER BY a");
        Transformation<?> sort = sink.getInputs().get(0);
        assertThat(sort.getParallelism()).isEqualTo(1);
        assertThat(sort.getMaxParallelism()).isEqualTo(1);
        Transformation<?> exchange = sort.getInputs().get(0);
        assertThat(exchange.getParallelism()).isEqualTo(1);
        assertThat(exchange.getMaxParallelism()).isEqualTo(1);
        Transformation<?> source = exchange.getInputs().get(0);
        assertThat(source.getParallelism()).isEqualTo(4);
        assertThat(source.getMaxParallelism()).isEqualTo(-1);
    }

    @Test
    public void testLimitQuery() {
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink (\n"
                                + "  a bigint,\n"
                                + "  b bigint,\n"
                                + "  c varchar\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false',\n"
                                + "  'table-sink-class' = 'DEFAULT')");

        // the exec plan:
        // Sink(table=[default_catalog.default_database.MySink], fields=[EXPR$0])
        // +- Limit(offset=[0], fetch=[5], global=[true])
        //    +- Exchange(distribution=[single])
        //       +- Limit(offset=[0], fetch=[5], global=[false])
        //          +- TableSourceScan(table=[[default_catalog, default_database, MyTable]],
        // fields=[a, b, c])

        Transformation<?> sink =
                generateTransformation("INSERT INTO MySink SELECT * FROM MyTable LIMIT 5");
        Transformation<?> topLimit = sink.getInputs().get(0);
        assertThat(topLimit.getParallelism()).isEqualTo(1);
        assertThat(topLimit.getMaxParallelism()).isEqualTo(1);
        Transformation<?> exchange = topLimit.getInputs().get(0);
        assertThat(exchange.getParallelism()).isEqualTo(1);
        assertThat(exchange.getMaxParallelism()).isEqualTo(1);
        Transformation<?> bottomLimit = exchange.getInputs().get(0);
        assertThat(bottomLimit.getParallelism()).isEqualTo(4);
        assertThat(bottomLimit.getMaxParallelism()).isEqualTo(-1);
    }

    @Test
    public void testSortLimitQuery() {
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink (\n"
                                + "  a bigint,\n"
                                + "  b bigint,\n"
                                + "  c varchar\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false',\n"
                                + "  'table-sink-class' = 'DEFAULT')");

        // the exec plan:
        // Sink(table=[default_catalog.default_database.MySink], fields=[EXPR$0])
        // +- SortLimit(orderBy=[a ASC], offset=[0], fetch=[5], global=[true])
        //    +- Exchange(distribution=[single])
        //        +- SortLimit(orderBy=[a ASC], offset=[0], fetch=[5], global=[false])
        //            +- TableSourceScan(table=[[default_catalog, default_database, MyTable]],
        // fields=[a, b, c])

        Transformation<?> sink =
                generateTransformation(
                        "INSERT INTO MySink SELECT * FROM MyTable ORDER BY a LIMIT 5");
        Transformation<?> topSortLimit = sink.getInputs().get(0);
        assertThat(topSortLimit.getParallelism()).isEqualTo(1);
        assertThat(topSortLimit.getMaxParallelism()).isEqualTo(1);
        Transformation<?> exchange = topSortLimit.getInputs().get(0);
        assertThat(exchange.getParallelism()).isEqualTo(1);
        assertThat(exchange.getMaxParallelism()).isEqualTo(1);
        Transformation<?> bottomSortLimit = exchange.getInputs().get(0);
        assertThat(bottomSortLimit.getParallelism()).isEqualTo(4);
        assertThat(bottomSortLimit.getMaxParallelism()).isEqualTo(-1);
    }

    @Test
    public void testRankQuery() {
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink (\n"
                                + "  a bigint,\n"
                                + "  b bigint,\n"
                                + "  rk bigint\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false',\n"
                                + "  'table-sink-class' = 'DEFAULT')");

        // the exec plan:
        // Sink(table=[default_catalog.default_database.MySink], fields=[EXPR$0])
        // +- Calc(select=[a, b, 2 AS $2])
        //    +- Rank(rankType=[RANK], rankRange=[rankStart=2, rankEnd=2], partitionBy=[],
        // orderBy=[a ASC, c ASC], global=[true], select=[a, b, c])
        //       +- Sort(orderBy=[a ASC, c ASC])
        //          +- Exchange(distribution=[single])
        //             +- Rank(rankType=[RANK], rankRange=[rankStart=1, rankEnd=2], partitionBy=[],
        // orderBy=[a ASC, c ASC], global=[false], select=[a, b, c])
        //                +- Sort(orderBy=[a ASC, c ASC])
        //                   +- TableSourceScan(table=[[default_catalog, default_database,
        // MyTable]], fields=[a, b, c])

        Transformation<?> sink =
                generateTransformation(
                        "INSERT INTO MySink SELECT * FROM "
                                + "(SELECT a, b, RANK() OVER (ORDER BY a, c) rk FROM MyTable) t "
                                + "WHERE rk = 2");
        Transformation<?> calc = sink.getInputs().get(0);
        assertThat(calc.getParallelism()).isEqualTo(1);
        assertThat(calc.getMaxParallelism()).isEqualTo(-1);
        Transformation<?> topRank = calc.getInputs().get(0);
        assertThat(topRank.getParallelism()).isEqualTo(1);
        assertThat(topRank.getMaxParallelism()).isEqualTo(1);
        Transformation<?> topSort = topRank.getInputs().get(0);
        assertThat(topSort.getParallelism()).isEqualTo(1);
        assertThat(topSort.getMaxParallelism()).isEqualTo(1);
        Transformation<?> exchange = topSort.getInputs().get(0);
        assertThat(exchange.getParallelism()).isEqualTo(1);
        assertThat(exchange.getMaxParallelism()).isEqualTo(1);
        Transformation<?> bottomRank = exchange.getInputs().get(0);
        assertThat(bottomRank.getParallelism()).isEqualTo(4);
        assertThat(bottomRank.getMaxParallelism()).isEqualTo(-1);
        Transformation<?> bottomSort = bottomRank.getInputs().get(0);
        assertThat(bottomSort.getParallelism()).isEqualTo(4);
        assertThat(bottomSort.getMaxParallelism()).isEqualTo(-1);
    }

    @Test
    public void testJoinQuery() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable2 (\n"
                                + "  d BIGINT,\n"
                                + "  e BIGINT,\n"
                                + "  f VARCHAR\n"
                                + ") WITH (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'format' = 'testcsv',\n"
                                + "  'path' = '/tmp')");

        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink (\n"
                                + "  a bigint,\n"
                                + "  b bigint,\n"
                                + "  d bigint,\n"
                                + "  e bigint\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false',\n"
                                + "  'table-sink-class' = 'DEFAULT')");
        util.getTableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "HashJoin, SortMergeJoin");

        // the exec plan:
        // Sink(table=[default_catalog.default_database.MySink], fields=[EXPR$0])
        // +- NestedLoopJoin(joinType=[FullOuterJoin], where=[(a = d)], select=[a, b, d, e],
        // build=[left])
        //    :- Exchange(distribution=[single])
        //    :  +- Calc(select=[a, b])
        //    :     +- TableSourceScan(table=[[default_catalog, default_database, MyTable]],
        // fields=[a, b, c])
        //    +- Exchange(distribution=[single])
        //       +- Calc(select=[d, e])
        //          +- +- TableSourceScan(table=[[default_catalog, default_database, MyTable2]],
        // fields=[d, e, f])

        Transformation<?> sink =
                generateTransformation(
                        "INSERT INTO MySink SELECT a, b, d, e FROM MyTable FULL JOIN MyTable2 on a = d");
        Transformation<?> join = sink.getInputs().get(0);
        assertThat(join.getParallelism()).isEqualTo(1);
        assertThat(join.getMaxParallelism()).isEqualTo(1);

        Transformation<?> leftExchange = join.getInputs().get(0);
        assertThat(leftExchange.getParallelism()).isEqualTo(1);
        assertThat(leftExchange.getMaxParallelism()).isEqualTo(1);
        Transformation<?> leftCalc = leftExchange.getInputs().get(0);
        assertThat(leftCalc.getParallelism()).isEqualTo(4);
        assertThat(leftCalc.getMaxParallelism()).isEqualTo(-1);

        Transformation<?> rightExchange = join.getInputs().get(1);
        assertThat(rightExchange.getParallelism()).isEqualTo(1);
        assertThat(rightExchange.getMaxParallelism()).isEqualTo(1);
        Transformation<?> rightCalc = rightExchange.getInputs().get(0);
        assertThat(rightCalc.getParallelism()).isEqualTo(4);
        assertThat(rightCalc.getMaxParallelism()).isEqualTo(-1);
    }

    private Transformation<?> generateTransformation(String statement) {
        List<Operation> operations = util.getPlanner().getParser().parse(statement);
        List<Transformation<?>> transformations =
                util.getPlanner()
                        .translate(
                                Collections.singletonList((ModifyOperation) (operations.get(0))));
        assertThat(transformations).hasSize(1);
        return transformations.get(0);
    }
}
