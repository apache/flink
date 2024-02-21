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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.ConcatDistinctAggFunction;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.VarSum1AggFunction;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.VarSum2AggFunction;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvg;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.time.Duration;

/** {@link TableTestProgram} definitions for testing {@link StreamExecGroupAggregate}. */
public class GroupAggregateTestPrograms {

    static final SourceTestStep SOURCE_ONE =
            SourceTestStep.newBuilder("source_t")
                    .addSchema("a INT", "b BIGINT", "c VARCHAR")
                    .producedBeforeRestore(
                            Row.of(1, 1L, "Hi"),
                            Row.of(2, 2L, "Hello"),
                            Row.of(2, 2L, "Hello World"))
                    .producedAfterRestore(
                            Row.of(1, 1L, "Hi Again!"),
                            Row.of(2, 2L, "Hello Again!"),
                            Row.of(2, 2L, "Hello World Again!"))
                    .build();

    static final SourceTestStep SOURCE_TWO =
            SourceTestStep.newBuilder("source_t")
                    .addSchema("a INT", "b BIGINT", "c INT", "d VARCHAR", "e BIGINT")
                    .producedBeforeRestore(
                            Row.of(2, 3L, 2, "Hello World Like", 1L),
                            Row.of(3, 4L, 3, "Hello World Its nice", 2L),
                            Row.of(2, 2L, 1, "Hello World", 2L),
                            Row.of(1, 1L, 0, "Hello", 1L),
                            Row.of(5, 11L, 10, "GHI", 1L),
                            Row.of(3, 5L, 4, "ABC", 2L),
                            Row.of(4, 10L, 9, "FGH", 2L),
                            Row.of(4, 7L, 6, "CDE", 2L),
                            Row.of(5, 14L, 13, "JKL", 2L),
                            Row.of(4, 9L, 8, "EFG", 1L),
                            Row.of(5, 15L, 14, "KLM", 2L),
                            Row.of(5, 12L, 11, "HIJ", 3L),
                            Row.of(4, 8L, 7, "DEF", 1L),
                            Row.of(5, 13L, 12, "IJK", 3L),
                            Row.of(3, 6L, 5, "BCD", 3L))
                    .producedAfterRestore(
                            Row.of(1, 1L, 0, "Hello", 1L),
                            Row.of(3, 5L, 4, "ABC", 2L),
                            Row.of(4, 10L, 9, "FGH", 2L),
                            Row.of(4, 7L, 6, "CDE", 2L),
                            Row.of(7, 7L, 7, "MNO", 7L),
                            Row.of(3, 6L, 5, "BCD", 3L),
                            Row.of(7, 7L, 7, "XYZ", 7L))
                    .build();

    static final TableTestProgram GROUP_BY_SIMPLE =
            TableTestProgram.of(
                            "group-aggregate-simple", "validates basic aggregation using group by")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "b BIGINT",
                                            "cnt BIGINT",
                                            "avg_a DOUBLE",
                                            "min_c VARCHAR",
                                            "PRIMARY KEY (b) NOT ENFORCED")
                                    .consumedBeforeRestore(
                                            "+I[1, 1, null, Hi]",
                                            "+I[2, 1, 2.0, Hello]",
                                            "+U[2, 2, 2.0, Hello]")
                                    .consumedAfterRestore(
                                            "+U[1, 2, null, Hi]",
                                            "+U[2, 3, 2.0, Hello]",
                                            "+U[2, 4, 2.0, Hello]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "b, "
                                    + "COUNT(*) AS cnt, "
                                    + "AVG(a) FILTER (WHERE a > 1) AS avg_a, "
                                    + "MIN(c) AS min_c "
                                    + "FROM source_t GROUP BY b")
                    .build();

    static final TableTestProgram GROUP_BY_SIMPLE_MINI_BATCH =
            TableTestProgram.of(
                            "group-aggregate-simple-mini-batch",
                            "validates basic aggregation using group by with mini batch")
                    .setupTableSource(SOURCE_ONE)
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                            Duration.ofSeconds(10))
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5L)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            GROUP_BY_SIMPLE
                                                    .getSetupSinkTestSteps()
                                                    .get(0)
                                                    .schemaComponents)
                                    .consumedBeforeRestore(
                                            "+I[1, 1, null, Hi]", "+I[2, 2, 2.0, Hello]")
                                    .consumedAfterRestore(
                                            "+U[1, 2, null, Hi]", "+U[2, 4, 2.0, Hello]")
                                    .build())
                    .runSql(GroupAggregateTestPrograms.GROUP_BY_SIMPLE.getRunSqlTestStep().sql)
                    .build();

    static final TableTestProgram GROUP_BY_DISTINCT =
            TableTestProgram.of("group-aggregate-distinct", "validates group by distinct")
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "e BIGINT",
                                            "cnt_a1 BIGINT",
                                            "cnt_a2 BIGINT",
                                            "sum_a BIGINT",
                                            "sum_b BIGINT",
                                            "avg_b DOUBLE",
                                            "cnt_d BIGINT",
                                            "PRIMARY KEY (e) NOT ENFORCED")
                                    .consumedBeforeRestore(
                                            "+I[1, 0, 1, 2, 3, 3.0, 1]",
                                            "+I[2, 0, 1, 3, 4, 4.0, 1]",
                                            "+U[2, 0, 2, 5, 6, 3.0, 2]",
                                            "+U[1, 0, 2, 3, 4, 2.0, 2]",
                                            "+U[1, 1, 3, 8, 15, 5.0, 3]",
                                            "+U[2, 0, 2, 5, 11, 3.0, 3]",
                                            "+U[2, 0, 3, 9, 21, 5.0, 4]",
                                            "+U[2, 0, 3, 9, 28, 5.0, 5]",
                                            "+U[2, 1, 4, 14, 42, 7.0, 6]",
                                            "+U[1, 1, 4, 12, 24, 6.0, 4]",
                                            "+U[2, 1, 4, 14, 57, 8.0, 7]",
                                            "+I[3, 1, 1, 5, 12, 12.0, 1]",
                                            "+U[1, 1, 4, 12, 32, 6.0, 5]",
                                            "+U[3, 1, 1, 5, 25, 12.0, 2]",
                                            "+U[3, 1, 2, 8, 31, 10.0, 3]")
                                    .consumedAfterRestore(
                                            "+U[1, 1, 4, 12, 32, 5.0, 5]",
                                            "+U[2, 1, 4, 14, 57, 7.0, 7]",
                                            "+U[2, 1, 4, 14, 57, 8.0, 7]",
                                            "+U[2, 1, 4, 14, 57, 7.0, 7]",
                                            "+I[7, 0, 1, 7, 7, 7.0, 1]",
                                            "+U[3, 1, 2, 8, 31, 9.0, 3]",
                                            "+U[7, 0, 1, 7, 7, 7.0, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "e, "
                                    + "COUNT(DISTINCT a) FILTER (WHERE b > 10) AS cnt_a1, "
                                    + "COUNT(DISTINCT a) AS cnt_a2, "
                                    + "SUM(DISTINCT a) AS sum_a, "
                                    + "SUM(DISTINCT b) AS sum_b, "
                                    + "AVG(b) AS avg_b, "
                                    + "COUNT(DISTINCT d) AS concat_d "
                                    + "FROM source_t GROUP BY e")
                    .build();

    static final TableTestProgram GROUP_BY_DISTINCT_MINI_BATCH =
            TableTestProgram.of(
                            "group-aggregate-distinct-mini-batch",
                            "validates group by distinct with mini batch")
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                            Duration.ofSeconds(10))
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5L)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            GROUP_BY_DISTINCT
                                                    .getSetupSinkTestSteps()
                                                    .get(0)
                                                    .schemaComponents)
                                    .consumedBeforeRestore(
                                            "+I[3, 1, 2, 8, 31, 10.0, 3]",
                                            "+I[2, 1, 4, 14, 42, 7.0, 6]",
                                            "+I[1, 1, 4, 12, 24, 6.0, 4]",
                                            "+U[2, 1, 4, 14, 57, 8.0, 7]",
                                            "+U[1, 1, 4, 12, 32, 6.0, 5]")
                                    .consumedAfterRestore(
                                            "+U[3, 1, 2, 8, 31, 9.0, 3]",
                                            "+U[2, 1, 4, 14, 57, 7.0, 7]",
                                            "+I[7, 0, 1, 7, 7, 7.0, 2]",
                                            "+U[1, 1, 4, 12, 32, 5.0, 5]")
                                    .build())
                    .runSql(GROUP_BY_DISTINCT.getRunSqlTestStep().sql)
                    .build();

    static final TableTestProgram GROUP_BY_UDF_WITH_MERGE =
            TableTestProgram.of(
                            "group-aggregate-udf-with-merge",
                            "validates udfs with merging using group by")
                    .setupCatalogFunction("my_avg", WeightedAvgWithMerge.class)
                    .setupTemporarySystemFunction("my_concat", ConcatDistinctAggFunction.class)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "d BIGINT",
                                            "s1 BIGINT",
                                            "c1 VARCHAR",
                                            "PRIMARY KEY (d) NOT ENFORCED")
                                    .consumedBeforeRestore(
                                            "+I[1, 1, Hello World Like]",
                                            "+I[2, 2, Hello World Its nice]",
                                            "+U[2, 2, Hello World Its nice|Hello World]",
                                            "+U[1, 1, Hello World Like|Hello]",
                                            "+U[1, 1, Hello World Like|Hello|GHI]",
                                            "+U[2, 2, Hello World Its nice|Hello World|ABC]",
                                            "+U[2, 2, Hello World Its nice|Hello World|ABC|FGH]",
                                            "+U[2, 2, Hello World Its nice|Hello World|ABC|FGH|CDE]",
                                            "+U[2, 2, Hello World Its nice|Hello World|ABC|FGH|CDE|JKL]",
                                            "+U[1, 1, Hello World Like|Hello|GHI|EFG]",
                                            "+U[2, 2, Hello World Its nice|Hello World|ABC|FGH|CDE|JKL|KLM]",
                                            "+I[3, 3, HIJ]",
                                            "+U[1, 1, Hello World Like|Hello|GHI|EFG|DEF]",
                                            "+U[3, 3, HIJ|IJK]",
                                            "+U[3, 3, HIJ|IJK|BCD]")
                                    .consumedAfterRestore("+I[7, 7, MNO]", "+U[7, 7, MNO|XYZ]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "e, "
                                    + "my_avg(e, a) as s1, "
                                    + "my_concat(d) as c1 "
                                    + "FROM source_t GROUP BY e")
                    .build();

    static final TableTestProgram GROUP_BY_UDF_WITH_MERGE_MINI_BATCH =
            TableTestProgram.of(
                            "group-aggregate-udf-with-merge-mini-batch",
                            "validates udfs with merging using group by with mini batch")
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                            Duration.ofSeconds(10))
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5L)
                    .setupCatalogFunction("my_avg", WeightedAvgWithMerge.class)
                    .setupTemporarySystemFunction("my_concat", ConcatDistinctAggFunction.class)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            GROUP_BY_UDF_WITH_MERGE
                                                    .getSetupSinkTestSteps()
                                                    .get(0)
                                                    .schemaComponents)
                                    .consumedBeforeRestore(
                                            "+I[2, 2, Hello World Its nice|Hello World|ABC|FGH|CDE|JKL]",
                                            "+I[1, 1, Hello World Like|Hello|GHI|EFG]",
                                            "+U[2, 2, Hello World Its nice|Hello World|ABC|FGH|CDE|JKL|KLM]",
                                            "+U[1, 1, Hello World Like|Hello|GHI|EFG|DEF]",
                                            "+I[3, 3, HIJ|IJK|BCD]")
                                    .consumedAfterRestore("+I[7, 7, MNO|XYZ]")
                                    .build())
                    .runSql(GROUP_BY_UDF_WITH_MERGE.getRunSqlTestStep().sql)
                    .build();

    static final TableTestProgram GROUP_BY_UDF_WITHOUT_MERGE =
            TableTestProgram.of(
                            "group-aggregate-udf-without-merge",
                            "validates udfs without merging using group by")
                    .setupTemporarySystemFunction("my_sum1", VarSum1AggFunction.class)
                    .setupCatalogFunction("my_avg", WeightedAvg.class)
                    .setupTemporarySystemFunction("my_sum2", VarSum2AggFunction.class)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "d BIGINT",
                                            "s1 BIGINT",
                                            "s2 BIGINT",
                                            "s3 BIGINT",
                                            "PRIMARY KEY (d) NOT ENFORCED")
                                    .consumedBeforeRestore(
                                            "+I[1, 12, 0, 1]",
                                            "+I[2, 13, 0, 2]",
                                            "+U[2, 24, 0, 2]",
                                            "+U[1, 22, 0, 1]",
                                            "+U[1, 42, 0, 1]",
                                            "+U[2, 38, 0, 2]",
                                            "+U[2, 57, 0, 2]",
                                            "+U[2, 73, 0, 2]",
                                            "+U[2, 96, 0, 2]",
                                            "+U[1, 60, 0, 1]",
                                            "+U[2, 120, 0, 2]",
                                            "+I[3, 21, 0, 3]",
                                            "+U[1, 77, 0, 1]",
                                            "+U[3, 43, 0, 3]",
                                            "+U[3, 58, 0, 3]")
                                    .consumedAfterRestore(
                                            "+U[1, 87, 0, 1]",
                                            "+U[2, 134, 0, 2]",
                                            "+U[2, 153, 0, 2]",
                                            "+U[2, 169, 0, 2]",
                                            "+I[7, 17, 0, 7]",
                                            "+U[3, 73, 0, 3]",
                                            "+U[7, 34, 0, 7]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "e, "
                                    + "my_sum1(c, 10) as s1, "
                                    + "my_sum2(5, c) as s2, "
                                    + "my_avg(e, a) as s3 "
                                    + "FROM source_t GROUP BY e")
                    .build();

    static final TableTestProgram GROUP_BY_UDF_WITHOUT_MERGE_MINI_BATCH =
            TableTestProgram.of(
                            "group-aggregate-udf-without-merge-mini-batch",
                            "validates udfs without merging using group by with mini batch")
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                            Duration.ofSeconds(10))
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5L)
                    .setupTemporarySystemFunction("my_sum1", VarSum1AggFunction.class)
                    .setupCatalogFunction("my_avg", WeightedAvg.class)
                    .setupTemporarySystemFunction("my_sum2", VarSum2AggFunction.class)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            GROUP_BY_UDF_WITHOUT_MERGE
                                                    .getSetupSinkTestSteps()
                                                    .get(0)
                                                    .schemaComponents)
                                    .consumedBeforeRestore(
                                            "+I[2, 24, 0, 2]",
                                            "+I[1, 42, 0, 1]",
                                            "+U[2, 96, 0, 2]",
                                            "+U[1, 60, 0, 1]",
                                            "+I[3, 58, 0, 3]",
                                            "+U[2, 120, 0, 2]",
                                            "+U[1, 77, 0, 1]")
                                    .consumedAfterRestore(
                                            "+I[7, 17, 0, 7]",
                                            "+U[1, 87, 0, 1]",
                                            "+U[2, 169, 0, 2]",
                                            "+U[3, 73, 0, 3]",
                                            "+U[7, 34, 0, 7]")
                                    .build())
                    .runSql(GROUP_BY_UDF_WITHOUT_MERGE.getRunSqlTestStep().sql)
                    .build();

    static final TableTestProgram AGG_WITH_STATE_TTL_HINT =
            TableTestProgram.of("agg-with-state-ttl-hint", "agg with state ttl hint")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "b BIGINT",
                                            "cnt BIGINT",
                                            "avg_a DOUBLE",
                                            "min_c VARCHAR",
                                            "PRIMARY KEY (b) NOT ENFORCED")
                                    .consumedBeforeRestore(
                                            "+I[1, 1, null, Hi]",
                                            "+I[2, 1, 2.0, Hello]",
                                            "+U[2, 2, 2.0, Hello]")
                                    // Due to state TTL in hint, the state in the metadata
                                    // savepoint has expired.
                                    .consumedAfterRestore(
                                            "+I[1, 1, null, Hi Again!]",
                                            "+I[2, 1, 2.0, Hello Again!]",
                                            "+U[2, 2, 2.0, Hello Again!]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT /*+ STATE_TTL('source_t' = '1s') */"
                                    + "b, "
                                    + "COUNT(*) AS cnt, "
                                    + "AVG(a) FILTER (WHERE a > 1) AS avg_a, "
                                    + "MIN(c) AS min_c "
                                    + "FROM source_t GROUP BY b")
                    .build();
}
