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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.factories.TestValuesCatalog;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.optimize.program.FlinkRuntimeFilterProgramTest;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for runtime filter. */
class RuntimeFilterITCase extends BatchTestBase {

    private final TestValuesCatalog catalog =
            new TestValuesCatalog("testCatalog", "test_database", true);
    private TableEnvironment tEnv;

    static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of(BatchShuffleMode.ALL_EXCHANGES_BLOCKING, true),
                Arguments.of(BatchShuffleMode.ALL_EXCHANGES_BLOCKING, false),
                Arguments.of(BatchShuffleMode.ALL_EXCHANGES_PIPELINED, true),
                Arguments.of(BatchShuffleMode.ALL_EXCHANGES_PIPELINED, false));
    }

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        tEnv = tEnv();
        catalog.open();
        tEnv.registerCatalog("testCatalog", catalog);
        tEnv.useCatalog("testCatalog");
        tEnv.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_RUNTIME_FILTER_ENABLED, true);
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_RUNTIME_FILTER_MAX_BUILD_DATA_SIZE,
                        MemorySize.parse("10m"));

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE dim (\n"
                                + "  x INT,\n"
                                + "  y INT,\n"
                                + "  z BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(TestData.data7())));

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE fact (\n"
                                + "  `a` INT,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` INT,\n"
                                + "  `d` VARCHAR,\n"
                                + "  `e` BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'disable-lookup' = 'true',\n"
                                + "  'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(TestData.data5())));

        // note that we set the row count here so that the runtime filter can take effect
        // it is not the real data volume of the table
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "dim"),
                new CatalogTableStatistics(
                        FlinkRuntimeFilterProgramTest.SUITABLE_DIM_ROW_COUNT, 1, 1, 1),
                false);
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "fact"),
                new CatalogTableStatistics(
                        FlinkRuntimeFilterProgramTest.SUITABLE_FACT_ROW_COUNT, 1, 1, 1),
                false);
    }

    @ParameterizedTest(name = "mode = {0}, ofcg = {1}")
    @MethodSource("parameters")
    void testSimpleRuntimeFilter(BatchShuffleMode shuffleMode, boolean ofcg) {
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, ofcg);
        configBatchShuffleMode(tEnv.getConfig(), shuffleMode);
        String sql = "select * from fact, dim where x = a and z >= 3";
        // Check runtime filter is working
        assertThat(tEnv().explainSql(sql)).contains("RuntimeFilter");
        checkResult(
                sql,
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(3, 4, 3, "Hallo Welt wie gehts?", 2, 3, 4, 3),
                                Row.of(3, 5, 4, "ABC", 2, 3, 4, 3),
                                Row.of(3, 6, 5, "BCD", 3, 3, 4, 3),
                                Row.of(5, 11, 10, "GHI", 1, 5, 10, 3),
                                Row.of(5, 12, 11, "HIJ", 3, 5, 10, 3),
                                Row.of(5, 13, 12, "IJK", 3, 5, 10, 3),
                                Row.of(5, 14, 13, "JKL", 2, 5, 10, 3),
                                Row.of(5, 15, 14, "KLM", 2, 5, 10, 3))),
                false);
    }

    @ParameterizedTest(name = "mode = {0}, ofcg = {1}")
    @MethodSource("parameters")
    void testRuntimeFilterWithBuildSidePushDown(BatchShuffleMode shuffleMode, boolean ofcg) {
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, ofcg);
        configBatchShuffleMode(tEnv.getConfig(), shuffleMode);
        // The following two config are used to let the build side is a direct Agg (without
        // Exchange)
        tEnv.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1L);
        tEnv.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "ONE_PHASE");

        String sql =
                "select * from fact join (select x, sum(z) from dim where z = 2 group by x) dimSide on x = a";
        // Check runtime filter is working
        assertThat(tEnv().explainSql(sql)).contains("RuntimeFilter");
        checkResult(
                sql,
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(2, 2, 1, "Hallo Welt", 2, 2, 2),
                                Row.of(2, 3, 2, "Hallo Welt wie", 1, 2, 2),
                                Row.of(3, 4, 3, "Hallo Welt wie gehts?", 2, 3, 2),
                                Row.of(3, 5, 4, "ABC", 2, 3, 2),
                                Row.of(3, 6, 5, "BCD", 3, 3, 2),
                                Row.of(4, 10, 9, "FGH", 2, 4, 4),
                                Row.of(4, 7, 6, "CDE", 2, 4, 4),
                                Row.of(4, 8, 7, "DEF", 1, 4, 4),
                                Row.of(4, 9, 8, "EFG", 1, 4, 4),
                                Row.of(5, 11, 10, "GHI", 1, 5, 2),
                                Row.of(5, 12, 11, "HIJ", 3, 5, 2),
                                Row.of(5, 13, 12, "IJK", 3, 5, 2),
                                Row.of(5, 14, 13, "JKL", 2, 5, 2),
                                Row.of(5, 15, 14, "KLM", 2, 5, 2))),
                false);
    }

    @ParameterizedTest(name = "mode = {0}, ofcg = {1}")
    @MethodSource("parameters")
    void testRuntimeFilterWithProbeSidePushDown(BatchShuffleMode shuffleMode, boolean ofcg)
            throws Exception {
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, ofcg);
        configBatchShuffleMode(tEnv.getConfig(), shuffleMode);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE fact2 (\n"
                                + "  `a` INT,\n"
                                + "  `b` BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'disable-lookup' = 'true',\n"
                                + "  'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(TestData.data5().take(5).toList())));

        // note that we set the row count here so that the runtime filter can take effect
        // it is not the real data volume of the table
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "fact2"),
                new CatalogTableStatistics(
                        FlinkRuntimeFilterProgramTest.SUITABLE_FACT_ROW_COUNT, 1, 1, 1),
                false);

        String sql =
                "select * from fact, fact2, dim where fact.a = fact2.a and fact.a = dim.x and z = 2";
        // Check runtime filter is working
        assertThat(tEnv().explainSql(sql)).contains("RuntimeFilter");
        checkResult(
                sql,
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(2, 2, 1, "Hallo Welt", 2, 2, 2, 2, 2, 2),
                                Row.of(2, 2, 1, "Hallo Welt", 2, 2, 3, 2, 2, 2),
                                Row.of(2, 3, 2, "Hallo Welt wie", 1, 2, 2, 2, 2, 2),
                                Row.of(2, 3, 2, "Hallo Welt wie", 1, 2, 3, 2, 2, 2),
                                Row.of(3, 4, 3, "Hallo Welt wie gehts?", 2, 3, 4, 3, 3, 2),
                                Row.of(3, 4, 3, "Hallo Welt wie gehts?", 2, 3, 5, 3, 3, 2),
                                Row.of(3, 5, 4, "ABC", 2, 3, 4, 3, 3, 2),
                                Row.of(3, 5, 4, "ABC", 2, 3, 5, 3, 3, 2),
                                Row.of(3, 6, 5, "BCD", 3, 3, 4, 3, 3, 2),
                                Row.of(3, 6, 5, "BCD", 3, 3, 5, 3, 3, 2))),
                false);
    }
}
