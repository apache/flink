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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

/** IT test for dynamic filtering. */
public class DynamicFilteringITCase extends BatchTestBase {

    private TableEnvironment tEnv;
    private Catalog catalog;

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
        catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED, true);

        String dataId1 = TestValuesTableFactory.registerData(TestData.data7());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE dim (\n"
                                + "  x INT,\n"
                                + "  y INT,\n"
                                + "  z BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId1));

        String dataId2 = TestValuesTableFactory.registerData(TestData.data5());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE fact1 (\n"
                                + "  `a` INT,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` INT,\n"
                                + "  `d` VARCHAR,\n"
                                + "  `e` BIGINT\n"
                                + ") partitioned by (a)\n"
                                + " WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + "  'partition-list' = 'a:1;a:2;a:3;a:4;a:5;',\n"
                                + "  'dynamic-filtering-fields' = 'a',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'disable-lookup' = 'true',\n"
                                + "  'bounded' = 'true'\n"
                                + ")",
                        dataId2));

        String dataId3 = TestValuesTableFactory.registerData(TestData.data5());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE fact2 (\n"
                                + "  `a` INT,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` INT,\n"
                                + "  `d` VARCHAR,\n"
                                + "  `e` BIGINT\n"
                                + ") partitioned by (e, a)\n"
                                + " WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + "  'partition-list' = 'e:1,a:1;e:1,a:2;e:1,a:4;e:1,a:5;e:2,a:2;e:2,a:3;e:2,a:4;e:2,a:5;e:3,a:3;e:3,a:5;',\n"
                                + "  'dynamic-filtering-fields' = 'a;e',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'disable-lookup' = 'true',\n"
                                + "  'bounded' = 'true'\n"
                                + ")",
                        dataId3));
    }

    @ParameterizedTest(name = "mode = {0}, ofcg = {1}")
    @MethodSource("parameters")
    public void testSimpleDynamicFiltering(BatchShuffleMode shuffleMode, boolean ofcg) {
        configBatchShuffleMode(tEnv.getConfig(), shuffleMode);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, ofcg);
        checkResult(
                "SELECT * FROM fact1, dim WHERE x = a AND z = 2",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(2, 2, 1, "Hallo Welt", 2, 2, 2, 2),
                                Row.of(2, 3, 2, "Hallo Welt wie", 1, 2, 2, 2),
                                Row.of(3, 4, 3, "Hallo Welt wie gehts?", 2, 3, 3, 2),
                                Row.of(3, 5, 4, "ABC", 2, 3, 3, 2),
                                Row.of(3, 6, 5, "BCD", 3, 3, 3, 2),
                                Row.of(4, 10, 9, "FGH", 2, 4, 5, 2),
                                Row.of(4, 10, 9, "FGH", 2, 4, 7, 2),
                                Row.of(4, 7, 6, "CDE", 2, 4, 5, 2),
                                Row.of(4, 7, 6, "CDE", 2, 4, 7, 2),
                                Row.of(4, 8, 7, "DEF", 1, 4, 5, 2),
                                Row.of(4, 8, 7, "DEF", 1, 4, 7, 2),
                                Row.of(4, 9, 8, "EFG", 1, 4, 5, 2),
                                Row.of(4, 9, 8, "EFG", 1, 4, 7, 2),
                                Row.of(5, 11, 10, "GHI", 1, 5, 9, 2),
                                Row.of(5, 12, 11, "HIJ", 3, 5, 9, 2),
                                Row.of(5, 13, 12, "IJK", 3, 5, 9, 2),
                                Row.of(5, 14, 13, "JKL", 2, 5, 9, 2),
                                Row.of(5, 15, 14, "KLM", 2, 5, 9, 2))),
                false);
    }

    @ParameterizedTest(name = "mode = {0}, ofcg = {1}")
    @MethodSource("parameters")
    public void testDynamicFilteringChainWithMultipleInput(
            BatchShuffleMode shuffleMode, boolean ofcg) throws Exception {
        configBatchShuffleMode(tEnv.getConfig(), shuffleMode);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, ofcg);
        String dataId1 = TestValuesTableFactory.registerData(TestData.data7());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE dim2 (\n"
                                + "  x INT,\n"
                                + "  y INT,\n"
                                + "  z BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId1));

        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "dim"),
                new CatalogTableStatistics(1, 1, 1, 1),
                false);
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "dim2"),
                new CatalogTableStatistics(100, 1, 1, 1),
                false);
        checkResult(
                "SELECT * FROM fact1, dim, dim2 WHERE dim.x = fact1.a and dim2.y = fact1.a AND dim.z = 1",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(1, 1, 0, "Hallo", 1, 1, 0, 1, 2, 1, 1),
                                Row.of(2, 2, 1, "Hallo Welt", 2, 2, 1, 1, 2, 2, 2),
                                Row.of(2, 3, 2, "Hallo Welt wie", 1, 2, 1, 1, 2, 2, 2),
                                Row.of(4, 10, 9, "FGH", 2, 4, 6, 1, 3, 4, 3),
                                Row.of(4, 7, 6, "CDE", 2, 4, 6, 1, 3, 4, 3),
                                Row.of(4, 8, 7, "DEF", 1, 4, 6, 1, 3, 4, 3),
                                Row.of(4, 9, 8, "EFG", 1, 4, 6, 1, 3, 4, 3),
                                Row.of(5, 11, 10, "GHI", 1, 5, 8, 1, 4, 5, 2),
                                Row.of(5, 12, 11, "HIJ", 3, 5, 8, 1, 4, 5, 2),
                                Row.of(5, 13, 12, "IJK", 3, 5, 8, 1, 4, 5, 2),
                                Row.of(5, 14, 13, "JKL", 2, 5, 8, 1, 4, 5, 2),
                                Row.of(5, 15, 14, "KLM", 2, 5, 8, 1, 4, 5, 2))),
                false);
    }

    @ParameterizedTest(name = "mode = {0}, ofcg = {1}")
    @MethodSource("parameters")
    public void testDynamicFilteringCannotChainWithMultipleInput(
            BatchShuffleMode shuffleMode, boolean ofcg) {
        configBatchShuffleMode(tEnv.getConfig(), shuffleMode);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, ofcg);
        checkResult(
                "SELECT * FROM fact1, dim, fact2 WHERE x = fact1.a and fact2.a = fact1.a AND z = 1 and fact1.e = 2 and fact2.e = 1",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(
                                        2,
                                        2,
                                        1,
                                        "Hallo Welt",
                                        2,
                                        2,
                                        1,
                                        1,
                                        2,
                                        3,
                                        2,
                                        "Hallo Welt wie",
                                        1),
                                Row.of(4, 10, 9, "FGH", 2, 4, 6, 1, 4, 8, 7, "DEF", 1),
                                Row.of(4, 10, 9, "FGH", 2, 4, 6, 1, 4, 9, 8, "EFG", 1),
                                Row.of(4, 7, 6, "CDE", 2, 4, 6, 1, 4, 8, 7, "DEF", 1),
                                Row.of(4, 7, 6, "CDE", 2, 4, 6, 1, 4, 9, 8, "EFG", 1),
                                Row.of(5, 14, 13, "JKL", 2, 5, 8, 1, 5, 11, 10, "GHI", 1),
                                Row.of(5, 15, 14, "KLM", 2, 5, 8, 1, 5, 11, 10, "GHI", 1))),
                false);
    }

    @ParameterizedTest(name = "mode = {0}, ofcg = {1}")
    @MethodSource("parameters")
    public void testReuseDimSide(BatchShuffleMode shuffleMode, boolean ofcg) {
        configBatchShuffleMode(tEnv.getConfig(), shuffleMode);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, ofcg);
        checkResult(
                "SELECT * FROM fact1, dim WHERE x = a AND z = 1 and b = 3"
                        + "UNION ALL "
                        + "SELECT * FROM fact2, dim WHERE x = a AND z = 1 and b = 2",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(2, 2, 1, "Hallo Welt", 2, 2, 1, 1),
                                Row.of(2, 3, 2, "Hallo Welt wie", 1, 2, 1, 1))),
                false);
    }

    @ParameterizedTest(name = "mode = {0}, ofcg = {1}")
    @MethodSource("parameters")
    public void testDynamicFilteringWithStaticPartitionPruning(
            BatchShuffleMode shuffleMode, boolean ofcg) {
        configBatchShuffleMode(tEnv.getConfig(), shuffleMode);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, ofcg);
        checkResult(
                "SELECT * FROM fact2, dim WHERE x = a and e = z AND y < 5 and a = 3",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(3, 4, 3, "Hallo Welt wie gehts?", 2, 3, 3, 2),
                                Row.of(3, 5, 4, "ABC", 2, 3, 3, 2),
                                Row.of(3, 6, 5, "BCD", 3, 3, 4, 3))),
                false);
    }

    @ParameterizedTest(name = "mode = {0}, ofcg = {1}")
    @MethodSource("parameters")
    public void testMultiplePartitionKeysWithFullKey(BatchShuffleMode shuffleMode, boolean ofcg) {
        configBatchShuffleMode(tEnv.getConfig(), shuffleMode);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, ofcg);
        checkResult(
                "SELECT * FROM fact2, dim WHERE x = a AND z = e and y = 1",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(Row.of(2, 3, 2, "Hallo Welt wie", 1, 2, 1, 1))),
                false);
    }

    @ParameterizedTest(name = "mode = {0}, ofcg = {1}")
    @MethodSource("parameters")
    public void testMultiplePartitionKeysWithPartialKey(
            BatchShuffleMode shuffleMode, boolean ofcg) {
        configBatchShuffleMode(tEnv.getConfig(), shuffleMode);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, ofcg);
        checkResult(
                "SELECT * FROM fact2, dim WHERE z = e and y = 1",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(1, 1, 0, "Hallo", 1, 2, 1, 1),
                                Row.of(2, 3, 2, "Hallo Welt wie", 1, 2, 1, 1),
                                Row.of(4, 8, 7, "DEF", 1, 2, 1, 1),
                                Row.of(4, 9, 8, "EFG", 1, 2, 1, 1),
                                Row.of(5, 11, 10, "GHI", 1, 2, 1, 1))),
                false);
    }
}
