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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.planner.utils.TestingTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Base test class for ITCases of flink-table module in batch mode. This class is an upgraded
 * version of {@link BatchTestBase}, which migrates Junit4 to replace junit5. We recommend using
 * this class instead of {@link BatchTestBase} for the batch ITCases of flink-table module in the
 * future.
 *
 * <p>The class will be renamed to BatchTestBase after all batch ITCase are migrated to junit5.
 */
public class BatchTestBaseV2 extends AbstractTestBaseV2 {

    protected TableEnvironment tEnv;
    protected StreamExecutionEnvironment env;

    @Override
    protected TableEnvironment getTableEnvironment() {
        tEnv =
                TestingTableEnvironment.create(
                        EnvironmentSettings.newInstance().inBatchMode().build(),
                        null,
                        TableConfig.getDefault());
        tEnv.getConfig().set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, false);
        configForMiniCluster(tEnv.getConfig());
        Planner planner = ((TableEnvironmentImpl) tEnv).getPlanner();
        env = ((PlannerBase) planner).getExecEnv();
        return tEnv;
    }

    @AfterEach
    public void after() {
        TestValuesTableFactory.clearAllData();
    }

    protected void checkResult(String query, List<Row> expectedResult, boolean isSorted) {
        Table table = tEnv.sqlQuery(query);
        List<Row> result = executeQuery(table);
        Optional<String> checkResult = checkSame(expectedResult, result, isSorted);
        if (checkResult.isPresent()) {
            String plan = explainLogical(table);
            Assertions.fail(
                    String.format(
                            "Results do not match for query:\n  %s\n %s\n Plan:\n  %s",
                            query, checkResult, plan));
        }
    }

    private String explainLogical(Table table) {
        RelNode ast = TableTestUtil.toRelNode(table);
        String logicalPlan = getPlan(ast);
        return String.format(
                "== Abstract Syntax Tree ==%s%s%s== Optimized Logical Plan ==%s%s",
                System.lineSeparator(),
                FlinkRelOptUtil.toString(
                        ast, SqlExplainLevel.DIGEST_ATTRIBUTES, false, false, false, false, false),
                System.lineSeparator(),
                System.lineSeparator(),
                logicalPlan);
    }

    private String getPlan(RelNode relNode) {
        Planner planner = ((TableEnvironmentImpl) tEnv).getPlanner();
        RelNode optimized = ((PlannerBase) planner).optimize(relNode);
        return FlinkRelOptUtil.toString(
                optimized, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false, false, false, false, false);
    }

    private Optional<String> checkSame(
            List<Row> expectedResult, List<Row> result, boolean isSorted) {
        if (expectedResult.size() != result.size()
                || !prepareResult(expectedResult, isSorted)
                        .equals(prepareResult(result, isSorted))) {
            List<String> expectedString = prepareResult(expectedResult, isSorted);
            expectedString.add(
                    0, String.format("== Correct Result - %s ==", expectedResult.size()));
            List<String> resultString = prepareResult(result, isSorted);
            resultString.add(0, String.format("== Actual Result - %s ==", expectedResult.size()));
            String outString = sideBySide(expectedString, resultString);
            return Optional.of(outString);
        } else {
            return Optional.empty();
        }
    }

    private String sideBySide(List<String> left, List<String> right) {
        int maxLeftSize = left.stream().map(String::length).max(Comparator.naturalOrder()).get();
        for (int i = 0; i < Math.max(right.size() - left.size(), 0); i++) {
            left.add("");
        }
        for (int i = 0; i < Math.max(left.size() - right.size(), 0); i++) {
            right.add("");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < left.size(); i++) {
            String leftStr = left.get(i);
            String rightStr = right.get(i);
            int leftWhiteSpaceSize = (maxLeftSize - leftStr.length()) + 3;
            if (leftStr.equals(rightStr) || leftStr.startsWith("== Correct")) {
                sb.append(" ");
            } else {
                sb.append("!");
            }
            sb.append(leftStr);
            for (int j = 0; j < leftWhiteSpaceSize; j++) {
                sb.append(" ");
            }
            sb.append(rightStr);

            if (i < left.size() - 1) {
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    private List<String> prepareResult(List<Row> rows, boolean isSorted) {
        if (!isSorted) {
            return rows.stream().map(Row::toString).sorted().collect(Collectors.toList());
        } else {
            return rows.stream().map(Row::toString).collect(Collectors.toList());
        }
    }

    private List<Row> executeQuery(Table table) {
        return CollectionUtil.iteratorToList(table.execute().collect());
    }

    private static void configForMiniCluster(TableConfig tableConfig) {
        tableConfig.set(
                ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
                DEFAULT_PARALLELISM);
    }
}
