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

package org.apache.flink.table.planner.runtime.batch.sql.join;

import org.apache.flink.connector.source.PartitionSerializer;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.connector.source.partitioning.KeyGroupedPartitioning;
import org.apache.flink.table.expressions.TransformExpression;
import org.apache.flink.types.Row;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for storage partition join using sort merge join. */

/** TODO right now we test query plan only, need to add result verification. */
public class TestStoragePartitionJoin extends BatchTestBase {
    private TableEnvironment tEnv;

    // Common test data constants
    private static final String[] TABLE1_COLUMNS = {"id", "name", "salary"};
    private static final String[] TABLE2_COLUMNS = {"id", "department", "location"};
    private static final String JOIN_SQL =
            "SELECT t1.id, t1.name, t1.salary, t2.department, t2.location " +
                    "FROM %s t1 INNER JOIN %s t2 ON t1.id = t2.id";

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        tEnv = tEnv();

        // Disable other join operators to force sort merge join usage
        tEnv
                .getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "HashJoin, NestedLoopJoin");

        // reset storage partition join config to false before each test
        tEnv
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_STORAGE_PARTITION_JOIN_ENABLED, false);

        // Create test tables with sample data
        setupTestTables();
    }

    /**
     * Test that a basic sort merge join will add hash exchanges for both sides of the join.
     */
    @Test
    public void testBasicSortMergeJoinWithEqualCondition() {
        String sql = String.format(JOIN_SQL, "table1", "table2");
        verifySortMergeJoinPlan(
                sql,
                "Exchange(distribution=[hash[id]])",
                2,
                "Basic Sort Merge Join");
    }

    /**
     * Test that storage partition join is disabled by default
     * and does not add any exchanges for partitioned tables.
     */
    @Test
    public void testStoragePartitionJoinDisabledByDefault() {
        createStandardPartitionedTables("partitioned_table1", "partitioned_table2");

        String sql = String.format(JOIN_SQL, "partitioned_table1", "partitioned_table2");
        verifySortMergeJoinPlan(
                sql,
                "Exchange(distribution=[hash[id]])",
                2,
                "Storage Partition Join Disabled");
    }

    @Test
    public void testStoragePartitionJoin() {
        tEnv
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_STORAGE_PARTITION_JOIN_ENABLED, true);

        createStandardPartitionedTables("partitioned_table1", "partitioned_table2");

        String sql = String.format(JOIN_SQL, "partitioned_table1", "partitioned_table2");
        verifySortMergeJoinPlan(
                sql,
                "Exchange(distribution=[keep_input_as_is[hash[id]]])",
                2,
                "Storage Partition Join Enabled");
    }

    /**
     * Common utility to create a partitioned table with the given configuration.
     */
    private void createPartitionedTable(
            String tableName,
            String[] columns,
            KeyGroupedPartitioning partitioning) {
        // Build column definitions - all columns are VARCHAR
        StringBuilder columnDefs = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                columnDefs.append(",\n");
            }
            columnDefs.append("  ").append(columns[i]).append(" VARCHAR");
        }

        // Serialize the partitioning using PartitionSerializer
        String partitionString;
        try {
            partitionString = PartitionSerializer.serialize(partitioning);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize partitioning", e);
        }

        // Create the table SQL with partitioning property
        String createTableSql = "CREATE TABLE " + tableName + " (\n"
                + columnDefs.toString() + "\n"
                + ") WITH (\n"
                + "  'connector' = 'values',\n"
                + "  'bounded' = 'true',\n"
                + "  'source.partitioning' = '" + partitionString + "'\n"
                + ")";
        // Execute the SQL statement
        tEnv.executeSql(createTableSql);
    }

    /**
     * Common utility to create standard partitioned tables for testing.
     */
    private void createStandardPartitionedTables(String table1Name, String table2Name) {
        // Create partitioned table1 with columns: id, name, salary
        TransformExpression[] table1Keys = {new TransformExpression("id", null, null)};
        Row[] table1PartitionValues = {Row.of("1"), Row.of("2")};
        KeyGroupedPartitioning table1Partitioning = new KeyGroupedPartitioning(
                table1Keys,
                table1PartitionValues,
                2);
        createPartitionedTable(table1Name, TABLE1_COLUMNS, table1Partitioning);

        // Create partitioned table2 with columns: id, department, location
        TransformExpression[] table2Keys = {new TransformExpression("id", null, null)};
        Row[] table2PartitionValues = {Row.of("1"), Row.of("2")};
        KeyGroupedPartitioning table2Partitioning = new KeyGroupedPartitioning(
                table2Keys,
                table2PartitionValues,
                2);
        createPartitionedTable(table2Name, TABLE2_COLUMNS, table2Partitioning);
    }

    /**
     * Common utility to execute SQL and extract optimized execution plan.
     */
    private String getOptimizedExecutionPlan(String sql) {
        String explainResult = tEnv.explainSql(sql);

        // Extract only the Optimized Execution Plan section
        String[] sections = explainResult.split("== Optimized Execution Plan ==");
        return sections.length > 1 ? sections[1].trim() : explainResult;
    }

    /**
     * Common utility to verify exchange patterns in execution plan.
     */
    private void verifyExchangePattern(
            String optimizedExecutionPlan,
            String exchangePattern,
            int expectedCount) {
        long exchangeCount = Arrays.stream(optimizedExecutionPlan.split("\n"))
                .filter(line -> line.contains(exchangePattern))
                .count();
        assertThat(exchangeCount).isEqualTo(expectedCount);
    }

    /**
     * Common utility to verify sort merge join execution plan.
     */
    private void verifySortMergeJoinPlan(
            String sql,
            String exchangePattern,
            int expectedExchangeCount,
            String testDescription) {
        String optimizedExecutionPlan = getOptimizedExecutionPlan(sql);
        System.out.println(
                testDescription + " - Optimized Execution Plan: " + optimizedExecutionPlan);

        assertThat(optimizedExecutionPlan).contains("SortMergeJoin");
        verifyExchangePattern(optimizedExecutionPlan, exchangePattern, expectedExchangeCount);
    }

    private void setupTestTables() {
        // Create table1
        tEnv.executeSql(
                "CREATE TABLE table1 (\n"
                        + "  id INT,\n"
                        + "  name VARCHAR,\n"
                        + "  salary INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'true'\n"
                        + ")");

        // Create table2
        tEnv.executeSql(
                "CREATE TABLE table2 (\n"
                        + "  id INT,\n"
                        + "  department VARCHAR,\n"
                        + "  location VARCHAR\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'true'\n"
                        + ")");
    }
}
