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

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for APPROX_COUNT_DISTINCT aggregate function in batch mode.
 *
 * <p>This test verifies backward compatibility after unifying the batch and streaming
 * implementations of APPROX_COUNT_DISTINCT.
 */
class BatchApproxCountDistinctITCase extends BatchTestBase {

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
    }

    @Test
    void testApproxCountDistinctWithString() throws Exception {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, "user_a"),
                        Row.of(2, "user_a"), // duplicate
                        Row.of(3, "user_b"),
                        Row.of(4, "user_c"),
                        Row.of(5, "user_c"), // duplicate
                        Row.of(6, "user_d"));

        String dataId = TestValuesTableFactory.registerData(testData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE test_source (\n"
                                        + "  id INT,\n"
                                        + "  user_id STRING\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql = "SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_uv FROM test_source";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        // user_a, user_b, user_c, user_d -> 4 distinct values
        Long approxUv = (Long) results.get(0).getField("approx_uv");
        assertThat(approxUv).isEqualTo(4L);
    }

    @Test
    void testApproxCountDistinctWithInteger() throws Exception {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, 100),
                        Row.of(2, 100), // duplicate
                        Row.of(3, 200),
                        Row.of(4, 300),
                        Row.of(5, 200), // duplicate
                        Row.of(6, 400));

        String dataId = TestValuesTableFactory.registerData(testData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE int_source (\n"
                                        + "  id INT,\n"
                                        + "  value_int INT\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql = "SELECT APPROX_COUNT_DISTINCT(value_int) AS approx_distinct FROM int_source";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        // 100, 200, 300, 400 -> 4 distinct values
        Long approxDistinct = (Long) results.get(0).getField("approx_distinct");
        assertThat(approxDistinct).isEqualTo(4L);
    }

    @Test
    void testApproxCountDistinctWithLong() throws Exception {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, 1000000000L),
                        Row.of(2, 2000000000L),
                        Row.of(3, 1000000000L), // duplicate
                        Row.of(4, 3000000000L));

        String dataId = TestValuesTableFactory.registerData(testData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE long_source (\n"
                                        + "  id INT,\n"
                                        + "  value_long BIGINT\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql = "SELECT APPROX_COUNT_DISTINCT(value_long) AS approx_distinct FROM long_source";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        Long approxDistinct = (Long) results.get(0).getField("approx_distinct");
        assertThat(approxDistinct).isEqualTo(3L);
    }

    @Test
    void testApproxCountDistinctWithDouble() throws Exception {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, 1.1d),
                        Row.of(2, 2.2d),
                        Row.of(3, 1.1d), // duplicate
                        Row.of(4, 3.3d),
                        Row.of(5, 0.0d),
                        Row.of(6, -0.0d)); // 0.0 and -0.0 should be treated as the same

        String dataId = TestValuesTableFactory.registerData(testData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE double_source (\n"
                                        + "  id INT,\n"
                                        + "  value_double DOUBLE\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql =
                "SELECT APPROX_COUNT_DISTINCT(value_double) AS approx_distinct FROM double_source";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        // 1.1, 2.2, 3.3, 0.0 -> 4 distinct values (0.0 and -0.0 are same)
        Long approxDistinct = (Long) results.get(0).getField("approx_distinct");
        assertThat(approxDistinct).isEqualTo(4L);
    }

    @Test
    void testApproxCountDistinctWithDecimal() throws Exception {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, new BigDecimal("123.45")),
                        Row.of(2, new BigDecimal("234.56")),
                        Row.of(3, new BigDecimal("123.45")), // duplicate
                        Row.of(4, new BigDecimal("345.67")));

        String dataId = TestValuesTableFactory.registerData(testData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE decimal_source (\n"
                                        + "  id INT,\n"
                                        + "  value_decimal DECIMAL(10, 2)\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql =
                "SELECT APPROX_COUNT_DISTINCT(value_decimal) AS approx_distinct FROM decimal_source";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        Long approxDistinct = (Long) results.get(0).getField("approx_distinct");
        assertThat(approxDistinct).isEqualTo(3L);
    }

    @Test
    void testApproxCountDistinctWithDate() throws Exception {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, LocalDate.of(2024, 1, 1)),
                        Row.of(2, LocalDate.of(2024, 1, 2)),
                        Row.of(3, LocalDate.of(2024, 1, 1)), // duplicate
                        Row.of(4, LocalDate.of(2024, 1, 3)));

        String dataId = TestValuesTableFactory.registerData(testData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE date_source (\n"
                                        + "  id INT,\n"
                                        + "  value_date DATE\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql = "SELECT APPROX_COUNT_DISTINCT(value_date) AS approx_distinct FROM date_source";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        Long approxDistinct = (Long) results.get(0).getField("approx_distinct");
        assertThat(approxDistinct).isEqualTo(3L);
    }

    @Test
    void testApproxCountDistinctWithTime() throws Exception {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, LocalTime.of(10, 0, 0)),
                        Row.of(2, LocalTime.of(11, 0, 0)),
                        Row.of(3, LocalTime.of(10, 0, 0)), // duplicate
                        Row.of(4, LocalTime.of(12, 0, 0)));

        String dataId = TestValuesTableFactory.registerData(testData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE time_source (\n"
                                        + "  id INT,\n"
                                        + "  value_time TIME\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql = "SELECT APPROX_COUNT_DISTINCT(value_time) AS approx_distinct FROM time_source";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        Long approxDistinct = (Long) results.get(0).getField("approx_distinct");
        assertThat(approxDistinct).isEqualTo(3L);
    }

    @Test
    void testApproxCountDistinctWithTimestamp() throws Exception {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, LocalDateTime.of(2024, 1, 1, 10, 0, 0)),
                        Row.of(2, LocalDateTime.of(2024, 1, 1, 11, 0, 0)),
                        Row.of(3, LocalDateTime.of(2024, 1, 1, 10, 0, 0)), // duplicate
                        Row.of(4, LocalDateTime.of(2024, 1, 1, 12, 0, 0)));

        String dataId = TestValuesTableFactory.registerData(testData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE timestamp_source (\n"
                                        + "  id INT,\n"
                                        + "  value_ts TIMESTAMP(3)\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql =
                "SELECT APPROX_COUNT_DISTINCT(value_ts) AS approx_distinct FROM timestamp_source";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        Long approxDistinct = (Long) results.get(0).getField("approx_distinct");
        assertThat(approxDistinct).isEqualTo(3L);
    }

    @Test
    void testApproxCountDistinctWithNullValues() throws Exception {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, "user_a"),
                        Row.of(2, null), // NULL should be ignored
                        Row.of(3, "user_b"),
                        Row.of(4, null), // NULL should be ignored
                        Row.of(5, "user_a")); // duplicate

        String dataId = TestValuesTableFactory.registerData(testData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE null_source (\n"
                                        + "  id INT,\n"
                                        + "  user_id STRING\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql = "SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_uv FROM null_source";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        // user_a, user_b (NULLs are ignored) -> 2 distinct values
        Long approxUv = (Long) results.get(0).getField("approx_uv");
        assertThat(approxUv).isEqualTo(2L);
    }

    @Test
    void testApproxCountDistinctWithGroupBy() throws Exception {
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, "category_a", "user_1"),
                        Row.of(2, "category_a", "user_2"),
                        Row.of(3, "category_a", "user_1"), // duplicate in category_a
                        Row.of(4, "category_b", "user_3"),
                        Row.of(5, "category_b", "user_4"),
                        Row.of(6, "category_b", "user_5"));

        String dataId = TestValuesTableFactory.registerData(testData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE group_source (\n"
                                        + "  id INT,\n"
                                        + "  category STRING,\n"
                                        + "  user_id STRING\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql =
                "SELECT category, APPROX_COUNT_DISTINCT(user_id) AS approx_uv "
                        + "FROM group_source GROUP BY category ORDER BY category";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(2);
        // category_a: user_1, user_2 -> 2 distinct
        assertThat(results.get(0).getField("category")).isEqualTo("category_a");
        assertThat(results.get(0).getField("approx_uv")).isEqualTo(2L);
        // category_b: user_3, user_4, user_5 -> 3 distinct
        assertThat(results.get(1).getField("category")).isEqualTo("category_b");
        assertThat(results.get(1).getField("approx_uv")).isEqualTo(3L);
    }

    @Test
    void testApproxCountDistinctPrecision() throws Exception {
        // Test precision with larger dataset
        List<Row> largeData = new ArrayList<>();
        int distinctCount = 10000;

        for (int i = 0; i < distinctCount; i++) {
            largeData.add(Row.of(i, "user_" + i));
        }

        String dataId = TestValuesTableFactory.registerData(largeData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE large_source (\n"
                                        + "  id INT,\n"
                                        + "  user_id STRING\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql = "SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_uv FROM large_source";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        Long approxUv = (Long) results.get(0).getField("approx_uv");

        // HLL++ relative standard error is approximately 1%, allow 2% tolerance
        double error = Math.abs(approxUv - distinctCount) / (double) distinctCount;
        assertThat(error).isLessThan(0.02);
    }

    @Test
    void testApproxCountDistinctWithEmptyTable() throws Exception {
        List<Row> emptyData = new ArrayList<>();

        String dataId = TestValuesTableFactory.registerData(emptyData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE empty_source (\n"
                                        + "  id INT,\n"
                                        + "  user_id STRING\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));

        String sql = "SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_uv FROM empty_source";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        Long approxUv = (Long) results.get(0).getField("approx_uv");
        assertThat(approxUv).isEqualTo(0L);
    }
}
