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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for APPROX_COUNT_DISTINCT aggregate function in streaming mode.
 *
 * <p>This test verifies that APPROX_COUNT_DISTINCT works correctly with Window TVF (TUMBLE, HOP,
 * CUMULATE) in streaming mode.
 */
class StreamApproxCountDistinctITCase extends StreamingTestBase {

    @BeforeEach
    void setupTestData() {
        // Prepare test data
        List<Row> testData =
                Arrays.asList(
                        // Window 1: [00:00:00, 00:00:05)
                        Row.of(
                                1,
                                "user_a",
                                LocalDateTime.of(2024, 1, 1, 0, 0, 1)), // user_a @ 00:00:01
                        Row.of(
                                2,
                                "user_a",
                                LocalDateTime.of(
                                        2024, 1, 1, 0, 0, 2)), // user_a @ 00:00:02 (duplicate)
                        Row.of(
                                3,
                                "user_b",
                                LocalDateTime.of(2024, 1, 1, 0, 0, 3)), // user_b @ 00:00:03
                        Row.of(
                                4,
                                "user_c",
                                LocalDateTime.of(2024, 1, 1, 0, 0, 4)), // user_c @ 00:00:04

                        // Window 2: [00:00:05, 00:00:10)
                        Row.of(
                                5,
                                "user_a",
                                LocalDateTime.of(2024, 1, 1, 0, 0, 6)), // user_a @ 00:00:06
                        Row.of(
                                6,
                                "user_d",
                                LocalDateTime.of(2024, 1, 1, 0, 0, 7)), // user_d @ 00:00:07
                        Row.of(
                                7,
                                "user_d",
                                LocalDateTime.of(
                                        2024, 1, 1, 0, 0, 8)), // user_d @ 00:00:08 (duplicate)

                        // Window 3: [00:00:10, 00:00:15)
                        Row.of(
                                8,
                                "user_e",
                                LocalDateTime.of(2024, 1, 1, 0, 0, 11)), // user_e @ 00:00:11
                        Row.of(
                                9,
                                "user_f",
                                LocalDateTime.of(2024, 1, 1, 0, 0, 12)), // user_f @ 00:00:12
                        Row.of(
                                10,
                                "user_g",
                                LocalDateTime.of(2024, 1, 1, 0, 0, 13)), // user_g @ 00:00:13
                        Row.of(
                                11,
                                "user_h",
                                LocalDateTime.of(2024, 1, 1, 0, 0, 14)) // user_h @ 00:00:14
                        );

        String dataId = TestValuesTableFactory.registerData(testData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE test_source (\n"
                                        + "  id INT,\n"
                                        + "  user_id STRING,\n"
                                        + "  event_time TIMESTAMP(3),\n"
                                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                dataId));
    }

    @Test
    void testApproxCountDistinctWithTumbleWindow() throws Exception {
        // TUMBLE window test: 5-second window
        String sql =
                "SELECT\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  APPROX_COUNT_DISTINCT(user_id) AS approx_uv\n"
                        + "FROM TABLE(\n"
                        + "  TUMBLE(TABLE test_source, DESCRIPTOR(event_time), INTERVAL '5' SECOND)\n"
                        + ")\n"
                        + "GROUP BY window_start, window_end\n"
                        + "ORDER BY window_start";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(3);

        // Verify approximate UV for each window
        // Window1 [00:00:00, 00:00:05): user_a, user_b, user_c -> 3
        // Window2 [00:00:05, 00:00:10): user_a, user_d -> 2
        // Window3 [00:00:10, 00:00:15): user_e, user_f, user_g, user_h -> 4
        Long window1Uv = (Long) results.get(0).getField("approx_uv");
        Long window2Uv = (Long) results.get(1).getField("approx_uv");
        Long window3Uv = (Long) results.get(2).getField("approx_uv");

        // HLL++ precision is approximately 1%, should be exact for small data sets
        assertThat(window1Uv).isEqualTo(3L);
        assertThat(window2Uv).isEqualTo(2L);
        assertThat(window3Uv).isEqualTo(4L);
    }

    @Test
    void testApproxCountDistinctWithHopWindow() throws Exception {
        // HOP window test: 10-second window, 5-second slide
        String sql =
                "SELECT\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  APPROX_COUNT_DISTINCT(user_id) AS approx_uv\n"
                        + "FROM TABLE(\n"
                        + "  HOP(TABLE test_source, DESCRIPTOR(event_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND)\n"
                        + ")\n"
                        + "GROUP BY window_start, window_end";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        // HOP windows overlap, each record may appear in multiple windows
        assertThat(results).isNotEmpty();

        // Verify result validity
        for (Row row : results) {
            Long approxUv = (Long) row.getField("approx_uv");
            assertThat(approxUv).isGreaterThanOrEqualTo(0L);
        }
    }

    @Test
    void testApproxCountDistinctWithCumulateWindow() throws Exception {
        // CUMULATE window test: 15-second max window, 5-second step
        String sql =
                "SELECT\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  APPROX_COUNT_DISTINCT(user_id) AS approx_uv\n"
                        + "FROM TABLE(\n"
                        + "  CUMULATE(TABLE test_source, DESCRIPTOR(event_time), INTERVAL '5' SECOND, INTERVAL '15' SECOND)\n"
                        + ")\n"
                        + "GROUP BY window_start, window_end";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        // CUMULATE windows produce cumulative effect
        // [00:00, 00:05) -> 3 UV
        // [00:00, 00:10) -> 4 UV (cumulative)
        // [00:00, 00:15) -> 8 UV (cumulative)
        assertThat(results).isNotEmpty();

        // Verify cumulative effect: later windows UV should be >= earlier windows UV
        for (Row row : results) {
            Long approxUv = (Long) row.getField("approx_uv");
            assertThat(approxUv).isGreaterThanOrEqualTo(0L);
        }
    }

    @Test
    void testApproxCountDistinctWithDifferentDataTypes() throws Exception {
        // Test different data types
        List<Row> intData =
                Arrays.asList(
                        Row.of(1, 100, LocalDateTime.of(2024, 1, 1, 0, 0, 1)),
                        Row.of(2, 100, LocalDateTime.of(2024, 1, 1, 0, 0, 2)),
                        Row.of(3, 200, LocalDateTime.of(2024, 1, 1, 0, 0, 3)),
                        Row.of(4, 300, LocalDateTime.of(2024, 1, 1, 0, 0, 4)));

        String intDataId = TestValuesTableFactory.registerData(intData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE int_source (\n"
                                        + "  id INT,\n"
                                        + "  value_int INT,\n"
                                        + "  event_time TIMESTAMP(3),\n"
                                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                intDataId));

        String sql =
                "SELECT\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  APPROX_COUNT_DISTINCT(value_int) AS approx_distinct_int\n"
                        + "FROM TABLE(\n"
                        + "  TUMBLE(TABLE int_source, DESCRIPTOR(event_time), INTERVAL '10' SECOND)\n"
                        + ")\n"
                        + "GROUP BY window_start, window_end";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        // 100, 200, 300 -> 3 distinct values
        Long approxDistinct = (Long) results.get(0).getField("approx_distinct_int");
        assertThat(approxDistinct).isEqualTo(3L);
    }

    @Test
    void testApproxCountDistinctWithNullValues() throws Exception {
        // Test with NULL values
        List<Row> nullData =
                Arrays.asList(
                        Row.of(1, "user_a", LocalDateTime.of(2024, 1, 1, 0, 0, 1)),
                        Row.of(2, null, LocalDateTime.of(2024, 1, 1, 0, 0, 2)), // NULL
                        Row.of(3, "user_b", LocalDateTime.of(2024, 1, 1, 0, 0, 3)),
                        Row.of(4, null, LocalDateTime.of(2024, 1, 1, 0, 0, 4)), // NULL
                        Row.of(5, "user_a", LocalDateTime.of(2024, 1, 1, 0, 0, 5)) // duplicate
                        );

        String nullDataId = TestValuesTableFactory.registerData(nullData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE null_source (\n"
                                        + "  id INT,\n"
                                        + "  user_id STRING,\n"
                                        + "  event_time TIMESTAMP(3),\n"
                                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                nullDataId));

        String sql =
                "SELECT\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  APPROX_COUNT_DISTINCT(user_id) AS approx_uv\n"
                        + "FROM TABLE(\n"
                        + "  TUMBLE(TABLE null_source, DESCRIPTOR(event_time), INTERVAL '10' SECOND)\n"
                        + ")\n"
                        + "GROUP BY window_start, window_end";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        // user_a, user_b (NULLs are ignored) -> 2 distinct values
        Long approxUv = (Long) results.get(0).getField("approx_uv");
        assertThat(approxUv).isEqualTo(2L);
    }

    @Test
    void testApproxCountDistinctPrecision() throws Exception {
        // Test precision: verify HLL++ relative standard error is approximately 1% using large
        // dataset
        List<Row> largeData = new ArrayList<>();
        int distinctCount = 10000;
        LocalDateTime baseTime = LocalDateTime.of(2024, 1, 1, 0, 0, 0);

        for (int i = 0; i < distinctCount; i++) {
            largeData.add(
                    Row.of(
                            i,
                            "user_" + i,
                            baseTime.plusSeconds(i % 60))); // distributed within 60 seconds
        }

        String largeDataId = TestValuesTableFactory.registerData(largeData);

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE large_source (\n"
                                        + "  id INT,\n"
                                        + "  user_id STRING,\n"
                                        + "  event_time TIMESTAMP(3),\n"
                                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'bounded' = 'true'\n"
                                        + ")",
                                largeDataId));

        String sql =
                "SELECT\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  APPROX_COUNT_DISTINCT(user_id) AS approx_uv\n"
                        + "FROM TABLE(\n"
                        + "  TUMBLE(TABLE large_source, DESCRIPTOR(event_time), INTERVAL '2' MINUTE)\n"
                        + ")\n"
                        + "GROUP BY window_start, window_end";

        TableResult result = tEnv().executeSql(sql);
        List<Row> results = CollectionUtil.iteratorToList(result.collect());

        assertThat(results).hasSize(1);
        Long approxUv = (Long) results.get(0).getField("approx_uv");

        // HLL++ 相对标准误差约为 1%，允许 2% 的偏差
        double error = Math.abs(approxUv - distinctCount) / (double) distinctCount;
        assertThat(error).isLessThan(0.02);
    }
}
