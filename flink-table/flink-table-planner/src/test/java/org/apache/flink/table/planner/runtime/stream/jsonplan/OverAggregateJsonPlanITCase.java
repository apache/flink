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
 *
 */

package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Test json deserialization for over aggregate. */
class OverAggregateJsonPlanITCase extends JsonPlanTestBase {

    @Test
    void testProcTimeBoundedPartitionedRowsOver()
            throws ExecutionException, InterruptedException, IOException {
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.data5()),
                "a int",
                "b bigint",
                "c int",
                "d string",
                "e bigint",
                "proctime as PROCTIME()");
        createTestNonInsertOnlyValuesSinkTable("MySink", "a bigint", "b bigint", "c bigint");
        String sql =
                "insert into MySink SELECT a, "
                        + "  SUM(c) OVER ("
                        + "    PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), "
                        + "  MIN(c) OVER ("
                        + "    PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) "
                        + "FROM MyTable";
        compileSqlAndExecutePlan(sql).await();

        List<String> expected =
                Arrays.asList(
                        "+I[1, 0, 0]",
                        "+I[2, 1, 1]",
                        "+I[2, 3, 1]",
                        "+I[3, 3, 3]",
                        "+I[3, 7, 3]",
                        "+I[3, 12, 3]",
                        "+I[4, 6, 6]",
                        "+I[4, 13, 6]",
                        "+I[4, 21, 6]",
                        "+I[4, 30, 6]",
                        "+I[5, 10, 10]",
                        "+I[5, 21, 10]",
                        "+I[5, 33, 10]",
                        "+I[5, 46, 10]",
                        "+I[5, 60, 10]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testProcTimeUnboundedNonPartitionedRangeOver()
            throws IOException, ExecutionException, InterruptedException {
        List<Row> data =
                Arrays.asList(
                        Row.of(1L, 1, "Hello"),
                        Row.of(2L, 2, "Hello"),
                        Row.of(3L, 3, "Hello"),
                        Row.of(4L, 4, "Hello"),
                        Row.of(5L, 5, "Hello"),
                        Row.of(6L, 6, "Hello"),
                        Row.of(7L, 7, "Hello World"),
                        Row.of(8L, 8, "Hello World"),
                        Row.of(20L, 20, "Hello World"));
        createTestValuesSourceTable(
                "MyTable", data, "a bigint", "b int", "c string", "proctime as PROCTIME()");
        createTestNonInsertOnlyValuesSinkTable("MySink", "a string", "b int", "c string");

        String sql =
                "insert into MySink SELECT c, sum1, maxnull\n"
                        + "FROM (\n"
                        + " SELECT c,\n"
                        + "  max(cast(null as varchar)) OVER\n"
                        + "   (PARTITION BY c ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)\n"
                        + "   as maxnull,\n"
                        + "  sum(1) OVER\n"
                        + "   (PARTITION BY c ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)\n"
                        + "   as sum1\n"
                        + " FROM MyTable\n"
                        + ")";
        compileSqlAndExecutePlan(sql).await();
        List<String> expected =
                Arrays.asList(
                        "+I[Hello World, 1, null]",
                        "+I[Hello World, 2, null]",
                        "+I[Hello World, 3, null]",
                        "+I[Hello, 1, null]",
                        "+I[Hello, 2, null]",
                        "+I[Hello, 3, null]",
                        "+I[Hello, 4, null]",
                        "+I[Hello, 5, null]",
                        "+I[Hello, 6, null]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testRowTimeBoundedPartitionedRangeOver()
            throws IOException, ExecutionException, InterruptedException {
        List<Row> data =
                Arrays.asList(
                        Row.of(10L, 1L, 1, "Hello"),
                        Row.of(15L, 1L, 15, "Hello"),
                        Row.of(16L, 1L, 16, "Hello"),
                        Row.of(20L, 2L, 2, "Hello"),
                        Row.of(20L, 2L, 2, "Hello"),
                        Row.of(20L, 2L, 3, "Hello"),
                        Row.of(30L, 3L, 3, "Hello"),
                        Row.of(40L, 4L, 4, "Hello"),
                        Row.of(50L, 5L, 5, "Hello"),
                        Row.of(60L, 6L, 6, "Hello"),
                        Row.of(65L, 6L, 65, "Hello"),
                        Row.of(90L, 6L, 9, "Hello"),
                        Row.of(95L, 6L, 18, "Hello"), // out of order
                        Row.of(90L, 6L, 9, "Hello"),
                        Row.of(100L, 7L, 7, "Hello World"),
                        Row.of(110L, 7L, 17, "Hello World"),
                        Row.of(110L, 7L, 77, "Hello World"),
                        Row.of(140L, 7L, 18, "Hello World"),
                        Row.of(150L, 8L, 8, "Hello World"),
                        Row.of(200L, 20L, 20, "Hello World"));
        createTestValuesSourceTable(
                "MyTable",
                data,
                "ts bigint",
                "a bigint",
                "b int",
                "c string",
                "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts))",
                "watermark for rowtime as rowtime - INTERVAL '10' second");

        tableEnv.createTemporaryFunction(
                "LTCNT", new JavaUserDefinedAggFunctions.LargerThanCount());

        String sql =
                "insert into MySink SELECT "
                        + "  c, b,"
                        + "  LTCNT(a, CAST('4' AS BIGINT)) OVER (PARTITION BY c ORDER BY rowtime RANGE "
                        + "    BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW), "
                        + "  COUNT(a) OVER (PARTITION BY c ORDER BY rowtime RANGE "
                        + "    BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW), "
                        + "  SUM(a) OVER (PARTITION BY c ORDER BY rowtime RANGE "
                        + "    BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW)"
                        + " FROM MyTable";
        createTestNonInsertOnlyValuesSinkTable(
                "MySink", "a string", "b int", "c bigint", "d bigint", "e bigint");

        compileSqlAndExecutePlan(sql).await();
        List<String> expected =
                Arrays.asList(
                        "+I[Hello, 1, 0, 1, 1]",
                        "+I[Hello, 15, 0, 2, 2]",
                        "+I[Hello, 16, 0, 3, 3]",
                        "+I[Hello, 2, 0, 6, 9]",
                        "+I[Hello, 3, 0, 6, 9]",
                        "+I[Hello, 2, 0, 6, 9]",
                        "+I[Hello, 3, 0, 4, 9]",
                        "+I[Hello, 4, 0, 2, 7]",
                        "+I[Hello, 5, 1, 2, 9]",
                        "+I[Hello, 6, 2, 2, 11]",
                        "+I[Hello, 65, 2, 2, 12]",
                        "+I[Hello, 9, 2, 2, 12]",
                        "+I[Hello, 9, 2, 2, 12]",
                        "+I[Hello, 18, 3, 3, 18]",
                        "+I[Hello World, 17, 3, 3, 21]",
                        "+I[Hello World, 7, 1, 1, 7]",
                        "+I[Hello World, 77, 3, 3, 21]",
                        "+I[Hello World, 18, 1, 1, 7]",
                        "+I[Hello World, 8, 2, 2, 15]",
                        "+I[Hello World, 20, 1, 1, 20]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }
}
