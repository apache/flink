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

package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/** Test for window aggregate json plan. */
public class WindowAggregateJsonITCase extends JsonPlanTestBase {

    @Before
    public void setup() throws Exception {
        super.setup();
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.windowData()),
                new String[] {
                    "ts STRING",
                    "`int` INT",
                    "`double` DOUBLE",
                    "`float` FLOAT",
                    "`bigdec` DECIMAL(10, 2)",
                    "`string` STRING",
                    "`name` STRING",
                    "`rowtime` AS TO_TIMESTAMP(`ts`)",
                    "WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND",
                },
                new HashMap<String, String>() {
                    {
                        put("enable-watermark-push-down", "true");
                        put("failing-source", "true");
                    }
                });
    }

    @Test
    public void testEventTimeTumbleWindow() throws Exception {
        createTestValuesSinkTable(
                "MySink",
                "name STRING",
                "window_start TIMESTAMP(3)",
                "window_end TIMESTAMP(3)",
                "cnt BIGINT",
                "sum_int INT",
                "distinct_cnt BIGINT");
        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink select\n"
                                + "  name,\n"
                                + "  window_start,\n"
                                + "  window_end,\n"
                                + "  COUNT(*),\n"
                                + "  SUM(`int`),\n"
                                + "  COUNT(DISTINCT `string`)\n"
                                + "FROM TABLE(\n"
                                + "   TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))\n"
                                + "GROUP BY name, window_start, window_end");
        tableEnv.executeJsonPlan(jsonPlan).await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList(
                        "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:05, 4, 10, 2]",
                        "+I[a, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 1, 3, 1]",
                        "+I[b, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 2, 9, 2]",
                        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:20, 1, 4, 1]",
                        "+I[b, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 1, 1, 1]",
                        "+I[null, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 1, 7, 0]"),
                result);
    }

    @Test
    public void testEventTimeHopWindow() throws Exception {
        createTestValuesSinkTable("MySink", "name STRING", "cnt BIGINT");
        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink select\n"
                                + "  name,\n"
                                + "  COUNT(*)\n"
                                + "FROM TABLE(\n"
                                + "   HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))\n"
                                + "GROUP BY name, window_start, window_end");
        tableEnv.executeJsonPlan(jsonPlan).await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList(
                        "+I[a, 1]",
                        "+I[a, 4]",
                        "+I[a, 5]",
                        "+I[b, 1]",
                        "+I[b, 1]",
                        "+I[b, 1]",
                        "+I[b, 1]",
                        "+I[b, 2]",
                        "+I[b, 2]",
                        "+I[null, 1]",
                        "+I[null, 1]"),
                result);
    }

    @Test
    public void testEventTimeCumulateWindow() throws Exception {
        createTestValuesSinkTable("MySink", "name STRING", "cnt BIGINT");
        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink select\n"
                                + "  name,\n"
                                + "  COUNT(*)\n"
                                + "FROM TABLE(\n"
                                + "  CUMULATE(\n"
                                + "     TABLE MyTable,\n"
                                + "     DESCRIPTOR(rowtime),\n"
                                + "     INTERVAL '5' SECOND,\n"
                                + "     INTERVAL '15' SECOND))"
                                + "GROUP BY name, window_start, window_end");
        tableEnv.executeJsonPlan(jsonPlan).await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList(
                        "+I[a, 4]",
                        "+I[a, 5]",
                        "+I[a, 5]",
                        "+I[b, 1]",
                        "+I[b, 1]",
                        "+I[b, 1]",
                        "+I[b, 1]",
                        "+I[b, 1]",
                        "+I[b, 1]",
                        "+I[b, 2]",
                        "+I[b, 2]",
                        "+I[null, 1]",
                        "+I[null, 1]",
                        "+I[null, 1]"),
                result);
    }
}
