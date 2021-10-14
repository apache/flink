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

/** Test for window join json plan. */
public class WindowJoinJsonITCase extends JsonPlanTestBase {

    @Before
    public void setup() throws Exception {
        super.setup();
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.windowDataWithTimestamp()),
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

        createTestValuesSourceTable(
                "MyTable2",
                JavaScalaConversionUtil.toJava(TestData.windowData2WithTimestamp()),
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
                "uv1 BIGINT",
                "uv2 BIGINT");
        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink select\n"
                                + "  L.name,\n"
                                + "  L.window_start,\n"
                                + "  L.window_end,\n"
                                + "  uv1,\n"
                                + "  uv2\n"
                                + "FROM(\n"
                                + "  SELECT\n"
                                + "    name,\n"
                                + "    window_start,\n"
                                + "    window_end,\n"
                                + "    COUNT(DISTINCT `string`) as uv1\n"
                                + "  FROM TABLE(\n"
                                + "     TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))\n"
                                + "  GROUP BY name, window_start, window_end\n"
                                + "  ) L\n"
                                + "JOIN (\n"
                                + "  SELECT\n"
                                + "    name,\n"
                                + "    window_start,\n"
                                + "    window_end,\n"
                                + "    COUNT(DISTINCT `string`) as uv2\n"
                                + "  FROM TABLE(\n"
                                + "     TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))\n"
                                + "  GROUP BY name, window_start, window_end\n"
                                + "  ) R\n"
                                + "ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.name = R.name");
        tableEnv.executeJsonPlan(jsonPlan).await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList(
                        "+I[b, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 2, 2]",
                        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:20, 1, 1]",
                        "+I[b, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 1, 1]"),
                result);
    }
}
