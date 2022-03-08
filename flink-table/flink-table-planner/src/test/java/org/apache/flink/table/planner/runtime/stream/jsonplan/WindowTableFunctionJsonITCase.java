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

/** Test for window deduplicate json plan. */
public class WindowTableFunctionJsonITCase extends JsonPlanTestBase {

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
    }

    @Test
    public void testEventTimeTumbleWindow() throws Exception {
        createTestValuesSinkTable(
                "MySink",
                "ts STRING",
                "`int` INT",
                "`double` DOUBLE",
                "`float` FLOAT",
                "`bigdec` DECIMAL(10, 2)",
                "`string` STRING",
                "`name` STRING",
                "`rowtime` STRING",
                "window_start TIMESTAMP(3)",
                "window_end TIMESTAMP(3)",
                "window_time TIMESTAMP(3)");
        compileSqlAndExecutePlan(
                        "insert into MySink select\n"
                                + "  `ts`,\n"
                                + "  `int`,\n"
                                + "  `double`,\n"
                                + "  `float`, \n"
                                + "  `bigdec`, \n"
                                + "  `string`, \n"
                                + "  `name`, \n"
                                + "  CAST(`rowtime` AS STRING), \n"
                                + "  window_start, \n"
                                + "  window_end, \n"
                                + "  window_time \n"
                                + "FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))")
                .await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList(
                        "+I[2020-10-10 00:00:01, 1, 1.0, 1.0, 1.11, Hi, a, 2020-10-10 00:00:01.000, 2020-10-10T00:00, 2020-10-10T00:00:05, 2020-10-10T00:00:04.999]",
                        "+I[2020-10-10 00:00:02, 2, 2.0, 2.0, 2.22, Comment#1, a, 2020-10-10 00:00:02.000, 2020-10-10T00:00, 2020-10-10T00:00:05, 2020-10-10T00:00:04.999]",
                        "+I[2020-10-10 00:00:03, 2, 2.0, 2.0, 2.22, Comment#1, a, 2020-10-10 00:00:03.000, 2020-10-10T00:00, 2020-10-10T00:00:05, 2020-10-10T00:00:04.999]",
                        "+I[2020-10-10 00:00:04, 5, 5.0, 5.0, 5.55, null, a, 2020-10-10 00:00:04.000, 2020-10-10T00:00, 2020-10-10T00:00:05, 2020-10-10T00:00:04.999]",
                        "+I[2020-10-10 00:00:04, 5, 5.0, null, 5.55, Hi, a, 2020-10-10 00:00:04.000, 2020-10-10T00:00, 2020-10-10T00:00:05, 2020-10-10T00:00:04.999]",
                        "+I[2020-10-10 00:00:06, 6, 6.0, 6.0, 6.66, Hi, b, 2020-10-10 00:00:06.000, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 2020-10-10T00:00:09.999]",
                        "+I[2020-10-10 00:00:07, 3, 3.0, 3.0, null, Hello, b, 2020-10-10 00:00:07.000, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 2020-10-10T00:00:09.999]",
                        "+I[2020-10-10 00:00:08, 3, null, 3.0, 3.33, Comment#2, a, 2020-10-10 00:00:08.000, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 2020-10-10T00:00:09.999]",
                        "+I[2020-10-10 00:00:16, 4, 4.0, 4.0, 4.44, Hi, b, 2020-10-10 00:00:16.000, 2020-10-10T00:00:15, 2020-10-10T00:00:20, 2020-10-10T00:00:19.999]",
                        "+I[2020-10-10 00:00:32, 7, 7.0, 7.0, 7.77, null, null, 2020-10-10 00:00:32.000, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 2020-10-10T00:00:34.999]",
                        "+I[2020-10-10 00:00:34, 1, 3.0, 3.0, 3.33, Comment#3, b, 2020-10-10 00:00:34.000, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 2020-10-10T00:00:34.999]"),
                result);
    }
}
