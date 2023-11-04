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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for temporal sort json plan. */
class TemporalSortJsonITCase extends JsonPlanTestBase {

    @Test
    void testSortProcessingTime() throws Exception {
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.smallData3()),
                "a INT",
                "b BIGINT",
                "c STRING",
                "proctime as PROCTIME()");
        createTestValuesSinkTable("MySink", "a INT");
        compileSqlAndExecutePlan("insert into MySink SELECT a FROM MyTable order by proctime")
                .await();

        assertResult(
                Arrays.asList("+I[1]", "+I[2]", "+I[3]"),
                TestValuesTableFactory.getResultsAsStrings("MySink"));
    }

    @Test
    void testSortRowTime() throws Exception {
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
        createTestValuesSinkTable("MySink", "`int` INT");
        compileSqlAndExecutePlan(
                        "insert into MySink SELECT `int` FROM MyTable order by rowtime, `double`")
                .await();

        assertThat(TestValuesTableFactory.getResultsAsStrings("MySink"))
                .isEqualTo(
                        Arrays.asList(
                                "+I[1]", "+I[2]", "+I[2]", "+I[5]", "+I[6]", "+I[3]", "+I[3]",
                                "+I[4]", "+I[7]", "+I[1]"));
    }
}
