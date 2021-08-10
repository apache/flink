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
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/** Test for IntervalJoin json plan. */
public class IntervalJoinJsonPlanITCase extends JsonPlanTestBase {

    /** test process time inner join. * */
    @Test
    public void testProcessTimeInnerJoin() throws Exception {
        List<Row> rowT1 =
                Arrays.asList(
                        Row.of(1, 1L, "Hi1"),
                        Row.of(1, 2L, "Hi2"),
                        Row.of(1, 5L, "Hi3"),
                        Row.of(2, 7L, "Hi5"),
                        Row.of(1, 9L, "Hi6"),
                        Row.of(1, 8L, "Hi8"));

        List<Row> rowT2 = Arrays.asList(Row.of(1, 1L, "HiHi"), Row.of(2, 2L, "HeHe"));
        createTestValuesSourceTable(
                "T1", rowT1, "a int", "b bigint", "c varchar", "proctime as PROCTIME()");
        createTestValuesSourceTable(
                "T2", rowT2, "a int", "b bigint", "c varchar", "proctime as PROCTIME()");
        createTestValuesSinkTable("MySink", "a int", "c1 varchar", "c2 varchar");

        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink "
                                + "SELECT t2.a, t2.c, t1.c\n"
                                + "FROM T1 as t1 join T2 as t2 ON\n"
                                + "  t1.a = t2.a AND\n"
                                + "  t1.proctime BETWEEN t2.proctime - INTERVAL '5' SECOND AND\n"
                                + "    t2.proctime + INTERVAL '5' SECOND");
        tableEnv.executeJsonPlan(jsonPlan).await();
        List<String> expected =
                Arrays.asList(
                        "+I[1, HiHi, Hi1]",
                        "+I[1, HiHi, Hi2]",
                        "+I[1, HiHi, Hi3]",
                        "+I[1, HiHi, Hi6]",
                        "+I[1, HiHi, Hi8]",
                        "+I[2, HeHe, Hi5]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    public void testRowTimeInnerJoin() throws Exception {
        List<Row> rowT1 =
                Arrays.asList(
                        Row.of(1, 1L, "Hi1"),
                        Row.of(1, 2L, "Hi2"),
                        Row.of(1, 5L, "Hi3"),
                        Row.of(2, 7L, "Hi5"),
                        Row.of(1, 9L, "Hi6"),
                        Row.of(1, 8L, "Hi8"));

        List<Row> rowT2 = Arrays.asList(Row.of(1, 1L, "HiHi"), Row.of(2, 2L, "HeHe"));
        createTestValuesSourceTable(
                "T1",
                rowT1,
                "a int",
                "b bigint",
                "c varchar",
                "rowtime as TO_TIMESTAMP (FROM_UNIXTIME(b))",
                "watermark for rowtime as rowtime - INTERVAL '5' second");
        createTestValuesSourceTable(
                "T2",
                rowT2,
                "a int",
                "b bigint",
                "c varchar",
                "rowtime as TO_TIMESTAMP (FROM_UNIXTIME(b))",
                "watermark for rowtime as rowtime - INTERVAL '5' second");
        createTestValuesSinkTable("MySink", "a int", "c1 varchar", "c2 varchar");

        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink \n"
                                + "SELECT t2.a, t2.c, t1.c\n"
                                + "FROM T1 as t1 join T2 as t2 ON\n"
                                + "  t1.a = t2.a AND\n"
                                + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND\n"
                                + "    t2.rowtime + INTERVAL '6' SECOND");
        tableEnv.executeJsonPlan(jsonPlan).await();
        List<String> expected =
                Arrays.asList(
                        "+I[1, HiHi, Hi1]",
                        "+I[1, HiHi, Hi2]",
                        "+I[1, HiHi, Hi3]",
                        "+I[2, HeHe, Hi5]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }
}
