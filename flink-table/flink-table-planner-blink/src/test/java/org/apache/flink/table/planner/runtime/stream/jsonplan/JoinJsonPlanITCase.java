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

import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/** Test for join json plan. */
public class JoinJsonPlanITCase extends JsonPlanTestBase {

    @Test
    public void testSimpleJoin() throws Exception {
        List<String> dataT1 =
                Arrays.asList(
                        "1,1,Hi1", "1,2,Hi2", "1,2,Hi2", "1,5,Hi3", "2,7,Hi5", "1,9,Hi6", "1,8,Hi8",
                        "3,8,Hi9");
        List<String> dataT2 = Arrays.asList("1,1,HiHi", "2,2,HeHe", "3,2,HeHe");
        createTestCsvSourceTable("T1", dataT1, "a int", "b bigint", "c varchar");
        createTestCsvSourceTable("T2", dataT2, "a int", "b bigint", "c varchar");
        File sinkPath = createTestCsvSinkTable("MySink", "a int", "c1 varchar", "c2 varchar");

        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink "
                                + "SELECT t2.a, t2.c, t1.c\n"
                                + "FROM (\n"
                                + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T1\n"
                                + ") as t1\n"
                                + "JOIN (\n"
                                + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T2\n"
                                + ") as t2\n"
                                + "ON t1.a = t2.a AND t1.b > t2.b");
        tableEnv.executeJsonPlan(jsonPlan).await();
        List<String> expected =
                Arrays.asList(
                        "1,HiHi,Hi2",
                        "1,HiHi,Hi2",
                        "1,HiHi,Hi3",
                        "1,HiHi,Hi6",
                        "1,HiHi,Hi8",
                        "2,HeHe,Hi5");
        assertResult(expected, sinkPath);
    }
}
