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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/** Test for join json plan. */
class JoinJsonPlanITCase extends JsonPlanTestBase {

    @Override
    @BeforeEach
    protected void setup() throws Exception {
        super.setup();
        createTestValuesSourceTable(
                "A",
                JavaScalaConversionUtil.toJava(TestData.smallData3()),
                "a1 int",
                "a2 bigint",
                "a3 varchar");
        createTestValuesSourceTable(
                "B",
                JavaScalaConversionUtil.toJava(TestData.smallData5()),
                "b1 int",
                "b2 bigint",
                "b3 int",
                "b4 varchar",
                "b5 bigint");
    }

    /** test non-window inner join. * */
    @Test
    void testNonWindowInnerJoin() throws Exception {
        List<String> dataT1 =
                Arrays.asList(
                        "1,1,Hi1", "1,2,Hi2", "1,2,Hi2", "1,5,Hi3", "2,7,Hi5", "1,9,Hi6", "1,8,Hi8",
                        "3,8,Hi9");
        List<String> dataT2 = Arrays.asList("1,1,HiHi", "2,2,HeHe", "3,2,HeHe");
        createTestCsvSourceTable("T1", dataT1, "a int", "b bigint", "c varchar");
        createTestCsvSourceTable("T2", dataT2, "a int", "b bigint", "c varchar");
        File sinkPath = createTestCsvSinkTable("MySink", "a int", "c1 varchar", "c2 varchar");

        compileSqlAndExecutePlan(
                        "insert into MySink "
                                + "SELECT t2.a, t2.c, t1.c\n"
                                + "FROM (\n"
                                + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T1\n"
                                + ") as t1\n"
                                + "JOIN (\n"
                                + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T2\n"
                                + ") as t2\n"
                                + "ON t1.a = t2.a AND t1.b > t2.b")
                .await();
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

    @Test
    void testIsNullInnerJoinWithNullCond() throws Exception {
        List<String> dataT1 =
                Arrays.asList(
                        "1,1,Hi1", "1,2,Hi2", "1,2,Hi2", "1,5,Hi3", "2,7,Hi5", "1,9,Hi6", "1,8,Hi8",
                        "3,8,Hi9");
        List<String> dataT2 = Arrays.asList("1,1,HiHi", "2,2,HeHe", "3,2,HeHe");
        createTestCsvSourceTable("T1", dataT1, "a int", "b bigint", "c varchar");
        createTestCsvSourceTable("T2", dataT2, "a int", "b bigint", "c varchar");
        createTestValuesSinkTable("MySink", "a int", "c1 varchar", "c2 varchar");

        compileSqlAndExecutePlan(
                        "insert into MySink "
                                + "SELECT t2.a, t2.c, t1.c\n"
                                + "FROM (\n"
                                + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T1\n"
                                + ") as t1\n"
                                + "JOIN (\n"
                                + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T2\n"
                                + ") as t2\n"
                                + "ON \n"
                                + "  ((t1.a is null AND t2.a is null) OR\n"
                                + "  (t1.a = t2.a))\n"
                                + "  AND t1.b > t2.b")
                .await();
        List<String> expected =
                Arrays.asList(
                        "+I[1, HiHi, Hi2]",
                        "+I[1, HiHi, Hi2]",
                        "+I[1, HiHi, Hi3]",
                        "+I[1, HiHi, Hi6]",
                        "+I[1, HiHi, Hi8]",
                        "+I[2, HeHe, Hi5]",
                        "+I[null, HeHe, Hi9]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testJoin() throws Exception {
        createTestValuesSinkTable("MySink", "a3 varchar", "b4 varchar");
        compileSqlAndExecutePlan("insert into MySink \n" + "SELECT a3, b4 FROM A, B WHERE a2 = b2")
                .await();
        List<String> expected =
                Arrays.asList(
                        "+I[Hello world, Hallo Welt]", "+I[Hello, Hallo Welt]", "+I[Hi, Hallo]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testInnerJoin() throws Exception {
        createTestValuesSinkTable("MySink", "a1 int", "b1 int");
        compileSqlAndExecutePlan("insert into MySink \n" + "SELECT a1, b1 FROM A JOIN B ON a1 = b1")
                .await();
        List<String> expected = Arrays.asList("+I[1, 1]", "+I[2, 2]", "+I[2, 2]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testJoinWithFilter() throws Exception {
        createTestValuesSinkTable("MySink", "a3 varchar", "b4 varchar");
        compileSqlAndExecutePlan(
                        "insert into MySink \n"
                                + "SELECT a3, b4 FROM A, B where a2 = b2 and a2 < 2")
                .await();
        List<String> expected = Arrays.asList("+I[Hi, Hallo]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testInnerJoinWithDuplicateKey() throws Exception {
        createTestValuesSinkTable("MySink", "a1 int", "b1 int", "b3 int");
        compileSqlAndExecutePlan(
                        "insert into MySink \n"
                                + "SELECT a1, b1, b3 FROM A JOIN B ON a1 = b1 AND a1 = b3")
                .await();
        List<String> expected = Arrays.asList("+I[2, 2, 2]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }
}
