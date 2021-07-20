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

import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc0;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc2;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.UdfWithOpen;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Test for calc json plan. */
public class CalcJsonPlanITCase extends JsonPlanTestBase {

    @Test
    public void testSimpleCalc() throws Exception {
        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        createTestCsvSourceTable("MyTable", data, "a bigint", "b int not null", "c varchar");
        File sinkPath =
                createTestCsvSinkTable("MySink", "a bigint", "a1 varchar", "b int", "c1 varchar");

        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink select "
                                + "a, "
                                + "cast(a as varchar) as a1, "
                                + "b, "
                                + "substring(c, 1, 8) as c1 "
                                + "from MyTable where b > 1");
        tableEnv.executeJsonPlan(jsonPlan).await();

        assertResult(Collections.singletonList("3,3,2,hello wo"), sinkPath);
    }

    @Test
    public void testCalcWithUdf() throws Exception {
        tableEnv.createTemporaryFunction("udf1", new JavaFunc0());
        tableEnv.createTemporarySystemFunction("udf2", new JavaFunc2());
        tableEnv.createFunction("udf3", UdfWithOpen.class);

        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.smallData3()),
                "a int",
                "b bigint",
                "c varchar");
        File sinkPath =
                createTestCsvSinkTable(
                        "MySink", "a int", "a1 varchar", "b bigint", "c1 varchar", "c2 varchar");

        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink select "
                                + "a, "
                                + "cast(a as varchar) as a1, "
                                + "b, "
                                + "udf2(c, a) as c1, "
                                + "udf3(substring(c, 1, 8)) as c2 "
                                + "from MyTable where "
                                + "(udf1(a) > 2 or (a * b) > 1) and b > 0");
        tableEnv.executeJsonPlan(jsonPlan).await();

        assertResult(
                Arrays.asList("2,2,2,Hello2,$Hello", "3,3,2,Hello world3,$Hello wo"), sinkPath);
    }
}
