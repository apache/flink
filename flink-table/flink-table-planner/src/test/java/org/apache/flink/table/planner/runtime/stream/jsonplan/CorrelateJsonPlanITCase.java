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
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Integration tests for correlate. */
class CorrelateJsonPlanITCase extends JsonPlanTestBase {

    @BeforeEach
    void before() {
        List<Row> data = Collections.singletonList(Row.of("1,1,hi"));
        createTestValuesSourceTable("MyTable", data, "a varchar");
    }

    @Test
    void testSystemFuncByObject() throws ExecutionException, InterruptedException {
        tableEnv.createTemporarySystemFunction(
                "STRING_SPLIT", new JavaUserDefinedTableFunctions.StringSplit());
        createTestValuesSinkTable("MySink", "a STRING", "b STRING");
        String query =
                "insert into MySink SELECT a, v FROM MyTable, lateral table(STRING_SPLIT(a, ',')) as T(v)";
        compileSqlAndExecutePlan(query).await();
        List<String> expected = Arrays.asList("+I[1,1,hi, 1]", "+I[1,1,hi, 1]", "+I[1,1,hi, hi]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testSystemFuncByClass() throws ExecutionException, InterruptedException {
        tableEnv.createTemporarySystemFunction(
                "STRING_SPLIT", JavaUserDefinedTableFunctions.StringSplit.class);
        createTestValuesSinkTable("MySink", "a STRING", "b STRING");
        String query =
                "insert into MySink SELECT a, v FROM MyTable, lateral table(STRING_SPLIT(a, ',')) as T(v)";
        compileSqlAndExecutePlan(query).await();
        List<String> expected = Arrays.asList("+I[1,1,hi, 1]", "+I[1,1,hi, 1]", "+I[1,1,hi, hi]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testTemporaryFuncByObject() throws ExecutionException, InterruptedException {
        tableEnv.createTemporaryFunction(
                "STRING_SPLIT", new JavaUserDefinedTableFunctions.StringSplit());
        createTestValuesSinkTable("MySink", "a STRING", "b STRING");
        String query =
                "insert into MySink SELECT a, v FROM MyTable, lateral table(STRING_SPLIT(a, ',')) as T(v)";
        compileSqlAndExecutePlan(query).await();
        List<String> expected = Arrays.asList("+I[1,1,hi, 1]", "+I[1,1,hi, 1]", "+I[1,1,hi, hi]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testTemporaryFuncByClass() throws ExecutionException, InterruptedException {
        tableEnv.createTemporaryFunction(
                "STRING_SPLIT", JavaUserDefinedTableFunctions.StringSplit.class);
        createTestValuesSinkTable("MySink", "a STRING", "b STRING");
        String query =
                "insert into MySink SELECT a, v FROM MyTable, lateral table(STRING_SPLIT(a, ',')) as T(v)";
        compileSqlAndExecutePlan(query).await();
        List<String> expected = Arrays.asList("+I[1,1,hi, 1]", "+I[1,1,hi, 1]", "+I[1,1,hi, hi]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testFilter() throws ExecutionException, InterruptedException {
        tableEnv.createTemporarySystemFunction(
                "STRING_SPLIT", new JavaUserDefinedTableFunctions.StringSplit());
        createTestValuesSinkTable("MySink", "a STRING", "b STRING");
        String query =
                "insert into MySink "
                        + "SELECT a, v FROM MyTable, lateral table(STRING_SPLIT(a, ',')) as T(v) "
                        + "where try_cast(v as int) > 0";
        compileSqlAndExecutePlan(query).await();
        List<String> expected = Arrays.asList("+I[1,1,hi, 1]", "+I[1,1,hi, 1]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testUnnest() throws ExecutionException, InterruptedException {
        List<Row> data =
                Collections.singletonList(
                        Row.of("Bob", new Row[] {Row.of("1"), Row.of("2"), Row.of("3")}));
        createTestValuesSourceTable(
                "MyNestedTable", data, "name STRING", "arr ARRAY<ROW<nested STRING>>");
        createTestValuesSinkTable("MySink", "name STRING", "nested STRING");
        String query =
                "INSERT INTO MySink SELECT name, nested FROM MyNestedTable CROSS JOIN UNNEST(arr) AS t (nested)";
        compileSqlAndExecutePlan(query).await();
        List<String> expected = Arrays.asList("+I[Bob, 1]", "+I[Bob, 2]", "+I[Bob, 3]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }
}
