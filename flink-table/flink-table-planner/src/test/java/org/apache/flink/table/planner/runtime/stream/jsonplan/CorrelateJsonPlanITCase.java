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

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Integration tests for correlate. */
public class CorrelateJsonPlanITCase extends JsonPlanTestBase {

    @Before
    public void before() {
        List<Row> data = Collections.singletonList(Row.of("1,1,hi"));
        createTestValuesSourceTable("MyTable", data, "a varchar");
    }

    @Test
    public void testSystemFuncByObject()
            throws ExecutionException, InterruptedException, IOException {
        tableEnv.createTemporarySystemFunction(
                "STRING_SPLIT", new JavaUserDefinedTableFunctions.StringSplit());
        createTestValuesSinkTable("MySink", "a STRING", "b STRING");
        String query =
                "insert into MySink SELECT a, v FROM MyTable, lateral table(STRING_SPLIT(a, ',')) as T(v)";
        executeSqlWithJsonPlanVerified(query).await();
        List<String> expected = Arrays.asList("+I[1,1,hi, 1]", "+I[1,1,hi, 1]", "+I[1,1,hi, hi]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    public void testSystemFuncByClass()
            throws ExecutionException, InterruptedException, IOException {
        tableEnv.createTemporarySystemFunction(
                "STRING_SPLIT", JavaUserDefinedTableFunctions.StringSplit.class);
        createTestValuesSinkTable("MySink", "a STRING", "b STRING");
        String query =
                "insert into MySink SELECT a, v FROM MyTable, lateral table(STRING_SPLIT(a, ',')) as T(v)";
        executeSqlWithJsonPlanVerified(query).await();
        List<String> expected = Arrays.asList("+I[1,1,hi, 1]", "+I[1,1,hi, 1]", "+I[1,1,hi, hi]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    public void testTemporaryFuncByObject()
            throws ExecutionException, InterruptedException, IOException {
        tableEnv.createTemporaryFunction(
                "STRING_SPLIT", new JavaUserDefinedTableFunctions.StringSplit());
        createTestValuesSinkTable("MySink", "a STRING", "b STRING");
        String query =
                "insert into MySink SELECT a, v FROM MyTable, lateral table(STRING_SPLIT(a, ',')) as T(v)";
        executeSqlWithJsonPlanVerified(query).await();
        List<String> expected = Arrays.asList("+I[1,1,hi, 1]", "+I[1,1,hi, 1]", "+I[1,1,hi, hi]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    public void testTemporaryFuncByClass()
            throws ExecutionException, InterruptedException, IOException {
        tableEnv.createTemporaryFunction(
                "STRING_SPLIT", JavaUserDefinedTableFunctions.StringSplit.class);
        createTestValuesSinkTable("MySink", "a STRING", "b STRING");
        String query =
                "insert into MySink SELECT a, v FROM MyTable, lateral table(STRING_SPLIT(a, ',')) as T(v)";
        executeSqlWithJsonPlanVerified(query).await();
        List<String> expected = Arrays.asList("+I[1,1,hi, 1]", "+I[1,1,hi, 1]", "+I[1,1,hi, hi]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    public void testFilter() throws ExecutionException, InterruptedException, IOException {
        tableEnv.createTemporarySystemFunction(
                "STRING_SPLIT", new JavaUserDefinedTableFunctions.StringSplit());
        createTestValuesSinkTable("MySink", "a STRING", "b STRING");
        String query =
                "insert into MySink SELECT a, v FROM MyTable, lateral table(STRING_SPLIT(a, ',')) as T(v) where cast(v as int) > 0";
        executeSqlWithJsonPlanVerified(query).await();
        List<String> expected = Arrays.asList("+I[1,1,hi, 1]", "+I[1,1,hi, 1]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }
}
