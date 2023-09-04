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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Test for Sarg JsonPlan ser/de. */
class SargJsonPlanITCase extends JsonPlanTestBase {
    @Test
    void testSarg() throws ExecutionException, InterruptedException {
        List<Row> data =
                Arrays.asList(Row.of(1), Row.of(2), Row.of((Integer) null), Row.of(4), Row.of(5));
        createTestValuesSourceTable("MyTable", data, "a int");
        createTestNonInsertOnlyValuesSinkTable("`result`", "a int");
        String sql =
                "insert into `result` SELECT a\n"
                        + "FROM MyTable WHERE a = 1 OR a = 2 OR a IS NULL";
        compileSqlAndExecutePlan(sql).await();
        List<String> expected = Arrays.asList("+I[1]", "+I[2]", "+I[null]");
        assertResult(expected, TestValuesTableFactory.getResults("result"));
    }
}
