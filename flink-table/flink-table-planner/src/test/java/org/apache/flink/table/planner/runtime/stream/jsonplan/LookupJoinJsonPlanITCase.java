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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/** Test for LookupJoin json plan. */
class LookupJoinJsonPlanITCase extends JsonPlanTestBase {

    @BeforeEach
    @Override
    protected void setup() throws Exception {
        super.setup();
        List<Row> rowT1 =
                Arrays.asList(
                        Row.of(1L, 12, "Julian"),
                        Row.of(2L, 15, "Hello"),
                        Row.of(3L, 15, "Fabian"),
                        Row.of(8L, 11, "Hello world"),
                        Row.of(9L, 12, "Hello world!"));

        List<Row> rowT2 =
                Arrays.asList(
                        Row.of(11, 1L, "Julian"),
                        Row.of(22, 2L, "Jark"),
                        Row.of(33, 3L, "Fabian"),
                        Row.of(11, 4L, "Hello world"),
                        Row.of(11, 5L, "Hello world"));
        createTestValuesSourceTable(
                "src", rowT1, "id bigint", "len int", "content varchar", "proctime as PROCTIME()");
        createTestValuesSourceTable(
                "user_table",
                rowT2,
                new String[] {"age int", "id bigint", "name varchar"},
                new HashMap<String, String>() {
                    {
                        put("disable-lookup", "false");
                    }
                });
        createTestValuesSinkTable(
                "MySink", "id bigint", "len int", "content varchar", "name varchar");
    }

    /** test join temporal table. * */
    @Test
    void testJoinLookupTable() throws Exception {
        compileSqlAndExecutePlan(
                        "insert into MySink "
                                + "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table \n"
                                + "for system_time as of T.proctime AS D ON T.id = D.id \n")
                .await();
        List<String> expected =
                Arrays.asList(
                        "+I[1, 12, Julian, Julian]",
                        "+I[2, 15, Hello, Jark]",
                        "+I[3, 15, Fabian, Fabian]");
        assertResult(expected, TestValuesTableFactory.getResultsAsStrings("MySink"));
    }

    @Test
    void testJoinLookupTableWithPushDown() throws Exception {
        compileSqlAndExecutePlan(
                        "insert into MySink \n"
                                + "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table \n "
                                + "for system_time as of T.proctime AS D ON T.id = D.id AND D.age > 20")
                .await();
        List<String> expected =
                Arrays.asList("+I[2, 15, Hello, Jark]", "+I[3, 15, Fabian, Fabian]");
        assertResult(expected, TestValuesTableFactory.getResultsAsStrings("MySink"));
    }

    @Test
    void testLeftJoinLookupTableWithPreFilter() throws Exception {
        compileSqlAndExecutePlan(
                        "insert into MySink "
                                + "SELECT T.id, T.len, T.content, D.name FROM src AS T LEFT JOIN user_table \n"
                                + "for system_time as of T.proctime AS D ON T.id = D.id AND D.age > 20 AND T.id <> 3\n")
                .await();
        List<String> expected =
                Arrays.asList(
                        "+I[1, 12, Julian, null]",
                        "+I[2, 15, Hello, Jark]",
                        "+I[3, 15, Fabian, null]",
                        "+I[8, 11, Hello world, null]",
                        "+I[9, 12, Hello world!, null]");
        assertResult(expected, TestValuesTableFactory.getResultsAsStrings("MySink"));
    }
}
