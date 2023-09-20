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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/** Test for table sink json plan. */
class TableSinkJsonPlanITCase extends JsonPlanTestBase {

    List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");

    @BeforeEach
    @Override
    protected void setup() throws Exception {
        super.setup();
        createTestCsvSourceTable("MyTable", data, "a bigint", "b int", "c varchar");
    }

    @Test
    void testPartitioning() throws Exception {
        File sinkPath =
                createTestCsvSinkTable(
                        "MySink",
                        new String[] {"a bigint", "p int not null", "b int", "c varchar"},
                        "b");

        compileSqlAndExecutePlan("insert into MySink partition (b=3) select * from MyTable")
                .await();

        assertResult(data, sinkPath);
    }

    @Test
    void testWritingMetadata() throws Exception {
        createTestValuesSinkTable(
                "MySink",
                new String[] {"a bigint", "b int", "c varchar METADATA"},
                new HashMap<String, String>() {
                    {
                        put("writable-metadata", "c:STRING");
                    }
                });

        compileSqlAndExecutePlan("insert into MySink select * from MyTable").await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList("+I[1, 1, hi]", "+I[2, 1, hello]", "+I[3, 2, hello world]"), result);
    }
}
