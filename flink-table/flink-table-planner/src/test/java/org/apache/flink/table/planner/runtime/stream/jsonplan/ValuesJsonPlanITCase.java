/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/** Test for values json plan. */
public class ValuesJsonPlanITCase extends JsonPlanTestBase {

    @Test
    public void testValues() throws Exception {
        createTestValuesSinkTable("MySink", "b INT", "a INT", "c VARCHAR");
        String jsonPlan =
                tableEnv.getJsonPlan(
                        "INSERT INTO MySink SELECT * from (VALUES (1, 2, 'Hi'), (3, 4, 'Hello'))");
        tableEnv.executeJsonPlan(jsonPlan).await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(Arrays.asList("+I[1, 2, Hi]", "+I[3, 4, Hello]"), result);
    }
}
