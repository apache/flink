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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Test;

/** Plan test for INSERT INTO. */
public class InsertIntoValuesTest extends TableTestBase {
    private final JavaStreamTableTestUtil util = javaStreamTestUtil();

    @Test
    public void testTypeInferenceWithNestedTypes() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t1 ("
                                + "  `mapWithStrings` MAP<STRING, STRING>,"
                                + "  `mapWithBytes` MAP<STRING, BYTES>"
                                + ") WITH ("
                                + "   'connector' = 'values'"
                                + ")");

        // As https://issues.apache.org/jira/browse/CALCITE-4603 says
        // before Calcite 1.27.0 it derived the type of nested collection based on the last element,
        // thus the type of the last element is narrower than the type of element in the middle
        util.verifyExecPlanInsert(
                "INSERT INTO t1 VALUES "
                        + "(MAP['a', '123', 'b', '123456'], MAP['k1', X'C0FFEE', 'k2', X'BABE']), "
                        + "(CAST(NULL AS MAP<STRING, STRING>), CAST(NULL AS MAP<STRING, BYTES>)), "
                        + "(MAP['a', '1', 'b', '1'], MAP['k1', X'10', 'k2', X'20'])");
    }
}
