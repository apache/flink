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

package org.apache.flink.table.planner.plan.rules.physical;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FlinkExpandConversionRuleTest extends TableTestBase {

    private final BatchTableTestUtil util = batchTestUtil(TableConfig.getDefault());

    @BeforeEach
    void setup() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable ("
                                + "  a INT,"
                                + "  b STRING"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'bounded' = 'true'"
                                + ")");
    }

    @Test
    void testOrderByWithGlobalAggregate() {
        util.verifyRelPlan("SELECT MAX(a) FROM (SELECT a, b FROM MyTable ORDER BY b ASC)");
    }
}
