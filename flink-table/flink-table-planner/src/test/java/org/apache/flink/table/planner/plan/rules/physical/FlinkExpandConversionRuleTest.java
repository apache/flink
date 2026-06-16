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

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.Expressions.$;

class FlinkExpandConversionRuleTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void setup() {
        util = batchTestUtil(TableConfig.getDefault());
        util.tableEnv().getConfig().set("parallelism.default", "1");

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
        Table src = util.tableEnv().from("MyTable");
        Table result = src.orderBy($("b").asc()).select($("a").max());

        util.verifyRelPlan(result);
    }
}
