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

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/** Test for deduplication json plan. */
public class DeduplicationJsonPlanITCase extends JsonPlanTestBase {

    @Test
    public void testDeduplication() throws Exception {
        List<Row> data =
                Arrays.asList(
                        Row.of(1L, "terry", "pen", 1000L),
                        Row.of(2L, "alice", "pen", 2000L),
                        Row.of(3L, "bob", "pen", 3000L),
                        Row.of(4L, "bob", "apple", 4000L),
                        Row.of(5L, "fish", "apple", 5000L));
        createTestValuesSourceTable(
                "MyTable",
                data,
                "order_id bigint",
                "`user` varchar",
                "product varchar",
                "order_time bigint ",
                "event_time as TO_TIMESTAMP(FROM_UNIXTIME(order_time)) ",
                "watermark for event_time as event_time - INTERVAL '5' second ");

        createTestNonInsertOnlyValuesSinkTable(
                "MySink",
                "order_id bigint",
                "`user` varchar",
                "product varchar",
                "order_time bigint",
                "primary key(product) not enforced");

        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink "
                                + "select order_id, user, product, order_time \n"
                                + "FROM (\n"
                                + "  SELECT *,\n"
                                + "    ROW_NUMBER() OVER (PARTITION BY product ORDER BY event_time ASC) AS row_num\n"
                                + "  FROM MyTable)\n"
                                + "WHERE row_num = 1 \n");
        tableEnv.getConfig()
                .getConfiguration()
                .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tableEnv.executeJsonPlan(jsonPlan).await();

        assertResult(
                Arrays.asList("+I[1, terry, pen, 1000]", "+I[4, bob, apple, 4000]"),
                TestValuesTableFactory.getRawResults("MySink"));
    }
}
