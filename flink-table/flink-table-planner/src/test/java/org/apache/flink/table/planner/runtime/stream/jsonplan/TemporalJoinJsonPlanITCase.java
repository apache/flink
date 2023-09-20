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

import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/** Test for TemporalJoin json plan. */
class TemporalJoinJsonPlanITCase extends JsonPlanTestBase {

    @BeforeEach
    @Override
    protected void setup() throws Exception {
        super.setup();
        List<Row> orders =
                Arrays.asList(
                        Row.of(2L, "Euro", 2L),
                        Row.of(1L, "US Dollar", 3L),
                        Row.of(50L, "Yen", 4L),
                        Row.of(3L, "Euro", 5L));
        createTestValuesSourceTable(
                "Orders",
                orders,
                "amount bigint",
                "currency STRING",
                "order_time bigint",
                "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(order_time)) ",
                "proctime as PROCTIME()",
                "WATERMARK FOR rowtime AS rowtime");
        List<Row> ratesHistory =
                Arrays.asList(
                        Row.of("US Dollar", 102L, 1L),
                        Row.of("Euro", 114L, 1L),
                        Row.of("Yen", 1L, 1L),
                        Row.of("Euro", 116L, 5L),
                        Row.of("Euro", 119L, 7L));
        createTestValuesSourceTable(
                "RatesHistory",
                ratesHistory,
                "currency STRING",
                "rate bigint",
                "rate_time bigint",
                "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(rate_time)) ",
                "proctime as PROCTIME()",
                "WATERMARK FOR rowtime AS rowtime",
                "PRIMARY KEY(currency) NOT ENFORCED");

        TemporalTableFunction temporalTableFunction =
                tableEnv.from("RatesHistory")
                        .createTemporalTableFunction($("rowtime"), $("currency"));
        tableEnv.createTemporarySystemFunction("Rates", temporalTableFunction);
        createTestValuesSinkTable("MySink", "amount bigint");
    }

    /** test process time inner join. * */
    @Test
    void testJoinTemporalFunction() throws Exception {
        compileSqlAndExecutePlan(
                        "INSERT INTO MySink "
                                + "SELECT amount * r.rate "
                                + "FROM Orders AS o,  "
                                + "LATERAL TABLE (Rates(o.rowtime)) AS r "
                                + "WHERE o.currency = r.currency ")
                .await();
        List<String> expected = Arrays.asList("+I[102]", "+I[228]", "+I[348]", "+I[50]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    void testTemporalTableJoin() throws Exception {
        compileSqlAndExecutePlan(
                        "INSERT INTO MySink "
                                + "SELECT amount * r.rate "
                                + "FROM Orders AS o  "
                                + "JOIN RatesHistory  FOR SYSTEM_TIME AS OF o.rowtime AS r "
                                + "ON o.currency = r.currency ")
                .await();
        List<String> expected = Arrays.asList("+I[102]", "+I[228]", "+I[348]", "+I[50]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }
}
