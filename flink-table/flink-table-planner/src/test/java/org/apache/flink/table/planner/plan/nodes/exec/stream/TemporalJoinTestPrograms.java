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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

/** {@link TableTestProgram} definitions for testing {@link StreamExecTemporalJoin}. */
public class TemporalJoinTestPrograms {
    static final SourceTestStep ORDERS =
            SourceTestStep.newBuilder("Orders")
                    .addSchema(
                            "amount bigint",
                            "currency STRING",
                            "order_time STRING",
                            "rowtime as TO_TIMESTAMP(order_time) ",
                            "WATERMARK FOR rowtime AS rowtime")
                    .producedBeforeRestore(
                            Row.of(2L, "Euro", "2020-10-10 00:00:42"),
                            Row.of(1L, "USD", "2020-10-10 00:00:43"),
                            Row.of(50L, "Yen", "2020-10-10 00:00:44"),
                            Row.of(3L, "Euro", "2020-10-10 00:00:45"))
                    .producedAfterRestore(
                            Row.of(1L, "Euro", "2020-10-10 00:00:58"),
                            Row.of(1L, "USD", "2020-10-10 00:00:58"))
                    .build();

    static final SourceTestStep ORDERS_WITH_NESTED_ID =
            SourceTestStep.newBuilder("OrdersNestedId")
                    .addSchema(
                            "amount bigint",
                            "nested_row ROW<currency STRING>",
                            "nested_map MAP<STRING NOT NULL, STRING>",
                            "order_time STRING",
                            "rowtime as TO_TIMESTAMP(order_time) ",
                            "WATERMARK FOR rowtime AS rowtime")
                    .producedBeforeRestore(
                            Row.of(
                                    2L,
                                    Row.of("Euro"),
                                    mapOf("currency", "Euro"),
                                    "2020-10-10 00:00:42"),
                            Row.of(
                                    1L,
                                    Row.of("usd"),
                                    mapOf("currency", "USD"),
                                    "2020-10-10 00:00:43"),
                            Row.of(
                                    50L,
                                    Row.of("Yen"),
                                    mapOf("currency", "Yen"),
                                    "2020-10-10 00:00:44"),
                            Row.of(
                                    3L,
                                    Row.of("Euro"),
                                    mapOf("currency", "Euro"),
                                    "2020-10-10 00:00:45"))
                    .producedAfterRestore(
                            Row.of(
                                    1L,
                                    Row.of("Euro"),
                                    mapOf("currency", "Euro"),
                                    "2020-10-10 00:00:58"),
                            Row.of(
                                    1L,
                                    Row.of("usd"),
                                    mapOf("currency", "USD"),
                                    "2020-10-10 00:00:58"))
                    .build();

    static final SourceTestStep RATES =
            SourceTestStep.newBuilder("RatesHistory")
                    .addSchema(
                            "currency STRING",
                            "rate bigint",
                            "rate_time STRING",
                            "rowtime as TO_TIMESTAMP(rate_time) ",
                            "WATERMARK FOR rowtime AS rowtime",
                            "PRIMARY KEY(currency) NOT ENFORCED")
                    .producedBeforeRestore(
                            Row.of("USD", 102L, "2020-10-10 00:00:41"),
                            Row.of("Euro", 114L, "2020-10-10 00:00:41"),
                            Row.of("Yen", 1L, "2020-10-10 00:00:41"),
                            Row.of("Euro", 116L, "2020-10-10 00:00:45"),
                            Row.of("Euro", 119L, "2020-10-10 00:00:47"))
                    .producedAfterRestore(
                            Row.of("USD", 103L, "2020-10-10 00:00:58"),
                            Row.of("Euro", 120L, "2020-10-10 00:00:59"))
                    .build();

    static final SinkTestStep AMOUNTS =
            SinkTestStep.newBuilder("MySink")
                    .addSchema("amount bigint")
                    .consumedBeforeRestore("+I[102]", "+I[228]", "+I[348]", "+I[50]")
                    .consumedAfterRestore("+I[103]", "+I[119]")
                    .build();
    static final TableTestProgram TEMPORAL_JOIN_TABLE_JOIN =
            TableTestProgram.of("temporal-join-table-join", "validates temporal join with a table")
                    .setupTableSource(ORDERS)
                    .setupTableSource(RATES)
                    .setupTableSink(AMOUNTS)
                    .runSql(
                            "INSERT INTO MySink "
                                    + "SELECT amount * r.rate "
                                    + "FROM Orders AS o "
                                    + "JOIN RatesHistory FOR SYSTEM_TIME AS OF o.rowtime AS r "
                                    + "ON o.currency = r.currency ")
                    .build();

    static final TableTestProgram TEMPORAL_JOIN_TABLE_JOIN_NESTED_KEY =
            TableTestProgram.of(
                            "temporal-join-table-join-nested-key",
                            "validates temporal join with a table when the join keys comes from a nested row")
                    .setupTableSource(ORDERS_WITH_NESTED_ID)
                    .setupTableSource(RATES)
                    .setupTableSink(AMOUNTS)
                    .runSql(
                            "INSERT INTO MySink "
                                    + "SELECT amount * r.rate "
                                    + "FROM OrdersNestedId AS o "
                                    + "JOIN RatesHistory FOR SYSTEM_TIME AS OF o.rowtime AS r "
                                    + "ON (case when o.nested_row.currency = 'usd' then upper(o.nested_row.currency) ELSE o.nested_row.currency END) = r.currency ")
                    .build();

    static final TableTestProgram TEMPORAL_JOIN_TABLE_JOIN_KEY_FROM_MAP =
            TableTestProgram.of(
                            "temporal-join-table-join-key-from-map",
                            "validates temporal join with a table when the join key comes from a map value")
                    .setupTableSource(ORDERS_WITH_NESTED_ID)
                    .setupTableSource(RATES)
                    .setupTableSink(AMOUNTS)
                    .runSql(
                            "INSERT INTO MySink "
                                    + "SELECT amount * r.rate "
                                    + "FROM OrdersNestedId AS o "
                                    + "JOIN RatesHistory FOR SYSTEM_TIME AS OF o.rowtime AS r "
                                    + "ON o.nested_map['currency'] = r.currency ")
                    .build();

    static final TableTestProgram TEMPORAL_JOIN_TEMPORAL_FUNCTION =
            TableTestProgram.of(
                            "temporal-join-temporal-function",
                            "validates temporal join with a temporal function")
                    .setupTableSource(ORDERS)
                    .setupTableSource(RATES)
                    .setupTemporarySystemTemporalTableFunction(
                            "Rates", "RatesHistory", $("rowtime"), $("currency"))
                    .setupTableSink(AMOUNTS)
                    .runSql(
                            "INSERT INTO MySink "
                                    + "SELECT amount * r.rate "
                                    + "FROM Orders AS o,  "
                                    + "LATERAL TABLE (Rates(o.rowtime)) AS r "
                                    + "WHERE o.currency = r.currency ")
                    .build();

    private static Map<String, String> mapOf(String key, String value) {
        final HashMap<String, String> map = new HashMap<>();
        map.put(key, value);
        return map;
    }
}
