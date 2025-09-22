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

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.LocalDateTime;

import static org.apache.flink.table.planner.plan.nodes.exec.stream.MultiJoinTestUtils.ORDERS_SOURCE;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.MultiJoinTestUtils.PAYMENTS_SOURCE;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.MultiJoinTestUtils.USERS_SOURCE;

/** {@link TableTestProgram} definitions for testing Multi-Join. */
public class MultiJoinTestPrograms {
    public static final TableTestProgram MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN =
            TableTestProgram.of("three-way-left-outer-join", "three way left outer join")
                    .setupTableSource(USERS_SOURCE)
                    .setupTableSource(ORDERS_SOURCE)
                    .setupTableSource(PAYMENTS_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING")
                                    .consumedValues(
                                            "+I[1, Gus, order1, payment1]",
                                            "+I[2, Bob, order2, payment2]",
                                            "+I[2, Bob, order3, payment2]",
                                            "+I[1, Gus, order1, payment3]",
                                            "+I[3, Alice, null, null]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                                    + "FROM Users u "
                                    + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                                    + "LEFT JOIN Payments p ON u.user_id = p.user_id")
                    .build();

    public static final TableTestProgram MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN_UPDATING =
            TableTestProgram.of(
                            "three-way-left-outer-join-updating",
                            "three way left outer join updating")
                    .setupTableSource(
                            SourceTestStep.newBuilder("Users")
                                    .addSchema(
                                            "user_id STRING PRIMARY KEY NOT ENFORCED, name STRING, cash INT")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "1", "Gus", 100),
                                            Row.ofKind(RowKind.INSERT, "2", "Bob", 200),
                                            Row.ofKind(RowKind.INSERT, "3", "Alice", 300),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "1", "Gus", 100),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER, "1", "Gus Updated", 100),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "1", "Gus Updated", 100),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "1", "Gus Updated", 0),
                                            Row.ofKind(RowKind.DELETE, "1", "Gus Updated", 0))
                                    .build())
                    .setupTableSource(ORDERS_SOURCE)
                    .setupTableSource(PAYMENTS_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING")
                                    .consumedValues(
                                            // After all updates and deletes, Gus should not be here
                                            // We only have Bob
                                            "+I[2, Bob, order2, payment2]",
                                            "+I[2, Bob, order3, payment2]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                                    + "FROM Users u "
                                    + "INNER JOIN Orders o ON u.user_id = o.user_id "
                                    + "LEFT JOIN Payments p ON u.user_id = p.user_id AND u.cash >= p.price")
                    .build();

    public static final TableTestProgram MULTI_JOIN_THREE_WAY_INNER_JOIN =
            TableTestProgram.of("three-way-inner-join", "three way inner join")
                    .setupTableSource(USERS_SOURCE)
                    .setupTableSource(ORDERS_SOURCE)
                    .setupTableSource(PAYMENTS_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING")
                                    .consumedValues(
                                            "+I[1, Gus, order1, payment1]",
                                            "+I[2, Bob, order2, payment2]",
                                            "+I[2, Bob, order3, payment2]",
                                            "+I[1, Gus, order1, payment3]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                                    + "FROM Users u "
                                    + "INNER JOIN Orders o ON u.user_id = o.user_id "
                                    + "INNER JOIN Payments p ON u.user_id = p.user_id")
                    .build();

    public static final TableTestProgram MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN_WITH_WHERE =
            TableTestProgram.of(
                            "three-way-inner-join-with-where",
                            "three way inner join with where clause")
                    .setupTableSource(USERS_SOURCE)
                    .setupTableSource(ORDERS_SOURCE)
                    .setupTableSource(PAYMENTS_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING")
                                    .consumedValues(
                                            "+I[1, Gus, order1, payment3]"
                                            // Most rows are filtered by WHERE u.name = 'Gus'
                                            // payment1 is filtered by WHERE p.price > 10
                                            )
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                                    + "FROM Users u "
                                    + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                                    + "LEFT JOIN Payments p ON u.user_id = p.user_id "
                                    + "WHERE u.name = 'Gus' AND p.price > 10")
                    .build();

    public static final TableTestProgram MULTI_JOIN_FOUR_WAY_COMPLEX =
            TableTestProgram.of("four-way-complex-updating-join", "four way complex updating join")
                    .setupTableSource(
                            SourceTestStep.newBuilder("Users")
                                    .addSchema(
                                            "name STRING",
                                            "cash INT",
                                            "user_id_0 STRING PRIMARY KEY NOT ENFORCED")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Gus", 100, "1"),
                                            Row.ofKind(RowKind.INSERT, "Joe no order", 10, "8"),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20, "2"),
                                            Row.ofKind(RowKind.INSERT, "Nomad", 50, "3"),
                                            Row.ofKind(RowKind.INSERT, "David", 5, "4"),
                                            Row.ofKind(RowKind.INSERT, "Eve", 0, "5"),
                                            Row.ofKind(RowKind.INSERT, "Frank", 70, "6"),
                                            Row.ofKind(RowKind.INSERT, "Welcher", 100, "7"),
                                            Row.ofKind(RowKind.INSERT, "Charlie Smith", 50, "9"),
                                            Row.ofKind(RowKind.DELETE, "Bob", 20, "2"),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    "Charlie Taylor",
                                                    50,
                                                    "9"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Orders")
                                    .addSchema(
                                            "order_id STRING PRIMARY KEY NOT ENFORCED",
                                            "product STRING",
                                            "user_id_1 STRING")
                                    .addOption("changelog-mode", "I,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "order0", "ProdB", "1"),
                                            Row.ofKind(RowKind.INSERT, "order6", "ProdF", "6"),
                                            Row.ofKind(RowKind.INSERT, "order1", "ProdA", "1"),
                                            Row.ofKind(RowKind.INSERT, "order2", "ProdB", "2"),
                                            Row.ofKind(RowKind.INSERT, "order3", "ProdC", "3"),
                                            Row.ofKind(RowKind.INSERT, "order4", "ProdD", "4"),
                                            Row.ofKind(RowKind.INSERT, "order7", "ProdG", "7"),
                                            Row.ofKind(RowKind.INSERT, "order9", "ProdA", "9"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Payments")
                                    .addSchema(
                                            "payment_id STRING PRIMARY KEY NOT ENFORCED",
                                            "price INT",
                                            "user_id_2 STRING")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "payment1", 50, "1"),
                                            Row.ofKind(RowKind.INSERT, "payment5", -1, "5"),
                                            Row.ofKind(RowKind.INSERT, "payment2", -5, "2"),
                                            Row.ofKind(RowKind.INSERT, "payment3", 30, "3"),
                                            Row.ofKind(RowKind.INSERT, "payment4", 40, "4"),
                                            Row.ofKind(RowKind.INSERT, "payment6", 60, "6"),
                                            Row.ofKind(RowKind.INSERT, "payment9", 30, "9"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Shipments")
                                    .addSchema("location STRING", "user_id_3 STRING")
                                    .addOption("changelog-mode", "I,UA,UB,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "London", "1"),
                                            Row.ofKind(RowKind.INSERT, "Paris", "2"),
                                            Row.ofKind(RowKind.INSERT, "Brasília", "3"),
                                            Row.ofKind(RowKind.INSERT, "New York", "3"),
                                            Row.ofKind(RowKind.INSERT, "Melbourne", "9"),
                                            Row.ofKind(RowKind.INSERT, "Lost Shipment", "10"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING",
                                            "location STRING")
                                    .addOption("changelog-mode", "I,UA,UB,D")
                                    .consumedValues(
                                            "+I[3, Nomad, order3, payment3, New York]",
                                            "+I[1, Gus, order0, payment1, London]",
                                            // NO +I[2, Bob...] here because Bob was deleted
                                            "+I[1, Gus, order1, payment1, London]",
                                            // NO +I[4, David...] here because David has only 5
                                            // dollars, so the following is invalid u.cash >=
                                            // p.price
                                            // +I[5, Eve...] here because payment price as -1
                                            "+I[5, Eve, null, payment5, null]",
                                            "+I[6, Frank, order6, payment6, null]",
                                            // NO +I[7, Welcher...] here since Welcher has no
                                            // payment
                                            // NO +I[8, Joe no order...] since no order and payment
                                            // +I[9, Charlie Taylor...] because the name was updated
                                            "+I[9, Charlie Taylor, order9, payment9, Melbourne]",
                                            // New +I[3, Nomad...] due to new location for user 3
                                            "+I[3, Nomad, order3, payment3, Brasília]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id_0, u.name, o.order_id, p.payment_id, s.location "
                                    + "FROM Users u "
                                    + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                                    + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2 AND (u.cash >= p.price OR p.price < 0) "
                                    + "LEFT JOIN Shipments s ON p.user_id_2 = s.user_id_3")
                    .build();

    public static final TableTestProgram MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN_WITH_RESTORE =
            TableTestProgram.of(
                            "three-way-left-outer-join-with-restore",
                            "three way left outer join with restore")
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("Users")
                                    .addSchema("user_id STRING", "name STRING")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "1", "Gus"),
                                            Row.ofKind(RowKind.INSERT, "2", "Bob"))
                                    .producedAfterRestore(Row.ofKind(RowKind.INSERT, "3", "Alice"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Orders")
                                    .addSchema("user_id STRING", "order_id STRING")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "1", "order1"),
                                            Row.ofKind(RowKind.INSERT, "2", "order2"))
                                    .producedAfterRestore(Row.ofKind(RowKind.INSERT, "2", "order3"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Payments")
                                    .addSchema("user_id STRING", "payment_id STRING")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "1", "payment1"),
                                            Row.ofKind(RowKind.INSERT, "2", "payment2"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "1", "payment3"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING")
                                    .consumedBeforeRestore(
                                            "+I[1, Gus, order1, payment1]",
                                            "+I[2, Bob, order2, payment2]")
                                    .consumedAfterRestore(
                                            "+I[2, Bob, order3, payment2]",
                                            "+I[1, Gus, order1, payment3]",
                                            "+I[3, Alice, null, null]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                                    + "FROM Users u "
                                    + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                                    + "LEFT JOIN Payments p ON u.user_id = p.user_id")
                    .build();

    public static final TableTestProgram MULTI_JOIN_THREE_WAY_INNER_JOIN_WITH_RESTORE =
            TableTestProgram.of(
                            "three-way-inner-join-with-restore",
                            "three way inner join with restore")
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("Users")
                                    .addSchema("user_id STRING", "name STRING")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "1", "Gus"),
                                            Row.ofKind(RowKind.INSERT, "2", "Bob"))
                                    .producedAfterRestore(Row.ofKind(RowKind.INSERT, "3", "Alice"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Orders")
                                    .addSchema("user_id STRING", "order_id STRING")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "1", "order1"),
                                            Row.ofKind(RowKind.INSERT, "2", "order2"))
                                    .producedAfterRestore(Row.ofKind(RowKind.INSERT, "2", "order3"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Payments")
                                    .addSchema("user_id STRING", "payment_id STRING")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "1", "payment1"),
                                            Row.ofKind(RowKind.INSERT, "2", "payment2"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "1", "payment3"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING")
                                    .consumedBeforeRestore(
                                            "+I[1, Gus, order1, payment1]",
                                            "+I[2, Bob, order2, payment2]")
                                    .consumedAfterRestore(
                                            "+I[2, Bob, order3, payment2]",
                                            "+I[1, Gus, order1, payment3]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                                    + "FROM Users u "
                                    + "INNER JOIN Orders o ON u.user_id = o.user_id "
                                    + "INNER JOIN Payments p ON u.user_id = p.user_id")
                    .build();

    public static final TableTestProgram
            MULTI_JOIN_THREE_WAY_INNER_JOIN_WITH_TTL_HINTS_WITH_RESTORE =
                    TableTestProgram.of(
                                    "three-way-inner-join-with-ttl-hints-with-restore",
                                    "three way inner join with restore and ttl hints")
                            .setupConfig(
                                    OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
                            .setupTableSource(
                                    SourceTestStep.newBuilder("Users")
                                            .addSchema("user_id STRING", "name STRING")
                                            .producedBeforeRestore(
                                                    Row.ofKind(RowKind.INSERT, "1", "Gus"),
                                                    Row.ofKind(RowKind.INSERT, "2", "Bob"))
                                            .producedAfterRestore(
                                                    Row.ofKind(RowKind.INSERT, "3", "Alice"))
                                            .build())
                            .setupTableSource(
                                    SourceTestStep.newBuilder("Orders")
                                            .addSchema("user_id STRING", "order_id STRING")
                                            .producedBeforeRestore(
                                                    Row.ofKind(RowKind.INSERT, "1", "order1"),
                                                    Row.ofKind(RowKind.INSERT, "2", "order2"))
                                            .producedAfterRestore(
                                                    Row.ofKind(RowKind.INSERT, "2", "order3"))
                                            .build())
                            .setupTableSource(
                                    SourceTestStep.newBuilder("Payments")
                                            .addSchema("user_id STRING", "payment_id STRING")
                                            .producedBeforeRestore(
                                                    Row.ofKind(RowKind.INSERT, "1", "payment1"),
                                                    Row.ofKind(RowKind.INSERT, "2", "payment2"))
                                            .producedAfterRestore(
                                                    Row.ofKind(RowKind.INSERT, "1", "payment3"))
                                            .build())
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink")
                                            .addSchema(
                                                    "user_id STRING",
                                                    "name STRING",
                                                    "order_id STRING",
                                                    "payment_id STRING")
                                            .consumedBeforeRestore(
                                                    "+I[1, Gus, order1, payment1]",
                                                    "+I[2, Bob, order2, payment2]")
                                            .consumedAfterRestore(
                                                    "+I[2, Bob, order3, payment2]",
                                                    "+I[1, Gus, order1, payment3]")
                                            .testMaterializedData()
                                            .build())
                            .runSql(
                                    "INSERT INTO sink "
                                            + "SELECT /*+ STATE_TTL('u' = '1d', 'p' = '2d') */u.user_id, u.name, o.order_id, p.payment_id "
                                            + "FROM Users u "
                                            + "INNER JOIN Orders o ON u.user_id = o.user_id "
                                            + "INNER JOIN Payments p ON u.user_id = p.user_id")
                            .build();

    public static final TableTestProgram MULTI_JOIN_THREE_WAY_INNER_JOIN_NO_JOIN_KEY =
            TableTestProgram.of(
                            "three-way-inner-join-no-join-key",
                            "three way inner join with no join key")
                    .setupTableSource(USERS_SOURCE)
                    .setupTableSource(ORDERS_SOURCE)
                    .setupTableSource(PAYMENTS_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING")
                                    .consumedValues(
                                            "+I[1, Gus, order1, payment2]",
                                            "+I[1, Gus, order1, payment5]",
                                            "+I[1, Gus, order1, payment3]",
                                            "+I[1, Gus, order1, payment1]",
                                            "+I[2, Bob, order1, payment2]",
                                            "+I[2, Bob, order1, payment5]",
                                            "+I[2, Bob, order1, payment3]",
                                            "+I[2, Bob, order1, payment1]",
                                            "+I[3, Alice, order1, payment2]",
                                            "+I[3, Alice, order1, payment5]",
                                            "+I[3, Alice, order1, payment3]",
                                            "+I[3, Alice, order1, payment1]",
                                            "+I[3, Alice, order2, payment2]",
                                            "+I[3, Alice, order2, payment5]",
                                            "+I[3, Alice, order2, payment3]",
                                            "+I[3, Alice, order2, payment1]",
                                            "+I[1, Gus, order2, payment2]",
                                            "+I[1, Gus, order2, payment5]",
                                            "+I[1, Gus, order2, payment3]",
                                            "+I[1, Gus, order2, payment1]",
                                            "+I[2, Bob, order2, payment2]",
                                            "+I[2, Bob, order2, payment5]",
                                            "+I[2, Bob, order2, payment3]",
                                            "+I[2, Bob, order2, payment1]",
                                            "+I[3, Alice, order3, payment2]",
                                            "+I[3, Alice, order3, payment5]",
                                            "+I[3, Alice, order3, payment3]",
                                            "+I[3, Alice, order3, payment1]",
                                            "+I[1, Gus, order3, payment2]",
                                            "+I[1, Gus, order3, payment5]",
                                            "+I[1, Gus, order3, payment3]",
                                            "+I[1, Gus, order3, payment1]",
                                            "+I[2, Bob, order3, payment2]",
                                            "+I[2, Bob, order3, payment5]",
                                            "+I[2, Bob, order3, payment3]",
                                            "+I[2, Bob, order3, payment1]",
                                            "+I[3, Alice, order4, payment2]",
                                            "+I[3, Alice, order4, payment5]",
                                            "+I[3, Alice, order4, payment3]",
                                            "+I[3, Alice, order4, payment1]",
                                            "+I[1, Gus, order4, payment2]",
                                            "+I[1, Gus, order4, payment5]",
                                            "+I[1, Gus, order4, payment3]",
                                            "+I[1, Gus, order4, payment1]",
                                            "+I[2, Bob, order4, payment2]",
                                            "+I[2, Bob, order4, payment5]",
                                            "+I[2, Bob, order4, payment3]",
                                            "+I[2, Bob, order4, payment1]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                                    + "FROM Users u "
                                    + "LEFT JOIN Orders o ON TRUE "
                                    + "INNER JOIN Payments p ON TRUE")
                    .build();

    public static final TableTestProgram MULTI_JOIN_FOUR_WAY_NO_COMMON_JOIN_KEY_RESTORE =
            TableTestProgram.of(
                            "four-way-join-no-common-join-key-with-restore",
                            "four way join no common join key with restore")
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("Users")
                                    .addSchema(
                                            "name STRING",
                                            "user_id_0 STRING PRIMARY KEY NOT ENFORCED",
                                            "cash INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "Gus", "1", 100),
                                            Row.ofKind(RowKind.INSERT, "Nomad", "3", 50),
                                            Row.ofKind(RowKind.INSERT, "Bob", "2", 20),
                                            Row.ofKind(RowKind.DELETE, "Bob", "2", 20))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "Frank", "6", 70),
                                            Row.ofKind(RowKind.INSERT, "David", "4", 5),
                                            Row.ofKind(RowKind.INSERT, "Joe no order", "8", 10),
                                            Row.ofKind(RowKind.INSERT, "Eve", "5", 0),
                                            Row.ofKind(RowKind.INSERT, "Welcher", "7", 100),
                                            Row.ofKind(RowKind.INSERT, "Charlie Smith", "9", 50),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    "Charlie Taylor",
                                                    "9",
                                                    50))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Orders")
                                    .addSchema(
                                            "order_id STRING PRIMARY KEY NOT ENFORCED",
                                            "product STRING",
                                            "user_id_1 STRING")
                                    .addOption("changelog-mode", "I,D")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "order2", "ProdB", "2"),
                                            Row.ofKind(RowKind.INSERT, "order0", "ProdB", "1"),
                                            Row.ofKind(RowKind.INSERT, "order3", "ProdC", "3"),
                                            Row.ofKind(RowKind.INSERT, "order1", "ProdA", "1"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "order6", "ProdF", "6"),
                                            Row.ofKind(RowKind.INSERT, "order4", "ProdD", "4"),
                                            Row.ofKind(RowKind.INSERT, "order9", "ProdA", "9"),
                                            Row.ofKind(RowKind.INSERT, "order7", "ProdG", "7"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Payments")
                                    .addSchema(
                                            "user_id_2 STRING",
                                            "payment_id STRING PRIMARY KEY NOT ENFORCED",
                                            "price INT")
                                    .addOption("changelog-mode", "I")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "3", "3", 30),
                                            Row.ofKind(RowKind.INSERT, "1", "1", 50),
                                            Row.ofKind(RowKind.INSERT, "2", "2", -5))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "6", "6", 60),
                                            Row.ofKind(RowKind.INSERT, "4", "4", 40),
                                            Row.ofKind(RowKind.INSERT, "9", "9", 30),
                                            Row.ofKind(RowKind.INSERT, "5", "5", -1))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Shipments")
                                    .addSchema("location STRING", "user_id_3 STRING")
                                    .addOption("changelog-mode", "I,UA,UB,D")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "Paris", "2"),
                                            Row.ofKind(RowKind.INSERT, "London", "1"),
                                            Row.ofKind(RowKind.INSERT, "New York", "3"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "Brasília", "3"),
                                            Row.ofKind(RowKind.INSERT, "Melbourne", "9"),
                                            Row.ofKind(RowKind.INSERT, "10", "Lost Shipment"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING",
                                            "location STRING")
                                    .addOption("sink-changelog-mode-enforced", "I,UA,UB,D")
                                    .consumedBeforeRestore(
                                            "+I[1, Gus, order0, 1, London]",
                                            "+I[1, Gus, order1, 1, London]",
                                            "+I[3, Nomad, order3, 3, New York]")
                                    .testMaterializedData()
                                    .consumedAfterRestore(
                                            "+I[5, Eve, null, 5, null]",
                                            "+I[6, Frank, order6, 6, null]",
                                            "+I[9, Charlie Taylor, order9, 9, Melbourne]",
                                            "+I[3, Nomad, order3, 3, Brasília]",
                                            "+I[4, David, order4, 4, null]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id_0, u.name, o.order_id, p.payment_id, s.location "
                                    + "FROM Users u "
                                    + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                                    + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2 "
                                    + "LEFT JOIN Shipments s ON p.payment_id = s.user_id_3")
                    .build();

    public static final TableTestProgram MULTI_JOIN_FOUR_WAY_NO_COMMON_JOIN_KEY =
            TableTestProgram.of(
                            "four-way-no-common-join-key-updating-join",
                            "four way no common join key updating join")
                    .setupTableSource(
                            SourceTestStep.newBuilder("Users")
                                    .addSchema(
                                            "name STRING",
                                            "cash INT",
                                            "user_id_0 STRING PRIMARY KEY NOT ENFORCED")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Gus", 100, "1"),
                                            Row.ofKind(RowKind.INSERT, "Joe no order", 10, "8"),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20, "2"),
                                            Row.ofKind(RowKind.INSERT, "Nomad", 50, "3"),
                                            Row.ofKind(RowKind.INSERT, "David", 5, "4"),
                                            Row.ofKind(RowKind.INSERT, "Eve", 0, "5"),
                                            Row.ofKind(RowKind.INSERT, "Frank", 70, "6"),
                                            Row.ofKind(RowKind.INSERT, "Welcher", 100, "7"),
                                            Row.ofKind(RowKind.INSERT, "Charlie Smith", 50, "9"),
                                            Row.ofKind(RowKind.DELETE, "Bob", 20, "2"),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    "Charlie Taylor",
                                                    50,
                                                    "9"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Orders")
                                    .addSchema(
                                            "order_id STRING PRIMARY KEY NOT ENFORCED",
                                            "product STRING",
                                            "user_id_1 STRING")
                                    .addOption("changelog-mode", "I,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "order0", "ProdB", "1"),
                                            Row.ofKind(RowKind.INSERT, "order6", "ProdF", "6"),
                                            Row.ofKind(RowKind.INSERT, "order1", "ProdA", "1"),
                                            Row.ofKind(RowKind.INSERT, "order2", "ProdB", "2"),
                                            Row.ofKind(RowKind.INSERT, "order3", "ProdC", "3"),
                                            Row.ofKind(RowKind.INSERT, "order4", "ProdD", "4"),
                                            Row.ofKind(RowKind.INSERT, "order7", "ProdG", "7"),
                                            Row.ofKind(RowKind.INSERT, "order9", "ProdA", "9"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Payments")
                                    .addSchema(
                                            "payment_id STRING PRIMARY KEY NOT ENFORCED",
                                            "price INT",
                                            "user_id_2 STRING")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "1", 50, "1"),
                                            Row.ofKind(RowKind.INSERT, "5", -1, "5"),
                                            Row.ofKind(RowKind.INSERT, "2", -5, "2"),
                                            Row.ofKind(RowKind.INSERT, "3", 30, "3"),
                                            Row.ofKind(RowKind.INSERT, "4", 40, "4"),
                                            Row.ofKind(RowKind.INSERT, "6", 60, "6"),
                                            Row.ofKind(RowKind.INSERT, "9", 30, "9"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Shipments")
                                    .addSchema("location STRING", "user_id_3 STRING")
                                    .addOption("changelog-mode", "I,UA,UB,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "London", "1"),
                                            Row.ofKind(RowKind.INSERT, "Paris", "2"),
                                            Row.ofKind(RowKind.INSERT, "Brasília", "3"),
                                            Row.ofKind(RowKind.INSERT, "New York", "3"),
                                            Row.ofKind(RowKind.INSERT, "Melbourne", "9"),
                                            Row.ofKind(RowKind.INSERT, "Lost Shipment", "10"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING",
                                            "location STRING")
                                    .addOption("sink-changelog-mode-enforced", "I,UA,UB,D")
                                    .consumedValues(
                                            "+I[1, Gus, order0, 1, London]",
                                            "+I[1, Gus, order1, 1, London]",
                                            "+I[3, Nomad, order3, 3, Brasília]",
                                            "+I[3, Nomad, order3, 3, New York]",
                                            "+I[4, David, order4, 4, null]",
                                            "+I[5, Eve, null, 5, null]",
                                            "+I[6, Frank, order6, 6, null]",
                                            "+I[9, Charlie Taylor, order9, 9, Melbourne]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id_0, u.name, o.order_id, p.payment_id, s.location "
                                    + "FROM Users u "
                                    + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                                    + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2 "
                                    + "LEFT JOIN Shipments s ON p.payment_id = s.user_id_3")
                    .build();

    public static final TableTestProgram MULTI_JOIN_FOUR_WAY_COMPLEX_WITH_RESTORE =
            TableTestProgram.of(
                            "four-way-complex-updating-join-with-restore",
                            "four way complex updating join with restore")
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("Users")
                                    .addSchema(
                                            "name STRING",
                                            "user_id_0 STRING PRIMARY KEY NOT ENFORCED",
                                            "cash INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "Gus", "1", 100),
                                            Row.ofKind(RowKind.INSERT, "Nomad", "3", 50),
                                            Row.ofKind(RowKind.INSERT, "Bob", "2", 20),
                                            Row.ofKind(RowKind.DELETE, "Bob", "2", 20))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "Frank", "6", 70),
                                            Row.ofKind(RowKind.INSERT, "David", "4", 5),
                                            Row.ofKind(RowKind.INSERT, "Joe no order", "8", 10),
                                            Row.ofKind(RowKind.INSERT, "Eve", "5", 0),
                                            Row.ofKind(RowKind.INSERT, "Welcher", "7", 100),
                                            Row.ofKind(RowKind.INSERT, "Charlie Smith", "9", 50),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    "Charlie Taylor",
                                                    "9",
                                                    50))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Orders")
                                    .addSchema(
                                            "order_id STRING PRIMARY KEY NOT ENFORCED",
                                            "product STRING",
                                            "user_id_1 STRING")
                                    .addOption("changelog-mode", "I,D")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "order2", "ProdB", "2"),
                                            Row.ofKind(RowKind.INSERT, "order0", "ProdB", "1"),
                                            Row.ofKind(RowKind.INSERT, "order3", "ProdC", "3"),
                                            Row.ofKind(RowKind.INSERT, "order1", "ProdA", "1"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "order6", "ProdF", "6"),
                                            Row.ofKind(RowKind.INSERT, "order4", "ProdD", "4"),
                                            Row.ofKind(RowKind.INSERT, "order9", "ProdA", "9"),
                                            Row.ofKind(RowKind.INSERT, "order7", "ProdG", "7"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Payments")
                                    .addSchema(
                                            "user_id_2 STRING",
                                            "payment_id STRING PRIMARY KEY NOT ENFORCED",
                                            "price INT")
                                    .addOption("changelog-mode", "I")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "3", "payment3", 30),
                                            Row.ofKind(RowKind.INSERT, "1", "payment1", 50),
                                            Row.ofKind(RowKind.INSERT, "2", "payment2", -5))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "6", "payment6", 60),
                                            Row.ofKind(RowKind.INSERT, "4", "payment4", 40),
                                            Row.ofKind(RowKind.INSERT, "9", "payment9", 30),
                                            Row.ofKind(RowKind.INSERT, "5", "payment5", -1))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Shipments")
                                    .addSchema("location STRING", "user_id_3 STRING")
                                    .addOption("changelog-mode", "I,UA,UB,D")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "Paris", "2"),
                                            Row.ofKind(RowKind.INSERT, "London", "1"),
                                            Row.ofKind(RowKind.INSERT, "New York", "3"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "Brasília", "3"),
                                            Row.ofKind(RowKind.INSERT, "Melbourne", "9"),
                                            Row.ofKind(RowKind.INSERT, "10", "Lost Shipment"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING",
                                            "location STRING")
                                    .addOption("sink-changelog-mode-enforced", "I,UA,UB,D")
                                    .consumedBeforeRestore(
                                            "+I[1, Gus, order0, payment1, London]",
                                            "+I[1, Gus, order1, payment1, London]",
                                            "+I[3, Nomad, order3, payment3, New York]")
                                    .testMaterializedData()
                                    .consumedAfterRestore(
                                            "+I[5, Eve, null, payment5, null]",
                                            "+I[6, Frank, order6, payment6, null]",
                                            "+I[9, Charlie Taylor, order9, payment9, Melbourne]",
                                            "+I[3, Nomad, order3, payment3, Brasília]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id_0, u.name, o.order_id, p.payment_id, s.location "
                                    + "FROM Users u "
                                    + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                                    + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2 AND (u.cash >= p.price OR p.price < 0) "
                                    + "LEFT JOIN Shipments s ON p.user_id_2 = s.user_id_3")
                    .build();

    public static final TableTestProgram
            MULTI_JOIN_FOUR_WAY_COMPLEX_PRESERVES_UPSERT_KEY_WITH_RESTORE =
                    TableTestProgram.of(
                                    "four-way-complex-preserves-upsert-key-with-restore",
                                    "four way complex preserves upsert key with restore")
                            .setupConfig(
                                    OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
                            .setupTableSource(
                                    SourceTestStep.newBuilder("Users")
                                            .addSchema(
                                                    "user_id_0 STRING PRIMARY KEY NOT ENFORCED",
                                                    "name STRING")
                                            .producedBeforeRestore(
                                                    Row.ofKind(RowKind.INSERT, "1", "Gus"),
                                                    Row.ofKind(RowKind.INSERT, "3", "Joe"))
                                            .producedAfterRestore(
                                                    Row.ofKind(RowKind.INSERT, "2", "Bob"))
                                            .build())
                            .setupTableSource(
                                    SourceTestStep.newBuilder("Orders")
                                            .addSchema(
                                                    "order_id STRING NOT NULL",
                                                    "user_id_1 STRING NOT NULL",
                                                    "product STRING",
                                                    "CONSTRAINT `PRIMARY` PRIMARY KEY (`order_id`, `user_id_1`) NOT ENFORCED")
                                            .addOption("changelog-mode", "I")
                                            .producedBeforeRestore(
                                                    Row.ofKind(
                                                            RowKind.INSERT, "order1", "1", "ProdA"))
                                            .producedAfterRestore(
                                                    Row.ofKind(
                                                            RowKind.INSERT, "order2", "2", "ProdB"))
                                            .build())
                            .setupTableSource(
                                    SourceTestStep.newBuilder("Payments")
                                            .addSchema(
                                                    "payment_id STRING NOT NULL",
                                                    "user_id_2 STRING NOT NULL",
                                                    "price INT",
                                                    "CONSTRAINT `PRIMARY` PRIMARY KEY (`payment_id`, `user_id_2`) NOT ENFORCED")
                                            .addOption("changelog-mode", "I")
                                            .producedBeforeRestore(
                                                    Row.ofKind(
                                                            RowKind.INSERT, "payment1", "1", 100),
                                                    Row.ofKind(
                                                            RowKind.INSERT, "payment3", "3", 300))
                                            .producedAfterRestore(
                                                    Row.ofKind(
                                                            RowKind.INSERT, "payment2", "2", 200))
                                            .build())
                            .setupTableSource(
                                    SourceTestStep.newBuilder("Address")
                                            .addSchema(
                                                    "user_id_3 STRING NOT NULL",
                                                    "location STRING",
                                                    "CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id_3`) NOT ENFORCED")
                                            .addOption("changelog-mode", "I")
                                            .producedBeforeRestore(
                                                    Row.ofKind(RowKind.INSERT, "1", "London"),
                                                    Row.ofKind(RowKind.INSERT, "3", "Berlin"))
                                            .producedAfterRestore(
                                                    Row.ofKind(RowKind.INSERT, "2", "Paris"))
                                            .build())
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink")
                                            .addOption("changelog-mode", "I, UA, D")
                                            .addSchema(
                                                    "user_id_0 STRING NOT NULL",
                                                    "order_id STRING NOT NULL",
                                                    "user_id_1 STRING NOT NULL",
                                                    "payment_id STRING NOT NULL",
                                                    "user_id_2 STRING NOT NULL",
                                                    "name STRING",
                                                    "location STRING",
                                                    "CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id_0`, `order_id`, `user_id_1`, `payment_id`, `user_id_2`) NOT ENFORCED")
                                            .consumedBeforeRestore(
                                                    "+I[1, order1, 1, payment1, 1, Gus, London]")
                                            .consumedAfterRestore(
                                                    "+I[2, order2, 2, payment2, 2, Bob, Paris]")
                                            .testMaterializedData()
                                            .build())
                            .runSql(
                                    "INSERT INTO `sink`\n"
                                            + "SELECT\n"
                                            + "    u.user_id_0,\n"
                                            + "    o.order_id,\n"
                                            + "    o.user_id_1,\n"
                                            + "    p.payment_id,\n"
                                            + "    p.user_id_2,\n"
                                            + "    u.name,\n"
                                            + "    a.location\n"
                                            + "FROM Users u\n"
                                            + "JOIN Orders o\n"
                                            + "  ON  u.user_id_0 = o.user_id_1 AND o.product IS NOT NULL\n"
                                            + "JOIN Payments p\n"
                                            + "  ON  u.user_id_0 = p.user_id_2 AND p.price >= 0\n"
                                            + "JOIN Address a\n"
                                            + "  ON  u.user_id_0 = a.user_id_3 AND a.location IS NOT NULL")
                            .build();

    public static final TableTestProgram MULTI_JOIN_WITH_TIME_ATTRIBUTES_MATERIALIZATION =
            TableTestProgram.of(
                            "three-way-join-with-time-attributes",
                            "three way join with time attributes materialization")
                    .setupTableSource(
                            SourceTestStep.newBuilder("UsersWithProctime")
                                    .addSchema(
                                            "user_id_0 STRING PRIMARY KEY NOT ENFORCED,"
                                                    + " name STRING,"
                                                    + " proctime AS PROCTIME()")
                                    .producedValues(Row.ofKind(RowKind.INSERT, "1", "Gus"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("OrdersWithRowtime")
                                    .addSchema(
                                            "order_id STRING PRIMARY KEY NOT ENFORCED,"
                                                    + "user_id_1 STRING,"
                                                    + "rowtime TIMESTAMP(3),"
                                                    + "WATERMARK FOR rowtime AS rowtime")
                                    .producedValues(
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    "order1",
                                                    "1",
                                                    LocalDateTime.parse("2024-01-01T12:00:00.123")))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Payments")
                                    .addSchema(
                                            "payment_id STRING PRIMARY KEY NOT ENFORCED,"
                                                    + "price INT,"
                                                    + "user_id_2 STRING")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "payment1", 100, "1"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "name STRING",
                                            "order_id STRING",
                                            "rowtime TIMESTAMP(3)",
                                            "price INT")
                                    .consumedValues("+I[Gus, order1, 2024-01-01T12:00:00.123, 100]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.name, o.order_id, o.rowtime, p.price "
                                    + "FROM UsersWithProctime u "
                                    + "INNER JOIN OrdersWithRowtime o ON u.user_id_0 = o.user_id_1 "
                                    + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2")
                    .build();

    public static final TableTestProgram
            MULTI_JOIN_WITH_TIME_ATTRIBUTES_MATERIALIZATION_WITH_RESTORE =
                    TableTestProgram.of(
                                    "three-way-join-with-time-attributes-with-restore",
                                    "three way join with time attributes materialization with restore")
                            .setupConfig(
                                    OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
                            .setupTableSource(
                                    SourceTestStep.newBuilder("UsersWithProctime")
                                            .addSchema(
                                                    "user_id_0 STRING PRIMARY KEY NOT ENFORCED,"
                                                            + " name STRING,"
                                                            + " proctime AS PROCTIME()")
                                            .producedBeforeRestore(
                                                    Row.ofKind(RowKind.INSERT, "1", "Gus"))
                                            .producedAfterRestore(
                                                    Row.ofKind(RowKind.INSERT, "2", "Bob"))
                                            .build())
                            .setupTableSource(
                                    SourceTestStep.newBuilder("OrdersWithRowtime")
                                            .addSchema(
                                                    "order_id STRING PRIMARY KEY NOT ENFORCED,"
                                                            + "user_id_1 STRING,"
                                                            + "rowtime TIMESTAMP(3),"
                                                            + "WATERMARK FOR rowtime AS rowtime")
                                            .producedBeforeRestore(
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            "order1",
                                                            "1",
                                                            LocalDateTime.parse(
                                                                    "2024-01-01T12:00:00.123")))
                                            .producedAfterRestore(
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            "order2",
                                                            "2",
                                                            LocalDateTime.parse(
                                                                    "2024-01-01T12:00:01.456")))
                                            .build())
                            .setupTableSource(
                                    SourceTestStep.newBuilder("Payments")
                                            .addSchema(
                                                    "payment_id STRING PRIMARY KEY NOT ENFORCED,"
                                                            + "price INT,"
                                                            + "user_id_2 STRING")
                                            .producedBeforeRestore(
                                                    Row.ofKind(
                                                            RowKind.INSERT, "payment1", 100, "1"))
                                            .producedAfterRestore(
                                                    Row.ofKind(
                                                            RowKind.INSERT, "payment2", 200, "2"))
                                            .build())
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink")
                                            .addSchema(
                                                    "name STRING",
                                                    "order_id STRING",
                                                    "rowtime TIMESTAMP(3)",
                                                    "price INT")
                                            .consumedBeforeRestore(
                                                    "+I[Gus, order1, 2024-01-01T12:00:00.123, 100]")
                                            .consumedAfterRestore(
                                                    "+I[Bob, order2, 2024-01-01T12:00:01.456, 200]")
                                            .testMaterializedData()
                                            .build())
                            .runSql(
                                    "INSERT INTO sink "
                                            + "SELECT u.name, o.order_id, o.rowtime, p.price "
                                            + "FROM UsersWithProctime u "
                                            + "INNER JOIN OrdersWithRowtime o ON u.user_id_0 = o.user_id_1 "
                                            + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2")
                            .build();

    public static final TableTestProgram MULTI_JOIN_MIXED_CHANGELOG_MODES =
            TableTestProgram.of(
                            "three-way-mixed-changelog-modes",
                            "three way join with mixed changelog modes and primary key configurations")
                    .setupTableSource(
                            SourceTestStep.newBuilder("AppendTable")
                                    .addSchema("id STRING PRIMARY KEY NOT ENFORCED, val STRING")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "1", "append1"),
                                            Row.ofKind(RowKind.INSERT, "2", "append2"),
                                            Row.ofKind(RowKind.INSERT, "3", "append3"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("RetractTable")
                                    .addSchema("ref_id STRING, data STRING")
                                    .addOption("changelog-mode", "I,UA,UB,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "1", "retract1"),
                                            Row.ofKind(RowKind.INSERT, "2", "retract2"),
                                            Row.ofKind(RowKind.INSERT, "3", "retract3"),
                                            Row.ofKind(RowKind.DELETE, "3", "retract3"),
                                            Row.ofKind(RowKind.INSERT, "1", "retract1_new"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("UpsertTable")
                                    .addSchema(
                                            "key_id STRING PRIMARY KEY NOT ENFORCED, status STRING")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "1", "active"),
                                            Row.ofKind(RowKind.INSERT, "2", "pending"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "2", "active"),
                                            Row.ofKind(RowKind.INSERT, "3", "inactive"),
                                            Row.ofKind(RowKind.DELETE, "3", "inactive"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "id STRING",
                                            "val STRING",
                                            "data STRING",
                                            "status STRING")
                                    .addOption("sink-changelog-mode-enforced", "I,UA,UB,D")
                                    .consumedValues(
                                            "+I[1, append1, retract1, active]",
                                            "+I[2, append2, retract2, active]",
                                            "+I[1, append1, retract1_new, active]",
                                            "+I[3, append3, null, null]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT a.id, a.val, r.data, u.status "
                                    + "FROM AppendTable a "
                                    + "LEFT JOIN RetractTable r ON a.id = r.ref_id "
                                    + "LEFT JOIN UpsertTable u ON a.id = u.key_id")
                    .build();

    public static final TableTestProgram MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN_WITH_CTE =
            TableTestProgram.of(
                            "left-outer-join-with-cte",
                            "CTE with three-way left outer join and aggregation")
                    .setupTableSource(USERS_SOURCE)
                    .setupTableSource(
                            SourceTestStep.newBuilder("Orders")
                                    .addSchema(
                                            "user_id STRING",
                                            "order_id STRING PRIMARY KEY NOT ENFORCED",
                                            "product STRING")
                                    .addOption("changelog-mode", "I, UA,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "2", "order2", "Product B"),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    "2",
                                                    "order2",
                                                    "Product B"),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    "2",
                                                    "order2",
                                                    "Product C"),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    "2",
                                                    "order2",
                                                    "Product C"))
                                    .build())
                    .setupTableSource(PAYMENTS_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("user_id STRING", "name STRING", "cnt BIGINT")
                                    .testMaterializedData()
                                    .consumedValues(Row.of("1", "Gus", 2), Row.of("2", "Bob", 1))
                                    .build())
                    .runSql(
                            "INSERT INTO sink WITH "
                                    + "    order_details AS ( "
                                    + "        SELECT o.user_id "
                                    + "        FROM Orders o "
                                    + "    ), "
                                    + "    user_elements AS ( "
                                    + "        SELECT "
                                    + "            u.id AS user_id "
                                    + "        FROM ( "
                                    + "          SELECT '2' AS id, '2' AS order_user_id "
                                    + "          UNION ALL "
                                    + "          SELECT '1' AS id, '2' AS order_user_id "
                                    + "          UNION ALL "
                                    + "          SELECT '5' AS id, '5' AS order_user_id "
                                    + "        ) u "
                                    + "        LEFT JOIN order_details od "
                                    + "            ON od.user_id = u.order_user_id "
                                    + "    ) "
                                    + "SELECT ue.user_id, us.name, COUNT(*) AS cnt "
                                    + "FROM user_elements ue "
                                    + "INNER JOIN Users us ON ue.user_id = us.user_id "
                                    + "LEFT JOIN Payments p ON ue.user_id = p.user_id "
                                    + "GROUP BY ue.user_id, us.name")
                    .build();

    public static final TableTestProgram MULTI_JOIN_LEFT_OUTER_WITH_NULL_KEYS =
            TableTestProgram.of(
                            "three-way-left-outer-with-null-keys",
                            "left outer join with NULL keys on multiple inputs")
                    .setupSql(
                            "CREATE VIEW UsersNulls AS SELECT * FROM (VALUES "
                                    + "('1','Gus'),"
                                    + "(CAST(NULL AS STRING), 'NullUser')"
                                    + ") AS T(user_id, name)")
                    .setupSql(
                            "CREATE VIEW OrdersNulls AS SELECT * FROM (VALUES "
                                    + "('1','order1'),"
                                    + "(CAST(NULL AS STRING), 'nullOrder')"
                                    + ") AS T(user_id, order_id)")
                    .setupSql(
                            "CREATE VIEW PaymentsNulls AS SELECT * FROM (VALUES "
                                    + "('1','payment1'),"
                                    + "('1','payment3'),"
                                    + "(CAST(NULL AS STRING), 'paymentNull')"
                                    + ") AS T(user_id, payment_id)")
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_id STRING",
                                            "name STRING",
                                            "order_id STRING",
                                            "payment_id STRING")
                                    .consumedValues(
                                            "+I[1, Gus, order1, payment1]",
                                            "+I[1, Gus, order1, payment3]",
                                            "+I[null, NullUser, null, null]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                                    + "FROM UsersNulls u "
                                    + "LEFT JOIN OrdersNulls o ON u.user_id = o.user_id "
                                    + "LEFT JOIN PaymentsNulls p ON u.user_id = p.user_id")
                    .build();

    public static final TableTestProgram MULTI_JOIN_NULL_SAFE_JOIN_WITH_NULL_KEYS =
            TableTestProgram.of(
                            "null-safe-join-with-null-keys",
                            "join with IS NOT DISTINCT FROM to match NULL keys")
                    .setupSql(
                            "CREATE VIEW UsersNullSafe AS SELECT * FROM (VALUES "
                                    + "('1','Gus'),"
                                    + "(CAST(NULL AS STRING), 'NullUser')"
                                    + ") AS T(user_id, name)")
                    .setupSql(
                            "CREATE VIEW OrdersNullSafe AS SELECT * FROM (VALUES "
                                    + "('1','order1'),"
                                    + "(CAST(NULL AS STRING), 'nullOrder')"
                                    + ") AS T(user_id, order_id)")
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("user_id STRING", "name STRING", "order_id STRING")
                                    .consumedValues(
                                            "+I[1, Gus, order1]", "+I[null, NullUser, nullOrder]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT u.user_id, u.name, o.order_id "
                                    + "FROM UsersNullSafe u "
                                    + "INNER JOIN OrdersNullSafe o ON u.user_id IS NOT DISTINCT FROM o.user_id")
                    .build();

    static final TableTestProgram MULTI_JOIN_TWO_WAY_LEFT_JOIN_PRESERVES_UPSERT_KEY_WITH_RESTORE =
            TableTestProgram.of(
                            "two-way-left-join-preserves-upsert-key-with-restore",
                            "validates upsert with non key filter with restore")
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("Users")
                                    .addOption("changelog-mode", "I, UA,D")
                                    .addSchema(
                                            "user_id INT NOT NULL",
                                            "shard_id INT NOT NULL",
                                            "description STRING",
                                            "CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`) NOT ENFORCED")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, 1, 1, "shard_a"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.UPDATE_AFTER, 3, 1, "another shard"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Orders")
                                    .addOption("changelog-mode", "I, UA,D")
                                    .addSchema(
                                            "user_id INT NOT NULL",
                                            "order_id BIGINT NOT NULL",
                                            "product STRING",
                                            "CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `order_id`) NOT ENFORCED")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, 1, 1L, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, 1L, "a_updated"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, 1, 2L, "b"),
                                            Row.ofKind(RowKind.DELETE, 1, 2L, "b"),
                                            Row.ofKind(RowKind.INSERT, 3, 1L, "b"),
                                            Row.ofKind(RowKind.INSERT, 9, 9L, "b"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addOption("changelog-mode", "I, UA, D")
                                    .addSchema(
                                            "`user_id` INT NOT NULL",
                                            "`order_id` BIGINT NOT NULL",
                                            "product STRING",
                                            "user_shard_id INT",
                                            "CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `order_id`) NOT ENFORCED")
                                    .consumedBeforeRestore("+I[1, 1, a_updated, 1]")
                                    .consumedAfterRestore("+I[3, 1, b, 1]", "+I[9, 9, b, null]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO `sink`\n"
                                    + "SELECT\n"
                                    + "    o.user_id,\n"
                                    + "    o.order_id,\n"
                                    + "    o.product,\n"
                                    + "    u.shard_id\n"
                                    + "FROM Orders o\n"
                                    + "LEFT JOIN Users u\n"
                                    + "  ON  u.user_id = o.user_id\n")
                    .build();

    static final TableTestProgram MULTI_JOIN_THREE_WAY_JOIN_PRESERVES_UPSERT_KEY_WITH_RESTORE =
            TableTestProgram.of(
                            "three-way-upsert-preserves-key-with-restore",
                            "validates upsert with non key filter with restore")
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("Users")
                                    .addOption("changelog-mode", "I, UA,D")
                                    .addSchema(
                                            "user_id INT NOT NULL",
                                            "shard_id INT NOT NULL",
                                            "description STRING",
                                            "CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`) NOT ENFORCED")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, 1, 1, "shard_a"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, 2, 1, "shard_b"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Orders")
                                    .addOption("changelog-mode", "I, UA,D")
                                    .addSchema(
                                            "user_id INT NOT NULL",
                                            "order_id BIGINT NOT NULL",
                                            "product STRING",
                                            "CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `order_id`) NOT ENFORCED")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, 1, 1L, "a"),
                                            Row.ofKind(RowKind.INSERT, 1, 2L, "b"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, 2, 1L, "c"),
                                            Row.ofKind(RowKind.INSERT, 2, 2L, "d"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("Payments")
                                    .addOption("changelog-mode", "I, UA,D")
                                    .addSchema(
                                            "user_id INT NOT NULL",
                                            "payment_id BIGINT NOT NULL",
                                            "product STRING",
                                            "CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `payment_id`) NOT ENFORCED")
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, 1, 1L, "a"),
                                            Row.ofKind(RowKind.INSERT, 1, 2L, "b"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, 2, 1L, "c"),
                                            Row.ofKind(RowKind.INSERT, 2, 2L, "d"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addOption("changelog-mode", "I, UA, D")
                                    .addSchema(
                                            "`user_id` INT NOT NULL",
                                            "`order_id` BIGINT NOT NULL",
                                            "`user_id2` INT NOT NULL",
                                            "payment_id BIGINT NOT NULL",
                                            "`user_id3` INT NOT NULL",
                                            "description STRING",
                                            "CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `order_id`, `user_id2`, `payment_id`, `user_id3`) NOT ENFORCED")
                                    .consumedBeforeRestore(
                                            "+I[1, 1, 1, 2, 1, shard_a]",
                                            "+I[1, 1, 1, 1, 1, shard_a]",
                                            "+I[1, 2, 1, 1, 1, shard_a]",
                                            "+I[1, 2, 1, 2, 1, shard_a]")
                                    .consumedAfterRestore(
                                            "+I[2, 2, 2, 1, 2, shard_b]",
                                            "+I[2, 1, 2, 2, 2, shard_b]",
                                            "+I[2, 2, 2, 2, 2, shard_b]",
                                            "+I[2, 1, 2, 1, 2, shard_b]")
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO `sink`\n"
                                    + "SELECT\n"
                                    + "    o.user_id,\n"
                                    + "    o.order_id,\n"
                                    + "    p.user_id,\n"
                                    + "    p.payment_id,\n"
                                    + "    u.user_id,\n"
                                    + "    u.description\n"
                                    + "FROM Users u\n"
                                    + "JOIN `Orders` AS o\n"
                                    + "  ON  o.user_id = u.user_id\n"
                                    + "JOIN Payments p\n"
                                    + "  ON  o.user_id = p.user_id")
                    .build();

    public static final TableTestProgram
            MULTI_JOIN_WITH_TIME_ATTRIBUTES_IN_CONDITIONS_MATERIALIZATION =
                    TableTestProgram.of(
                                    "three-way-join-with-time-attributes-in-join-conditions",
                                    "A query from the nexmark benchmark: "
                                            + "auction and bid aggregation with time-based filtering")
                            .setupTableSource(
                                    SourceTestStep.newBuilder("auctions")
                                            .addSchema(
                                                    "id BIGINT PRIMARY KEY NOT ENFORCED",
                                                    "category STRING",
                                                    "auctionTimestamp STRING",
                                                    "expiresTimestamp STRING",
                                                    "auctionDateTime AS TO_TIMESTAMP(auctionTimestamp)",
                                                    "expires AS TO_TIMESTAMP(expiresTimestamp)",
                                                    "WATERMARK FOR auctionDateTime AS auctionDateTime - INTERVAL '1' SECOND")
                                            .addOption("changelog-mode", "I")
                                            .producedValues(
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            1L,
                                                            "Electronics",
                                                            "2024-01-01 12:00:00",
                                                            "2024-01-01 12:30:00"),
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            3L,
                                                            "Electronics",
                                                            "2024-01-01 12:10:00",
                                                            "2024-01-01 12:40:00"),
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            2L,
                                                            "Books",
                                                            "2024-01-01 12:05:00",
                                                            "2024-01-01 12:35:00"))
                                            .build())
                            .setupTableSource(
                                    SourceTestStep.newBuilder("bids")
                                            .addSchema(
                                                    "auction BIGINT",
                                                    "price DOUBLE",
                                                    "bidTimestamp STRING",
                                                    "bidDateTime AS TO_TIMESTAMP(bidTimestamp)",
                                                    "WATERMARK FOR bidDateTime AS bidDateTime - INTERVAL '1' SECOND")
                                            .addOption("changelog-mode", "I")
                                            .producedValues(
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            1L,
                                                            12.0,
                                                            "2024-01-01 12:15:00"),
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            1L,
                                                            15.0,
                                                            "2024-01-01 12:20:00"),
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            2L,
                                                            25.0,
                                                            "2024-01-01 12:25:00"),
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            3L,
                                                            18.0,
                                                            "2024-01-01 12:30:00"),
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            1L,
                                                            20.0,
                                                            "2024-01-01 12:45:00"))
                                            .build())
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink")
                                            .addSchema("category STRING", "avg_final DOUBLE")
                                            .consumedValues(
                                                    // Electronics: AVG(MAX(12.0, 15.0), MAX(18.0))
                                                    // =
                                                    // AVG(15.0, 18.0) = 16.5
                                                    "+I[Electronics, 16.5]",
                                                    // Books: MAX(25.0) = 25.0, AVG(25.0) = 25.0
                                                    "+I[Books, 25.0]")
                                            .testMaterializedData()
                                            .build())
                            .runSql(
                                    "INSERT INTO sink "
                                            + "SELECT Q.category, AVG(Q.final) "
                                            + "FROM ( "
                                            + "    SELECT MAX(B.price) AS final, A.category "
                                            + "    FROM auctions A, bids B "
                                            + "    WHERE A.id = B.auction AND B.bidDateTime BETWEEN A.auctionDateTime AND A.expires "
                                            + "    GROUP BY A.id, A.category "
                                            + ") Q "
                                            + "GROUP BY Q.category")
                            .build();
}
