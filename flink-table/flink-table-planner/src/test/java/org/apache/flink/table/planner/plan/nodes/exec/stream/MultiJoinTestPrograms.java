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
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import static org.apache.flink.table.planner.plan.nodes.exec.stream.MultiJoinTestUtils.ORDERS_SOURCE;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.MultiJoinTestUtils.PAYMENTS_SOURCE;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.MultiJoinTestUtils.USERS_SOURCE;

/** {@link TableTestProgram} definitions for testing Multi-Join. */
public class MultiJoinTestPrograms {
    public static final TableTestProgram MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN =
            TableTestProgram.of("three-way-left-outer-join", "three way left outer join")
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
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
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
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
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
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
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
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
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
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

    public static final TableTestProgram MULTI_JOIN_FOUR_WAY_COMPLEX_WITH_RESTORE =
            TableTestProgram.of(
                            "four-way-complex-updating-join-with-restore",
                            "four way complex updating join with restore")
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true)
                    .setupConfig(TableConfigOptions.PLAN_FORCE_RECOMPILE, true)
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
                                    .addOption("changelog-mode", "I,UA,UB,D")
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
}
