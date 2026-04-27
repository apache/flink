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
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/** Test data for multi-join tests. */
public class MultiJoinTestUtils {
    public static final SourceTestStep USERS_SOURCE =
            SourceTestStep.newBuilder("Users")
                    .addSchema("user_id STRING PRIMARY KEY NOT ENFORCED, name STRING, cash INT")
                    .producedValues(
                            Row.ofKind(RowKind.INSERT, "1", "Gus", 100),
                            Row.ofKind(RowKind.INSERT, "2", "Bob", 200),
                            Row.ofKind(RowKind.INSERT, "3", "Alice", 300))
                    .build();

    public static final SourceTestStep ORDERS_SOURCE =
            SourceTestStep.newBuilder("Orders")
                    .addSchema(
                            "user_id STRING, order_id STRING PRIMARY KEY NOT ENFORCED, product STRING")
                    .producedValues(
                            Row.ofKind(RowKind.INSERT, "1", "order1", "Product A"),
                            Row.ofKind(RowKind.INSERT, "2", "order2", "Product B"),
                            Row.ofKind(RowKind.INSERT, "2", "order3", "Product C"),
                            Row.ofKind(RowKind.INSERT, "4", "order4", "Product D"))
                    .build();

    public static final SourceTestStep PAYMENTS_SOURCE =
            SourceTestStep.newBuilder("Payments")
                    .addSchema(
                            "user_id STRING, payment_id STRING PRIMARY KEY NOT ENFORCED, price INT")
                    .producedValues(
                            Row.ofKind(RowKind.INSERT, "1", "payment1", 10),
                            Row.ofKind(RowKind.INSERT, "2", "payment2", 20),
                            Row.ofKind(RowKind.INSERT, "1", "payment3", 30),
                            Row.ofKind(RowKind.INSERT, "5", "payment5", 50))
                    .build();

    public static final SourceTestStep STRANGER_USERS =
            SourceTestStep.newBuilder("Users")
                    .addSchema("user_id STRING, name STRING, cash INT")
                    .producedValues(
                            Row.of("u1", "Will", 100),
                            Row.of("u2", "Eleven", 50),
                            Row.of("u3", "Dustin", 70),
                            Row.of("u4", "Mike", 200),
                            Row.of("u5", "Lucas", 100))
                    .build();

    public static final SourceTestStep STRANGER_ORDERS =
            SourceTestStep.newBuilder("Orders")
                    .addSchema("user_id STRING, order_id STRING, product STRING")
                    .producedValues(
                            Row.of("o1", "u1", "Map"),
                            Row.of("o2", "u1", "Flashlight"),
                            Row.of("o3", "u2", "Waffles"),
                            Row.of("o4", "u3", "Bike"),
                            Row.of("o5", "u4", "Comics"),
                            Row.of("o6", "u5", "Radio set"))
                    .build();

    public static final SourceTestStep STRANGER_PAYMENTS =
            SourceTestStep.newBuilder("Payments")
                    .addSchema("payment_id STRING, price INT, user_id STRING")
                    .producedValues(
                            Row.of("p1", 10, "u1"),
                            Row.of("p2", 20, "u1"),
                            Row.of("p3", 30, "u2"),
                            Row.of("p4", 100, "u1"),
                            Row.of("p5", 50, "u2"),
                            Row.of("p6", 100, "u5"),
                            Row.of("p7", 70, "u6"),
                            Row.of("p8", 200, "u7"),
                            Row.of("p9", 100, "u8"),
                            Row.of("p10", 100, "u9"),
                            Row.of("p11", 999, "u10"))
                    .build();

    public static final SinkTestStep RESULT_SINK =
            SinkTestStep.newBuilder("ResultSink")
                    .addSchema(
                            "user_id STRING",
                            "name STRING",
                            "order_id STRING",
                            "payment_id STRING",
                            "price INT")
                    .testMaterializedData()
                    .build();

    // Table data for consistency with JoinITCase
    public static final SourceTestStep TABLE_A =
            SourceTestStep.newBuilder("A")
                    .addSchema("a1 INT, a2 BIGINT, a3 STRING")
                    .producedValues(
                            Row.of(1, 1L, "Hi"),
                            Row.of(2, 2L, "Hello"),
                            Row.of(3, 2L, "Hello world"))
                    .build();

    public static final SourceTestStep TABLE_B =
            SourceTestStep.newBuilder("B")
                    .addSchema("b1 INT, b2 BIGINT, b3 INT, b4 STRING, b5 BIGINT")
                    .producedValues(
                            Row.of(1, 1L, 0, "Hallo", 1L),
                            Row.of(2, 2L, 1, "Hallo Welt", 2L),
                            Row.of(2, 3L, 2, "Hallo Welt wie", 1L),
                            Row.of(3, 4L, 3, "Hallo Welt wie gehts?", 2L),
                            Row.of(3, 5L, 4, "ABC", 2L),
                            Row.of(3, 6L, 5, "BCD", 3L))
                    .build();
}
