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
}
