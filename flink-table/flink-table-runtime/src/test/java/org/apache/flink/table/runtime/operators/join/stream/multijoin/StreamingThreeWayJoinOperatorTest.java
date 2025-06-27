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

package org.apache.flink.table.runtime.operators.join.stream.multijoin;

import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

@ExtendWith(ParameterizedTestExtension.class)
class StreamingThreeWayJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    public StreamingThreeWayJoinOperatorTest(StateBackendMode stateBackendMode) {
        // For inner join test, set joinTypes to INNER for all joins
        super(
                stateBackendMode,
                3,
                List.of(FlinkJoinType.INNER, FlinkJoinType.INNER, FlinkJoinType.INNER),
                defaultConditions(),
                false);
    }

    /**
     * SELECT u.*, o.*, p.* FROM Users u INNER JOIN Orders o ON u.id = o.user_id INNER JOIN Payments
     * p ON u.id = p.user_id -- Test three-way inner join with append-only data.
     */
    @TestTemplate
    void testThreeWayInnerJoin() throws Exception {
        /* -------- THREE-WAY JOIN APPEND TESTS ----------- */

        // First table alone doesn't emit
        insertUser("1", "Gus", "User 1 Details");
        emitsNothing();

        // First two tables don't emit
        insertOrder("1", "order_1", "Order 1 Details");
        emitsNothing();

        // All three tables match emits join
        insertPayment("1", "payment_1", "Payment 1 Details");
        emits(
                INSERT,
                "1",
                "Gus",
                "User 1 Details",
                "1",
                "order_1",
                "Order 1 Details",
                "1",
                "payment_1",
                "Payment 1 Details");

        // Testing with second set of records
        insertUser("2", "Bob", "User 2 Details");
        insertOrder("2", "order_2", "Order 2 Details");
        emitsNothing();

        insertPayment("2", "payment_2", "Payment 2 Details");
        emits(
                INSERT,
                "2",
                "Bob",
                "User 2 Details",
                "2",
                "order_2",
                "Order 2 Details",
                "2",
                "payment_2",
                "Payment 2 Details");
    }

    /**
     * SELECT u.*, o.*, p.* FROM Users u INNER JOIN Orders o ON u.id = o.user_id INNER JOIN Payments
     * p ON u.id = p.user_id -- Test updates and deletes across all three tables.
     */
    @TestTemplate
    void testThreeWayInnerJoinUpdating() throws Exception {
        /* -------- SETUP BASE DATA ----------- */

        // Set up initial three-way join
        insertUser("1", "Gus", "User 1 Details");
        insertOrder("1", "order_1", "Order 1 Details");
        insertPayment("1", "payment_1", "Payment 1 Details");
        emits(
                INSERT,
                "1",
                "Gus",
                "User 1 Details",
                "1",
                "order_1",
                "Order 1 Details",
                "1",
                "payment_1",
                "Payment 1 Details");

        /* -------- UPDATE TESTS ----------- */

        // +U on user emits +U
        updateAfterUser("1", "Gus", "User 1 Details Updated");
        emits(
                UPDATE_AFTER,
                "1",
                "Gus",
                "User 1 Details Updated",
                "1",
                "order_1",
                "Order 1 Details",
                "1",
                "payment_1",
                "Payment 1 Details");

        // +U on order emits +U
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");
        emits(
                UPDATE_AFTER,
                "1",
                "Gus",
                "User 1 Details Updated",
                "1",
                "order_1",
                "Order 1 Details Updated",
                "1",
                "payment_1",
                "Payment 1 Details");

        // +U on payment emits +U
        updateAfterPayment("1", "payment_1", "Payment 1 Details Updated");
        emits(
                UPDATE_AFTER,
                "1",
                "Gus",
                "User 1 Details Updated",
                "1",
                "order_1",
                "Order 1 Details Updated",
                "1",
                "payment_1",
                "Payment 1 Details Updated");

        /* -------- DELETE/REINSERT TESTS ----------- */

        // -D on payment emits -D for join
        deletePayment("1", "payment_1", "Payment 1 Details Updated");
        emits(
                DELETE,
                "1",
                "Gus",
                "User 1 Details Updated",
                "1",
                "order_1",
                "Order 1 Details Updated",
                "1",
                "payment_1",
                "Payment 1 Details Updated");

        // Re-add payment emits +I
        insertPayment("1", "payment_1", "Payment 1 New Details");
        emits(
                INSERT,
                "1",
                "Gus",
                "User 1 Details Updated",
                "1",
                "order_1",
                "Order 1 Details Updated",
                "1",
                "payment_1",
                "Payment 1 New Details");

        /* -------- SECOND JOIN TESTS ----------- */

        // Adding a second set with key "2"
        insertUser("2", "Bob", "User 2 Details");
        insertOrder("2", "order_2", "Order 2 Details");
        insertPayment("2", "payment_2", "Payment 2 Details");
        emits(
                INSERT,
                "2",
                "Bob",
                "User 2 Details",
                "2",
                "order_2",
                "Order 2 Details",
                "2",
                "payment_2",
                "Payment 2 Details");

        // Delete user 2 emits -D
        deleteUser("2", "Bob", "User 2 Details");
        emits(
                DELETE,
                "2",
                "Bob",
                "User 2 Details",
                "2",
                "order_2",
                "Order 2 Details",
                "2",
                "payment_2",
                "Payment 2 Details");

        // Re-add user 2 with update emits +I
        insertUser("2", "Bob_Updated", "User 2 Details Updated");
        emits(
                INSERT,
                "2",
                "Bob_Updated",
                "User 2 Details Updated",
                "2",
                "order_2",
                "Order 2 Details",
                "2",
                "payment_2",
                "Payment 2 Details");

        insertPayment("2", "payment_3", "Payment 3 Details");
        emits(
                INSERT,
                "2",
                "Bob_Updated",
                "User 2 Details Updated",
                "2",
                "order_2",
                "Order 2 Details",
                "2",
                "payment_3",
                "Payment 3 Details");
    }
}
