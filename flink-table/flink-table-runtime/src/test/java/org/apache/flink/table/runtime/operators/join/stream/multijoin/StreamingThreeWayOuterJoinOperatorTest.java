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
class StreamingThreeWayOuterJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    public StreamingThreeWayOuterJoinOperatorTest(StateBackendMode stateBackendMode) {
        // Testing left joins for a chain of tables: Users LEFT JOIN Orders LEFT JOIN Payments
        super(
                stateBackendMode, // Pass stateBackendMode to super
                3,
                List.of(FlinkJoinType.INNER, FlinkJoinType.LEFT, FlinkJoinType.LEFT),
                defaultConditions(),
                false);
    }

    /**
     * -- Test three-way left outer join with nulls and changelog transitions.
     *
     * <p>SQL: SELECT u.*, o.*, p.* FROM Users u LEFT OUTER JOIN Orders o ON u.user_id = o.user_id
     * LEFT OUTER JOIN Payments p ON o.user_id = p.user_id
     *
     * <p>Schema: Users(user_id PRIMARY KEY, name, details) Orders(user_id, order_id PRIMARY KEY,
     * name) Payments(user_id, payment_id PRIMARY KEY, name)
     */
    @TestTemplate
    void testThreeWayLeftOuterJoin() throws Exception {
        /* -------- LEFT OUTER JOIN APPEND TESTS ----------- */

        // Users without orders/payments are emitted with nulls
        insertUser("1", "Gus", "User 1 Details");

        // Should emit an update to the previous left outer join result
        // First delete the old record with nulls and then emit the joined record
        emits(INSERT, "1", "Gus", "User 1 Details", null, null, null, null, null, null);

        // Orders without users aren't emitted (LEFT join)
        insertOrder("2", "order_2", "Order 2 Details");
        emitsNothing();

        // Payments without users aren't emitted (LEFT join)
        insertPayment("3", "payment_3", "Payment 3 Details");
        emitsNothing();

        /* -------- PARTIAL MATCH TRANSITIONS ----------- */

        // Add matching order - deletes null result, emits partial join (left+middle, right null)
        insertOrder("1", "order_1", "Order 1 Details");
        emits(
                DELETE,
                r("1", "Gus", "User 1 Details", null, null, null, null, null, null),
                INSERT,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        null,
                        null,
                        null));

        // Add matching payment - deletes partial join, emits full join
        insertPayment("1", "payment_1", "Payment 1 Details");
        emits(
                DELETE,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        null,
                        null,
                        null),
                INSERT,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        "1",
                        "payment_1",
                        "Payment 1 Details"));

        /* -------- DELETE TRANSITIONS ----------- */

        // Delete payment - reverts to left+middle join
        deletePayment("1", "payment_1", "Payment 1 Details");
        emits(
                DELETE,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        "1",
                        "payment_1",
                        "Payment 1 Details"),
                INSERT,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        null,
                        null,
                        null));

        // Delete order - reverts left only join
        deleteOrder("1", "order_1", "Order 1 Details");
        emits(
                DELETE,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        null,
                        null,
                        null),
                INSERT,
                r("1", "Gus", "User 1 Details", null, null, null, null, null, null));

        // Re-add order - transitions back to left+middle join
        insertOrder("1", "order_1", "Order 1 Details");
        emits(
                DELETE,
                r("1", "Gus", "User 1 Details", null, null, null, null, null, null),
                INSERT,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        null,
                        null,
                        null));

        // Re-add payment - transitions to full join
        insertPayment("1", "payment_1", "Payment 1 Details");
        emits(
                DELETE,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        null,
                        null,
                        null),
                INSERT,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        "1",
                        "payment_1",
                        "Payment 1 Details"));

        /* -------- USER DELETE/REINSERT TESTS ----------- */

        // Delete left record removes entire result
        deleteUser("1", "Gus", "User 1 Details");
        emits(
                DELETE,
                "1",
                "Gus",
                "User 1 Details",
                "1",
                "order_1",
                "Order 1 Details",
                "1",
                "payment_1",
                "Payment 1 Details");

        // Re-add user restores join
        insertUser("1", "Gus", "User 1 Details");
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

        /* -------- USER UPDATE TESTS ----------- */

        // -U on user emits -U
        updateBeforeUser("1", "Gus", "User 1 Details");
        emits(
                UPDATE_BEFORE,
                "1",
                "Gus",
                "User 1 Details",
                "1",
                "order_1",
                "Order 1 Details",
                "1",
                "payment_1",
                "Payment 1 Details");

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

        /* -------- ORDER UPDATE TESTS ----------- */

        // -U on order emits -U and temporarily reverts to left+null join
        updateBeforeOrder("1", "order_1", "Order 1 Details");
        emits(
                UPDATE_BEFORE,
                r(
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details",
                        "1",
                        "payment_1",
                        "Payment 1 Details"),
                // and 1 and 3 in the conditions? payments
                // would still show up?
                INSERT,
                r("1", "Gus", "User 1 Details Updated", null, null, null, null, null, null));

        // +U on order removes null result and emits join
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");
        emits(
                DELETE,
                r("1", "Gus", "User 1 Details Updated", null, null, null, null, null, null),
                UPDATE_AFTER,
                r(
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_1",
                        "Payment 1 Details"));

        /* -------- PAYMENT UPDATE TESTS ----------- */

        // -U on payment emits -U and temporarily reverts to left+middle join
        updateBeforePayment("1", "payment_1", "Payment 1 Details");
        emits(
                UPDATE_BEFORE,
                r(
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_1",
                        "Payment 1 Details"),
                INSERT,
                r(
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        null,
                        null,
                        null));

        // +U on payment removes null result and emits join
        updateAfterPayment("1", "payment_1", "Payment 1 Details Updated");
        emits(
                DELETE,
                r(
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        null,
                        null,
                        null),
                UPDATE_AFTER,
                r(
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_1",
                        "Payment 1 Details Updated"));

        /* -------- MULTI-TABLE SCENARIOS ----------- */

        // Adding a second payment for same user/order
        insertPayment("1", "payment_2", "Payment 2 Details");
        emits(
                INSERT,
                "1",
                "Gus",
                "User 1 Details Updated",
                "1",
                "order_1",
                "Order 1 Details Updated",
                "1",
                "payment_2",
                "Payment 2 Details");

        // Delete user with multiple joined records deletes all join results
        deleteUser("1", "Gus", "User 1 Details Updated");
        emits(
                DELETE,
                r(
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_2",
                        "Payment 2 Details"),
                DELETE,
                r(
                        "1",
                        "Gus",
                        "User 1 Details Updated",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_1",
                        "Payment 1 Details Updated"));

        // New user with same key joins with both orders and payments
        insertUser("1", "Dawid", "User 3 Details");
        emits(
                INSERT,
                r(
                        "1",
                        "Dawid",
                        "User 3 Details",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_2",
                        "Payment 2 Details"),
                INSERT,
                r(
                        "1",
                        "Dawid",
                        "User 3 Details",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_1",
                        "Payment 1 Details Updated"));

        /* -------- COMPLEX SCENARIO: MULTIPLE PARTIAL MATCHES ----------- */

        // Payment for user 2 matches order we added at the beginning of the test, but we don't
        // emit since we have no user
        // insertOrder("2", "order_2", "Order 2 Details");
        insertPayment("2", "payment_3", "Payment 3 Details");
        emitsNothing();

        insertPayment("2", "payment_4", "Payment 4 Details");
        emitsNothing();

        // Now we have a match for all three
        insertUser("2", "Bob", "User 2 Details");

        // Add payment for user 2 (completes the join)
        emits(
                INSERT,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_4",
                        "Payment 4 Details"),
                INSERT,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_3",
                        "Payment 3 Details"));

        // Delete order first - since our join condition matches payment based on orders
        // we will get a full null padded row for bob
        // If our join condition joined based on attributes from third table with the first
        // We'd have gotten bob null payment
        deleteOrder("2", "order_2", "Order 2 Details");
        emits(
                DELETE,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_4",
                        "Payment 4 Details"),
                DELETE,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_3",
                        "Payment 3 Details"),
                INSERT,
                r("2", "Bob", "User 2 Details", null, null, null, null, null, null));

        insertOrder("2", "order_2", "Order 2 Details");
        emits(
                DELETE,
                r("2", "Bob", "User 2 Details", null, null, null, null, null, null),
                INSERT,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_4",
                        "Payment 4 Details"),
                INSERT,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_3",
                        "Payment 3 Details"));

        insertOrder("2", "order_3", "Order 3 Details");
        emits(
                INSERT,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_3",
                        "Order 3 Details",
                        "2",
                        "payment_4",
                        "Payment 4 Details"),
                INSERT,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_3",
                        "Order 3 Details",
                        "2",
                        "payment_3",
                        "Payment 3 Details"));

        updateAfterPayment("2", "payment_4", "Payment 4 Details Updated");
        emits(
                UPDATE_AFTER,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_4",
                        "Payment 4 Details Updated"),
                UPDATE_AFTER,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_3",
                        "Order 3 Details",
                        "2",
                        "payment_4",
                        "Payment 4 Details Updated"));

        updateAfterUser("2", "Bob", "User 2 Details Updated");
        emits(
                UPDATE_AFTER,
                r(
                        "2",
                        "Bob",
                        "User 2 Details Updated",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_4",
                        "Payment 4 Details Updated"),
                UPDATE_AFTER,
                r(
                        "2",
                        "Bob",
                        "User 2 Details Updated",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_3",
                        "Payment 3 Details"),
                UPDATE_AFTER,
                r(
                        "2",
                        "Bob",
                        "User 2 Details Updated",
                        "2",
                        "order_3",
                        "Order 3 Details",
                        "2",
                        "payment_4",
                        "Payment 4 Details Updated"),
                UPDATE_AFTER,
                r(
                        "2",
                        "Bob",
                        "User 2 Details Updated",
                        "2",
                        "order_3",
                        "Order 3 Details",
                        "2",
                        "payment_3",
                        "Payment 3 Details"));

        deleteUser("2", "Bob", "User 2 Details");
        emits(
                DELETE,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_4",
                        "Payment 4 Details Updated"),
                DELETE,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_2",
                        "Order 2 Details",
                        "2",
                        "payment_3",
                        "Payment 3 Details"),
                DELETE,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_3",
                        "Order 3 Details",
                        "2",
                        "payment_4",
                        "Payment 4 Details Updated"),
                DELETE,
                r(
                        "2",
                        "Bob",
                        "User 2 Details",
                        "2",
                        "order_3",
                        "Order 3 Details",
                        "2",
                        "payment_3",
                        "Payment 3 Details"));
    }
}
