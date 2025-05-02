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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.table.runtime.generated.GeneratedMultiJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator.JoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.AttributeRef;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class StreamingTwoWayInnerMultiJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    public StreamingTwoWayInnerMultiJoinOperatorTest() {
        // For inner join test, set outerJoinFlags to false for all inputs
        super(2, List.of(JoinType.INNER, JoinType.INNER), defaultConditions(), false);
    }

    /** SELECT u.*, o.* FROM Users u INNER JOIN Orders o ON u.id = o.user_id. */
    @Test
    void testTwoWayInnerJoin() throws Exception {
        /* -------- APPEND TESTS ----------- */

        // Users without orders aren't emitted
        insertUser("1", "Gus", "User 1 Details");
        emitsNothing();

        // User joins with matching order
        insertOrder("1", "order_1", "Order 1 Details");
        emits(INSERT, "1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details");

        // Orders without users aren't emitted
        insertOrder("2", "order_2", "Order 2 Details");
        emitsNothing();

        // Adding matching user triggers join
        insertUser("2", "Bob", "User 2 Details");
        emits(INSERT, "2", "Bob", "User 2 Details", "2", "order_2", "Order 2 Details");
    }

    /**
     * SELECT u.*, o.* FROM Users u INNER JOIN Orders o ON u.id = o.user_id -- Test updates and
     * deletes on both sides.
     */
    @Test
    void testTwoWayInnerJoinUpdating() throws Exception {
        /* -------- SETUP BASE DATA ----------- */
        insertUser("1", "Gus", "User 1 Details");
        emitsNothing();

        insertOrder("1", "order_1", "Order 1 Details");
        emits(INSERT, "1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details");

        /* -------- UPDATE TESTS ----------- */

        // +U on user.details emits +U
        updateAfterUser("1", "Gus", "User 1 Details Updated");
        emits(
                UPDATE_AFTER,
                "1",
                "Gus",
                "User 1 Details Updated",
                "1",
                "order_1",
                "Order 1 Details");

        // +U on order.details emits +U
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");
        emits(
                UPDATE_AFTER,
                "1",
                "Gus",
                "User 1 Details Updated",
                "1",
                "order_1",
                "Order 1 Details Updated");

        /* -------- DELETE TESTS ----------- */

        // -D on order emits -D
        deleteOrder("1", "order_1", "Order 1 Details Updated");
        emits(
                DELETE,
                "1",
                "Gus",
                "User 1 Details Updated",
                "1",
                "order_1",
                "Order 1 Details Updated");

        // Re-insert order emits +I
        insertOrder("1", "order_1", "Order 1 New Details");
        emits(INSERT, "1", "Gus", "User 1 Details Updated", "1", "order_1", "Order 1 New Details");
    }
}

class StreamingTwoWayOuterMultiJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    public StreamingTwoWayOuterMultiJoinOperatorTest() {
        // For outer join test, set outerJoinFlags to true for all inputs to test full outer join
        super(2, List.of(JoinType.INNER, JoinType.LEFT), defaultConditions(), false);
    }

    /**
     * SELECT u.*, o.* FROM Users u LEFT OUTER JOIN Orders o ON u.id = o.user_id -- Test left outer
     * join behavior with nulls and transitions.
     */
    @Test
    void testTwoWayLeftOuterJoin() throws Exception {
        /* -------- LEFT OUTER JOIN APPEND TESTS ----------- */

        // Left table row always emits, even without matching right row
        insertUser("1", "Gus", "User 1 Details");
        emits(INSERT, "1", "Gus", "User 1 Details", null, null, null);

        // Right-only record not emitted (LEFT join)
        insertOrder("2", "order_2", "Order 2 Details");
        emitsNothing();

        /* -------- MATCH/UNMATCH TRANSITIONS ----------- */

        // Add matching order - deletes null result, emits joined
        insertOrder("1", "order_1", "Order 1 Details");
        emits(
                DELETE, r("1", "Gus", "User 1 Details", null, null, null),
                INSERT, r("1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details"));

        // Delete order - reverts to left outer join result
        deleteOrder("1", "order_1", "Order 1 Details");
        emits(
                DELETE, r("1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details"),
                INSERT, r("1", "Gus", "User 1 Details", null, null, null));

        // Re-add order - transitions back to inner join
        insertOrder("1", "order_1", "Order 1 Details");
        emits(
                DELETE, r("1", "Gus", "User 1 Details", null, null, null),
                INSERT, r("1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details"));

        /* -------- USER DELETE/REINSERT TESTS ----------- */

        // Delete left record removes entire result
        deleteUser("1", "Gus", "User 1 Details");
        emits(DELETE, "1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details");

        // Re-add user restores join
        insertUser("1", "Gus", "User 1 Details");
        emits(INSERT, "1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details");

        /* -------- USER UPDATE TESTS ----------- */

        // -U on user emits -U
        updateBeforeUser("1", "Gus", "User 1 Details");
        emits(UPDATE_BEFORE, "1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details");

        // +U on user emits +U
        updateAfterUser("1", "Gus", "User 1 Details Updated");
        emits(
                UPDATE_AFTER,
                "1",
                "Gus",
                "User 1 Details Updated",
                "1",
                "order_1",
                "Order 1 Details");

        // Another +U on user
        updateAfterUser("1", "Gus", "User 1 Details Updated 2");
        emits(
                UPDATE_AFTER,
                "1",
                "Gus",
                "User 1 Details Updated 2",
                "1",
                "order_1",
                "Order 1 Details");

        /* -------- ORDER UPDATE TESTS ----------- */

        // -U on order emits -U and temporarily reverts to left outer
        updateBeforeOrder("1", "order_1", "Order 1 Details");
        emits(
                UPDATE_BEFORE,
                        r(
                                "1",
                                "Gus",
                                "User 1 Details Updated 2",
                                "1",
                                "order_1",
                                "Order 1 Details"),
                INSERT, r("1", "Gus", "User 1 Details Updated 2", null, null, null));

        // +U on order removes null result and emits join
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");
        emits(
                DELETE, r("1", "Gus", "User 1 Details Updated 2", null, null, null),
                UPDATE_AFTER,
                        r(
                                "1",
                                "Gus",
                                "User 1 Details Updated 2",
                                "1",
                                "order_1",
                                "Order 1 Details Updated"));

        // Another +U on order
        updateAfterOrder("1", "order_1", "Order 1 Details Updated 2");
        emits(
                UPDATE_AFTER,
                "1",
                "Gus",
                "User 1 Details Updated 2",
                "1",
                "order_1",
                "Order 1 Details Updated 2");

        /* -------- MULTI-ROW TESTS ----------- */

        // Adding second order for same user
        insertOrder("1", "order_2", "Order 2 Details");
        emits(INSERT, "1", "Gus", "User 1 Details Updated 2", "1", "order_2", "Order 2 Details");

        // Delete user with multiple orders deletes all join results
        deleteUser("1", "Gus", "User 1 Details Updated 2");
        emits(
                DELETE,
                r("1", "Gus", "User 1 Details Updated 2", "1", "order_2", "Order 2 Details"),
                DELETE,
                r(
                        "1",
                        "Gus",
                        "User 1 Details Updated 2",
                        "1",
                        "order_1",
                        "Order 1 Details Updated 2"));

        // New user with same key joins with both orders
        insertUser("1", "Dawid", "User 3 Details");
        emits(
                INSERT, r("1", "Dawid", "User 3 Details", "1", "order_2", "Order 2 Details"),
                INSERT,
                        r(
                                "1",
                                "Dawid",
                                "User 3 Details",
                                "1",
                                "order_1",
                                "Order 1 Details Updated 2"));
    }
}

class StreamingThreeWayJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    public StreamingThreeWayJoinOperatorTest() {
        // For inner join test, set joinTypes to INNER for all joins
        super(
                3,
                List.of(JoinType.INNER, JoinType.INNER, JoinType.INNER),
                defaultConditions(),
                false);
    }

    /**
     * SELECT u.*, o.*, p.* FROM Users u INNER JOIN Orders o ON u.id = o.user_id INNER JOIN Payments
     * p ON u.id = p.user_id -- Test three-way inner join with append-only data.
     */
    @Test
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
    @Test
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

class StreamingThreeWayOuterJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    public StreamingThreeWayOuterJoinOperatorTest() {
        // Testing left joins for a chain of tables: Users LEFT JOIN Orders LEFT JOIN Payments
        super(3, List.of(JoinType.INNER, JoinType.LEFT, JoinType.LEFT), defaultConditions(), false);
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
    @Test
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
                DELETE, r("1", "Gus", "User 1 Details", null, null, null, null, null, null),
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
                INSERT, r("1", "Gus", "User 1 Details", null, null, null, null, null, null));

        // Re-add order - transitions back to left+middle join
        insertOrder("1", "order_1", "Order 1 Details");
        emits(
                DELETE, r("1", "Gus", "User 1 Details", null, null, null, null, null, null),
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
                        r(
                                "1",
                                "Gus",
                                "User 1 Details Updated",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null));

        // +U on order removes null result and emits join
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");
        emits(
                DELETE, r("1", "Gus", "User 1 Details Updated", null, null, null, null, null, null),
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

class StreamingThreeWayOuterJoinCustomConditionOperatorTest
        extends StreamingMultiJoinOperatorTestBase {

    // Condition 1: input[1] (Orders) joins with input[0] (Users) ON user_id (field 0)
    // Condition 2: input[2] (Payments) joins with input[0] (Users) ON user_id (field 0)
    private static final List<GeneratedMultiJoinCondition> customJoinCondition =
            Arrays.asList(null, createJoinCondition(1, 0), createJoinCondition(2, 0));

    // Define the corresponding attribute map
    private static final Map<Integer, Map<AttributeRef, AttributeRef>> customAttributeMap =
            new HashMap<>();

    static {
        // Mapping for join between input 1 (Orders) and input 0 (Users)
        Map<AttributeRef, AttributeRef> map1 = new HashMap<>();
        map1.put(new AttributeRef(0, 0), new AttributeRef(1, 0)); // user[0] -> order[0]
        customAttributeMap.put(1, map1); // Key is the right-side input index (1)

        // Mapping for join between input 2 (Payments) and input 0 (Users)
        Map<AttributeRef, AttributeRef> map2 = new HashMap<>();
        map2.put(new AttributeRef(0, 0), new AttributeRef(2, 0)); // user[0] -> payment[0]
        customAttributeMap.put(2, map2); // Key is the right-side input index (2)
    }

    public StreamingThreeWayOuterJoinCustomConditionOperatorTest() {
        // Testing left joins with custom conditions for a chain of tables:
        // Users LEFT JOIN Orders ON Users.id = Orders.id
        //       LEFT JOIN Payments ON Users.id = Payments.id (Custom part)
        super(
                3, // numInputs
                List.of(
                        JoinType.INNER,
                        JoinType.LEFT,
                        JoinType.LEFT), // joinTypes (first is placeholder)
                customJoinCondition, // Pass custom conditions
                customAttributeMap, // Pass the corresponding map
                false); // isFullOuterJoin
    }

    /**
     * -- Test three-way left outer join with nulls and changelog transitions.
     *
     * <p>SQL: SELECT u.*, o.*, p.* FROM Users u LEFT OUTER JOIN Orders o ON u.user_id = o.user_id
     * LEFT OUTER JOIN Payments p ON u.user_id = p.user_id <- This is the core difference here
     *
     * <p>Schema: Users(user_id PRIMARY KEY, name, details) Orders(user_id, order_id PRIMARY KEY,
     * name) Payments(user_id, payment_id PRIMARY KEY, name)
     */
    @Test
    void testThreeWayLeftOuterJoinCustomCondition() throws Exception {
        /* -------- LEFT OUTER JOIN APPEND TESTS ----------- */

        // Users without orders/payments are emitted with nulls
        insertPayment("1", "payment_1", "Payment 1 Details");
        emitsNothing();

        insertOrder("1", "order_1", "Order 1 Details");
        emitsNothing();

        // Add matching user and emits full join
        insertUser("1", "Gus", "User 1 Details");
        emits(
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

        /* -------- ORDER UPDATE TESTS ----------- */

        // -U on order emits -U and temporarily reverts to left+null join
        updateBeforeOrder("1", "order_1", "Order 1 Details");
        emits(
                UPDATE_BEFORE,
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
                // and 1 and 3 in the conditions? payments
                // would still show up?
                INSERT,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        null,
                        null,
                        null,
                        "1",
                        "payment_1",
                        "Payment 1 Details"));

        // +U on order removes null result and emits join
        updateAfterOrder("1", "order_1", "Order 1 Details Updated");
        emits(
                DELETE,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        null,
                        null,
                        null,
                        "1",
                        "payment_1",
                        "Payment 1 Details"),
                UPDATE_AFTER,
                r(
                        "1",
                        "Gus",
                        "User 1 Details",
                        "1",
                        "order_1",
                        "Order 1 Details Updated",
                        "1",
                        "payment_1",
                        "Payment 1 Details"));
    }
}
