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
class StreamingTwoWayOuterMultiJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    public StreamingTwoWayOuterMultiJoinOperatorTest(StateBackendMode stateBackendMode) {
        // For outer join test, set outerJoinFlags to true for all inputs to test full outer join
        super(
                stateBackendMode,
                2,
                List.of(FlinkJoinType.INNER, FlinkJoinType.LEFT),
                defaultConditions(),
                false);
    }

    /**
     * SELECT u.*, o.* FROM Users u LEFT OUTER JOIN Orders o ON u.id = o.user_id -- Test left outer
     * join behavior with nulls and transitions.
     */
    @TestTemplate
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
