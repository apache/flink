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
class StreamingTwoWayInnerMultiJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    public StreamingTwoWayInnerMultiJoinOperatorTest(StateBackendMode stateBackendMode) {
        // For inner join test, set outerJoinFlags to false for all inputs
        super(
                stateBackendMode,
                2,
                List.of(FlinkJoinType.INNER, FlinkJoinType.INNER),
                defaultConditions(),
                false);
    }

    /** SELECT u.*, o.* FROM Users u INNER JOIN Orders o ON u.id = o.user_id. */
    @TestTemplate
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
    @TestTemplate
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
