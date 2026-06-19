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
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

/** Tests for {@link StreamingMultiJoinOperator} where one input does not have a unique key. */
@ExtendWith(ParameterizedTestExtension.class)
class StreamingTwoWayJoinNoUniqueKeyOperatorTest extends StreamingMultiJoinOperatorTestBase {

    public StreamingTwoWayJoinNoUniqueKeyOperatorTest(StateBackendMode stateBackendMode) {
        // Inner join, 2 inputs, default conditions
        super(
                stateBackendMode,
                2,
                List.of(FlinkJoinType.INNER, FlinkJoinType.INNER),
                defaultConditions(),
                false);

        // Override the second input spec to NOT have a unique key
        this.inputSpecs.set(0, JoinInputSideSpec.withoutUniqueKey());
        this.inputSpecs.set(1, JoinInputSideSpec.withoutUniqueKey());
    }

    /**
     * SELECT u.*, o.* FROM Users u INNER JOIN Orders o ON u.id = o.user_id -- Orders table has NO
     * unique key defined for the operator.
     */
    @TestTemplate
    void testInnerJoinWithNoUniqueKeyOnRight() throws Exception {
        /* -------- APPEND TESTS ----------- */
        insertUser("1", "Gus", "User 1 Details");
        emitsNothing(); // User alone doesn't emit

        insertOrder("1", "order_1", "Order 1 Details");
        emits(
                INSERT,
                "1",
                "Gus",
                "User 1 Details",
                "1",
                "order_1",
                "Order 1 Details"); // Join emits

        // Insert same order should create identical output
        insertOrder("1", "order_1", "Order 1 Details");
        emits(
                INSERT,
                "1",
                "Gus",
                "User 1 Details",
                "1",
                "order_1",
                "Order 1 Details"); // Join emits again

        // Insert another order with the same join key, different details
        insertOrder("1", "order_2", "Order 2 Details");
        emits(
                INSERT,
                "1",
                "Gus",
                "User 1 Details",
                "1",
                "order_2",
                "Order 2 Details"); // Second join emits

        insertUser("2", "Bob", "User 2 Details");
        emitsNothing(); // Second user alone doesn't emit

        insertOrder("2", "order_3", "Order 3 Details");
        emits(INSERT, "2", "Bob", "User 2 Details", "2", "order_3", "Order 3 Details");

        /* -------- UPDATE/DELETE TESTS ----------- */
        // We emit now emit two updates since we have two identical order rows
        deleteUser("1", "Gus", "User 1 Details");
        emits(
                DELETE,
                r("1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details"),
                DELETE,
                r("1", "Gus", "User 1 Details", "1", "order_1", "Order 1 Details"),
                DELETE,
                r("1", "Gus", "User 1 Details", "1", "order_2", "Order 2 Details"));

        insertUser("1", "Gus Updated", "User 1 Details");
        emits(
                INSERT,
                r("1", "Gus Updated", "User 1 Details", "1", "order_1", "Order 1 Details"),
                INSERT,
                r("1", "Gus Updated", "User 1 Details", "1", "order_1", "Order 1 Details"),
                INSERT,
                r("1", "Gus Updated", "User 1 Details", "1", "order_2", "Order 2 Details"));

        // "Update" order_1 (results in -D for old, +I for new)
        deleteOrder("1", "order_1", "Order 1 Details");
        // No unique key, UB is treated as D
        emits(DELETE, "1", "Gus Updated", "User 1 Details", "1", "order_1", "Order 1 Details");

        updateAfterOrder("1", "order_1", "Order 1 Details - Second instance");
        // No unique key, UA is treated as I
        emits(
                UPDATE_AFTER,
                "1",
                "Gus Updated",
                "User 1 Details",
                "1",
                "order_1",
                "Order 1 Details - Second instance");

        // Delete order_2
        deleteOrder("1", "order_2", "Order 2 Details");
        emits(DELETE, "1", "Gus Updated", "User 1 Details", "1", "order_2", "Order 2 Details");

        // Delete user (retracts remaining join)
        deleteUser("1", "Gus Updated", "User 1 Details");
        emits(
                DELETE, r("1", "Gus Updated", "User 1 Details", "1", "order_1", "Order 1 Details"),
                DELETE,
                        r(
                                "1",
                                "Gus Updated",
                                "User 1 Details",
                                "1",
                                "order_1",
                                "Order 1 Details - Second instance"));
    }
}
