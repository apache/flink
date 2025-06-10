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

import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExtendWith(ParameterizedTestExtension.class)
class StreamingThreeWayOuterJoinCustomConditionOperatorTest
        extends StreamingMultiJoinOperatorTestBase {

    // Condition 1: input[1] (Orders) joins with input[0] (Users) ON user_id (field 0)
    // Condition 2: input[2] (Payments) joins with input[0] (Users) ON user_id (field 0)
    private static final List<GeneratedJoinCondition> customJoinCondition =
            Arrays.asList(null, createJoinCondition(1, 0), createJoinCondition(2, 0));

    // Define the corresponding attribute map
    private static final Map<Integer, List<ConditionAttributeRef>> customAttributeMap =
            new HashMap<>();

    static {
        // Mapping for join between input 1 (Orders) and input 0 (Users)
        customAttributeMap.put(1, Collections.singletonList(new ConditionAttributeRef(0, 0, 1, 0)));

        // Mapping for join between input 2 (Payments) and input 0 (Users)
        customAttributeMap.put(2, Collections.singletonList(new ConditionAttributeRef(0, 0, 2, 0)));
    }

    public StreamingThreeWayOuterJoinCustomConditionOperatorTest(
            StateBackendMode stateBackendMode) {
        // Testing left joins with custom conditions for a chain of tables:
        // Users LEFT JOIN Orders ON Users.id = Orders.id
        //       LEFT JOIN Payments ON Users.id = Payments.id (Custom part)
        super(
                stateBackendMode,
                3, // numInputs
                List.of(
                        FlinkJoinType.INNER,
                        FlinkJoinType.LEFT,
                        FlinkJoinType.LEFT), // joinTypes (first is placeholder)
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
    @TestTemplate
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
