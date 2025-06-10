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
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator}
 * with multiple join conditions.
 */
@ExtendWith(ParameterizedTestExtension.class)
class StreamingMultiConditionJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    // Condition for A JOIN B ON A.f0 = B.f0 AND A.f2 = B.f2
    private static final GeneratedJoinCondition multiCondition =
            createAndCondition(
                    createJoinCondition(1, 0), // equi-join on user_id
                    createCustomIndexJoinCondition(2, 2) // custom condition on details;
                    );

    private static final List<GeneratedJoinCondition> customJoinConditions =
            Arrays.asList(null, multiCondition); // null for the first input

    // Attribute map reflecting A.f0=B.f0 and A.f2=B.f2
    private static final Map<Integer, List<ConditionAttributeRef>> customAttributeMap =
            new HashMap<>();

    static {
        // Mapping for join between input 1 (B) and input 0 (A)
        customAttributeMap.put(
                1,
                Arrays.asList(
                        new ConditionAttributeRef(0, 0, 1, 0), // A.f0 -> B.f0
                        new ConditionAttributeRef(0, 2, 1, 2) // A.f2 -> B.f2
                        ));
    }

    public StreamingMultiConditionJoinOperatorTest(StateBackendMode stateBackendMode) {
        // Two-way inner join with multiple conditions
        super(
                stateBackendMode,
                2, // numInputs
                List.of(
                        FlinkJoinType.INNER,
                        FlinkJoinType.INNER), // joinTypes (first is placeholder)
                customJoinConditions, // Pass custom conditions
                customAttributeMap, // Pass the corresponding map
                false); // isFullOuterJoin
    }

    /**
     * SELECT u.*, o.* FROM Users u INNER JOIN Orders o ON u.user_id = o.user_id AND u.details =
     * o.details.
     *
     * <p>Primary key is user_id (index 0) and order_id (index 1)
     */
    @TestTemplate
    void testTwoWayInnerJoinMultiCondition() throws Exception {
        // Schema: Users(user_id_0, id_0, details_0), Orders(user_id_1, id_1, details_1)
        // Join: ON Users.user_id_0 = Orders.user_id_1 AND Users.details_0 = Orders.details_1

        // Insert user A
        insertUser("1", "uid_a", "detail_1"); // key=(1, detail_1)
        emitsNothing(); // Inner join, no match yet

        // Insert order B with matching user_id but different details
        insertOrder("1", "oid_b1", "detail_X"); // key=(1, detail_X)
        emitsNothing(); // Details don't match

        // Insert order C with matching user_id and details
        insertOrder("1", "oid_c1", "detail_1"); // key=(1, detail_1)
        emits(
                INSERT,
                "1",
                "uid_a",
                "detail_1", // User A
                "1",
                "oid_c1",
                "detail_1" // Order C
                );

        // Insert user D with different user_id but same details
        insertUser("2", "uid_d", "detail_1"); // key=(2, detail_1)
        emitsNothing(); // user_id doesn't match existing orders

        // Insert order E matching user D
        insertOrder("2", "oid_e1", "detail_1"); // key=(2, detail_1)
        emits(
                INSERT,
                "2",
                "uid_d",
                "detail_1", // User D
                "2",
                "oid_e1",
                "detail_1" // Order E
                );

        // Update user A - details change, breaking the join with order C
        updateBeforeUser("1", "uid_a", "detail_1");
        emits(
                UPDATE_BEFORE,
                r(
                        "1",
                        "uid_a",
                        "detail_1", // Old User A
                        "1",
                        "oid_c1",
                        "detail_1" // Order C
                        )); // Retract old join result

        updateAfterUser("1", "uid_a", "detail_UPDATED"); // key=(1, detail_UPDATED)
        emitsNothing(); // No UPDATE_AFTER emitted as details no longer match

        // Update order C - details change, matching the updated user A
        updateBeforeOrder("1", "oid_c1", "detail_1");
        emitsNothing();

        updateAfterOrder("1", "oid_c1", "detail_UPDATED"); // key=(1, detail_UPDATED)
        emits(
                UPDATE_AFTER,
                r(
                        "1",
                        "uid_a",
                        "detail_UPDATED", // Updated User A
                        "1",
                        "oid_c1",
                        "detail_UPDATED" // Updated Order C
                        )); // New join result emitted as INSERT

        // Delete user D
        deleteUser("2", "uid_d", "detail_1"); // key=(2, detail_1)
        emits(
                DELETE,
                r(
                        "2",
                        "uid_d",
                        "detail_1", // User D
                        "2",
                        "oid_e1",
                        "detail_1" // Order E
                        )); // Retract join result

        // Delete order C
        deleteOrder("1", "oid_c1", "detail_UPDATED"); // key=(1, detail_UPDATED)
        emits(
                DELETE,
                r(
                        "1",
                        "uid_a",
                        "detail_UPDATED", // Updated User A
                        "1",
                        "oid_c1",
                        "detail_UPDATED" // Updated Order C
                        )); // Retract join result
    }

    /**
     * Creates a GeneratedJoinCondition that compares fields 0 and 2 between the input at `index`
     * and the input at `indexToCompare`.
     */
    private static GeneratedJoinCondition createCustomIndexJoinCondition(
            int keyIndex, int keyIndexToCompare) {
        return new GeneratedJoinCondition("1", "", new Object[0]) {
            @Override
            public JoinCondition newInstance(ClassLoader classLoader) {
                // Field 0 is assumed for key comparison in this default test condition
                return new SpecificInputsEquiKeyCondition(keyIndex, keyIndexToCompare);
            }
        };
    }
}
