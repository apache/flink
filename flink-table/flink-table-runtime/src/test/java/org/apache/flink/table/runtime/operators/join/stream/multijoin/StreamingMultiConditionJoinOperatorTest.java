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

import org.apache.flink.table.runtime.generated.GeneratedMultiJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator.JoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.AttributeRef;
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
    private static final GeneratedMultiJoinCondition joinConditionAB = createMultiCondition(1, 0);

    private static final List<GeneratedMultiJoinCondition> customJoinConditions =
            Arrays.asList(null, joinConditionAB); // null for the first input

    // Attribute map reflecting A.f0=B.f0 and A.f2=B.f2
    private static final Map<Integer, Map<AttributeRef, AttributeRef>> customAttributeMap =
            new HashMap<>();

    static {
        // Mapping for join between input 1 (B) and input 0 (A)
        Map<AttributeRef, AttributeRef> map1 = new HashMap<>();
        // A.f0 -> B.f0 (using user_id fields from base schema)
        map1.put(new AttributeRef(0, 0), new AttributeRef(1, 0));
        // A.f2 -> B.f2 (using details fields from base schema)
        map1.put(new AttributeRef(0, 2), new AttributeRef(1, 2));
        customAttributeMap.put(1, map1); // Key is the right-side input index (1)
    }

    public StreamingMultiConditionJoinOperatorTest(StateBackendMode stateBackendMode) {
        // Two-way inner join with multiple conditions
        super(
                stateBackendMode,
                2, // numInputs
                List.of(JoinType.INNER, JoinType.INNER), // joinTypes (first is placeholder)
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
     * Creates a GeneratedMultiJoinCondition that compares fields 0 and 2 between the input at
     * `index` and the input at `indexToCompare`.
     */
    private static GeneratedMultiJoinCondition createMultiCondition(int index, int indexToCompare) {
        String funcCode =
                String.format(
                        "public class MultiConditionFunction_%1$d_%2$d extends org.apache.flink.api.common.functions.AbstractRichFunction "
                                + "implements org.apache.flink.table.runtime.generated.MultiJoinCondition {\n"
                                + "    private final int index = %1$d;\n"
                                + "    private final int indexToCompare = %2$d;\n"
                                + "    public MultiConditionFunction_%1$d_%2$d(Object[] reference) {}\n"
                                + "    @Override\n"
                                + "    public boolean apply(org.apache.flink.table.data.RowData[] inputs) {\n"
                                + "        // Basic null checks\n"
                                + "        if (inputs == null || inputs.length <= Math.max(index, indexToCompare) || inputs[indexToCompare] == null || inputs[index] == null) {\n"
                                + "            return false;\n"
                                + "        }\n"
                                + "        // Check null keys for field 0 and field 2\n"
                                + "        if (inputs[indexToCompare].isNullAt(0) || inputs[index].isNullAt(0) || inputs[indexToCompare].isNullAt(2) || inputs[index].isNullAt(2)) {\n"
                                + "            return false;\n"
                                + "        }\n"
                                + "        // Compare join keys (field 0 and field 2)\n"
                                + "        String key0Comp = inputs[indexToCompare].getString(0).toString();\n"
                                + "        String key0Curr = inputs[index].getString(0).toString();\n"
                                + "        String key2Comp = inputs[indexToCompare].getString(2).toString();\n"
                                + "        String key2Curr = inputs[index].getString(2).toString();\n"
                                + "        return key0Comp.equals(key0Curr) && key2Comp.equals(key2Curr);\n"
                                + "    }\n"
                                + "    @Override\n"
                                + "    public void close() throws Exception {\n"
                                + "        super.close();\n"
                                + "    }\n"
                                + "}\n",
                        index, indexToCompare);
        return new GeneratedMultiJoinCondition(
                String.format("MultiConditionFunction_%d_%d", index, indexToCompare),
                funcCode,
                new Object[0]);
    }
}
