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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedMultiJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator.JoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.AttributeRef;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests for {@link StreamingMultiJoinOperator} with non-equi join conditions. */
class StreamingTwoWayNonEquiJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    // Condition: Users.amount > Orders.amount
    private static final GeneratedMultiJoinCondition nonEquiCondition =
            createAmountGreaterThanCondition(1, 0);

    public StreamingTwoWayNonEquiJoinOperatorTest() {
        // Inner join, 2 inputs, custom non-equi condition
        super(
                2,
                List.of(JoinType.INNER, JoinType.INNER),
                // TODO Gustavo complement with the equal condition.
                // Maybe make the join conditions more generic
                Arrays.asList(null, nonEquiCondition), // Use custom condition for the join step
                new HashMap<>(), // Start with empty map, populate based on partitioning below
                false);

        // Partitioning is still based on user_id (field 0) = order_user_id (field 0)
        Map<AttributeRef, AttributeRef> map1 = new HashMap<>();
        map1.put(new AttributeRef(0, 0), new AttributeRef(1, 0)); // user[0] -> order[0]
        this.joinAttributeMap.put(1, map1);
    }

    /**
     * SELECT u.*, o.* FROM Users u INNER JOIN Orders o ON u.user_id = o.user_id AND u.amount >
     * o.amount.
     */
    @Test
    void testInnerJoinWithNonEquiCondition() throws Exception {
        /* -------- Basic Insertions and Matches ----------- */

        // User with amount 100
        insertUser("1", 100L, "Gus");
        emitsNothing();

        // Order with amount 50 (100 > 50 -> match)
        insertOrder("1", 50L, "order_1");
        emits(INSERT, r("1", 100L, "Gus", "1", 50L, "order_1"));

        // Order with amount 150 (100 > 150 -> no match)
        insertOrder("1", 150L, "order_2");
        emitsNothing();

        // Order with amount 100 (100 > 100 -> no match)
        insertOrder("1", 100L, "order_3");
        emitsNothing();

        /* -------- User Delete/Re-insert with Different Amount ----------- */

        // Delete the original user
        deleteUser("1", 100L, "Gus");
        emits(DELETE, r("1", 100L, "Gus", "1", 50L, "order_1"));

        // Re-insert user with a larger amount
        insertUser("1", 200L, "Gus Updated");
        // Should join with order_1 (200 > 50), order_2 (200 > 150), order_3 (200 > 100)
        emits(
                INSERT, r("1", 200L, "Gus Updated", "1", 50L, "order_1"),
                INSERT, r("1", 200L, "Gus Updated", "1", 100L, "order_3"),
                INSERT, r("1", 200L, "Gus Updated", "1", 150L, "order_2"));

        // Delete order_1
        deleteOrder("1", 50L, "order_1");
        emits(DELETE, r("1", 200L, "Gus Updated", "1", 50L, "order_1"));
    }

    /**
     * -- Test inner join with non-equi condition (u.amount > o.amount) focusing on user updates.
     *
     * <p>SQL: SELECT u.*, o.* FROM Users u INNER JOIN Orders o ON u.user_id = o.user_id AND
     * u.amount > o.amount
     *
     * <p>Schema: Users(user_id PRIMARY KEY, amount, name) Orders(user_id, order_id PRIMARY KEY,
     * amount)
     */
    @Test
    void testInnerJoinWithNonEquiConditionUserUpdates() throws Exception {
        /* -------- Setup Initial User and Order ----------- */
        // User with amount 100
        insertUser("1", 100L, "Gus");
        emitsNothing();

        /* -------- User Update ----------- */

        // Update User: amount from 100 to 40 (40 > 50 -> no match)
        updateBeforeUser("1", 100L, "Gus");
        emitsNothing(); // No new match

        updateAfterUser("1", 200L, "Gus Updated");
        emitsNothing();

        /* -------- Insert order (creates match) ----------- */
        // Insert another order that will also match
        insertOrder("1", 150L, "order_1"); // 200 > 150
        emits(INSERT, r("1", 200L, "Gus Updated", "1", 150L, "order_1"));

        /* -------- User Update: Amount Increase (Creates Match) ----------- */

        // Update User: amount from 40 to 200 (200 > 50 -> match)
        updateBeforeUser("1", 40L, "Gus Updated");
        emitsNothing();

        // TODO GUSTAVO UA? Probably not right
        updateAfterUser("1", 200L, "Gus Updated Again");
        emits(UPDATE_AFTER, r("1", 200L, "Gus Updated Again", "1", 150L, "order_1"));

        /* -------- Insert Additional Order and Test Subsequent User Update ----------- */

        // Insert another order that will also match
        insertOrder("1", 150L, "order_2"); // 200 > 150
        emits(INSERT, r("1", 200L, "Gus Updated Again", "1", 150L, "order_2"));

        // Update User: amount from 200 to 100.
        // (100 > 50 -> match with order_1)
        // (100 > 150 -> no match with order_2)
        updateBeforeUser("1", 200L, "Gus Updated Again");
        emits(
                UPDATE_BEFORE, r("1", 200L, "Gus Updated Again", "1", 150L, "order_1"),
                UPDATE_BEFORE, r("1", 200L, "Gus Updated Again", "1", 150L, "order_2"));
        updateAfterUser("1", 500L, "Gus Final");

        emits(
                UPDATE_AFTER, r("1", 500L, "Gus Final", "1", 150L, "order_1"),
                UPDATE_AFTER, r("1", 500L, "Gus Final", "1", 150L, "order_2"));
    }

    /**
     * -- Test inner join with non-equi condition (u.amount > o.amount) focusing on order updates.
     *
     * <p>SQL: SELECT u.*, o.* FROM Users u INNER JOIN Orders o ON u.user_id = o.user_id AND
     * u.amount > o.amount
     *
     * <p>Schema: Users(user_id PRIMARY KEY, amount, name) Orders(user_id, order_id PRIMARY KEY,
     * amount)
     */
    @Test
    void testInnerJoinWithNonEquiConditionOrderUpdates() throws Exception {
        /* -------- Setup Initial User and Non-Matching Order ----------- */

        // User with amount 100
        insertUser("1", 100L, "Gus");
        emitsNothing();

        // Order with amount 150 (100 > 150 -> no match)
        insertOrder("1", 150L, "order_1");
        emitsNothing();

        /* -------- Order Update: Amount Decrease (Creates Match) ----------- */

        // Update Order: amount from 150 to 50 (100 > 50 -> match)
        updateBeforeOrder("1", 150L, "order_1");
        emitsNothing();

        updateAfterOrder("1", 50L, "order_1");
        // TODO: Check if UA ist OK or we have to do insert
        emits(RowKind.UPDATE_AFTER, r("1", 100L, "Gus", "1", 50L, "order_1"));

        /* -------- Order Update: Amount Increase (Breaks Match) ----------- */
        // Update Order: amount from 50 to 100 (100 > 100 -> no match)

        // TODO Gustavo: should we retract here if we don't match anymore? hm, interesting question
        updateAfterOrder("1", 100L, "order_1");
        emitsNothing(); // No new match

        /* -------- Insert New User and Test Subsequent Order Update ----------- */

        // Insert another user who will match the current order (amount 100)
        insertUser("1", 200L, "Bob"); // 200 > 100 -> match
        emits(INSERT, r("1", 200L, "Bob", "1", 100L, "order_1"));

        // Update Order: amount from 100 to 250.
        // (100 > 250 -> no match with Gus)
        // (200 > 250 -> no match with Bob)
        updateBeforeOrder("1", 100L, "order_1");
        // TODO: Check if UB ist OK or we have to do delete
        emits(UPDATE_BEFORE, r("1", 200L, "Bob", "1", 100L, "order_1"));
        updateAfterOrder("1", 250L, "order_1_too_high");
        emitsNothing();
    }

    // Override input types to include an amount field
    @Override
    protected InternalTypeInfo<RowData> createInputTypeInfo(int inputIndex) {
        if (inputIndex == 0) { // Users: user_id (VARCHAR), amount (BIGINT), name (VARCHAR)
            return InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {
                                VarCharType.STRING_TYPE, new BigIntType(), VarCharType.STRING_TYPE
                            },
                            new String[] {"user_id_0", "amount_0", "name_0"}));
        } else { // Orders: order_user_id (VARCHAR), amount (BIGINT), order_id (VARCHAR)
            return InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {
                                VarCharType.STRING_TYPE, new BigIntType(), VarCharType.STRING_TYPE
                            },
                            new String[] {"user_id_1", "amount_1", "order_id_1"}));
        }
    }

    // Override unique key type (assuming amount is not part of UK)
    @Override
    protected InternalTypeInfo<RowData> createUniqueKeyType(int inputIndex) {
        if (inputIndex == 0) { // Users: user_id
            return InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {VarCharType.STRING_TYPE},
                            new String[] {"user_id_0"}));
        } else { // Orders: order_id
            return InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {VarCharType.STRING_TYPE},
                            new String[] {"order_id_1"}));
        }
    }

    // Override key selector for unique key (field 0 for Users, field 2 for Orders)
    @Override
    protected RowDataKeySelector createKeySelector(int inputIndex) {
        return HandwrittenSelectorUtil.getRowDataSelector(
                new int[] {inputIndex == 0 ? 0 : 2}, // UK is user_id[0] or order_id[2]
                inputTypeInfos
                        .get(inputIndex)
                        .toRowType()
                        .getChildren()
                        .toArray(new LogicalType[0]));
    }

    /**
     * Creates a GeneratedMultiJoinCondition that checks if amount (field 1) of input `index` is
     * greater than amount (field 1) of input `indexToCompare`.
     */
    protected static GeneratedMultiJoinCondition createAmountGreaterThanCondition(
            int index, int indexToCompare) {
        // Ensure indices are valid for comparison
        if (index <= 0 || indexToCompare < 0 || index == indexToCompare) {
            throw new IllegalArgumentException("Invalid indices for creating join condition.");
        }

        String funcCode =
                String.format(
                        "public class AmountGreaterThanCondition_%d_%d extends org.apache.flink.api.common.functions.AbstractRichFunction "
                                + "implements org.apache.flink.table.runtime.generated.MultiJoinCondition {\n"
                                + "    private final int index = %d;\n"
                                + "    private final int indexToCompare = %d;\n"
                                + "    public AmountGreaterThanCondition_%d_%d(Object[] reference) {}\n"
                                + "    @Override\n"
                                + "    public boolean apply(org.apache.flink.table.data.RowData[] inputs) {\n"
                                + "        // Basic null checks\n"
                                + "        if (inputs == null || inputs.length <= Math.max(index, indexToCompare) || inputs[indexToCompare] == null || inputs[index] == null) {\n"
                                + "            return false;\n"
                                + "        }\n"
                                + "        // Check null amounts (field 1)\n"
                                + "        if (inputs[indexToCompare].isNullAt(1) || inputs[index].isNullAt(1)) {\n"
                                + "            return false;\n"
                                + "        }\n"
                                + "        // Compare amounts\n"
                                + "        long amountToCompare = inputs[indexToCompare].getLong(1);\n"
                                + "        long currentAmount = inputs[index].getLong(1);\n"
                                + "        return amountToCompare > currentAmount; // Users.amount > Orders.amount \n"
                                + "    }\n"
                                + "    @Override\n"
                                + "    public void close() throws Exception {\n"
                                + "        super.close();\n"
                                + "    }\n"
                                + "}\n",
                        index, indexToCompare, index, indexToCompare, index, indexToCompare);
        // Use unique class name
        return new GeneratedMultiJoinCondition(
                String.format("AmountGreaterThanCondition_%d_%d", index, indexToCompare),
                funcCode,
                new Object[0]);
    }
}
