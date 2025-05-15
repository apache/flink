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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedMultiJoinCondition;
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
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
    private static final GeneratedMultiJoinCondition EqualIdAndGreaterAmountCondition =
            createFullJoinCondition(0, 0, 1,1, 0, 1);

    public StreamingTwoWayNonEquiJoinOperatorTest() {
        // Inner join, 2 inputs, custom non-equi condition
        super(
                2,
                List.of(JoinType.INNER, JoinType.INNER),
                // Maybe make the join conditions more generic
                Arrays.asList(null, EqualIdAndGreaterAmountCondition), // Use custom condition for the join step
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

        /* -------- User Update: Amount Increase and decreases (creates and removes Match) ----------- */

        updateBeforeUser("1", 200L, "Gus Updated");
        emits(UPDATE_BEFORE, r("1", 200L, "Gus Updated", "1", 150L, "order_1"));

        updateAfterUser("1", 200L, "Gus Updated Again");
        emits(UPDATE_AFTER, r("1", 200L, "Gus Updated Again", "1", 150L, "order_1"));

        updateBeforeUser("1", 200L, "Gus Updated Again");
        emits(UPDATE_BEFORE, r("1", 200L, "Gus Updated Again", "1", 150L, "order_1"));

        updateAfterUser("1", 50L, "Gus Updated Again to 50");
        emitsNothing();

        updateBeforeUser("1", 50L, "Gus Updated Again to 50");
        emitsNothing();

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
        emits(RowKind.UPDATE_AFTER, r("1", 100L, "Gus", "1", 50L, "order_1"));

        /* -------- Order Update: Amount Increase (Breaks Match) ----------- */
        // Update Order: amount from 50 to 100 (100 > 100 -> no match)
        updateBeforeOrder("1", 50L, "order_1");
        emits(RowKind.UPDATE_BEFORE, r("1", 100L, "Gus", "1", 50L, "order_1"));

        updateAfterOrder("1", 100L, "order_1");
        emitsNothing(); // No new match

        /* -------- Insert New User and Test Subsequent Order Update ----------- */
        // Insert another user who will match the current order (amount 100)
        insertUser("1", 200L, "Bob"); // 200 > 100 -> match
        emits(INSERT, r("1", 200L, "Bob", "1", 100L, "order_1"));

        // Update Order: amount from 100 to 250.
        updateBeforeOrder("1", 100L, "order_1");
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
     * Implements the non-equi join condition: inputs[indexToCompare].amount > inputs[index].amount.
     * Assumes amount is at field index 1.
     */
    private static class AmountGreaterThanConditionImpl extends AbstractRichFunction
            implements MultiJoinCondition {
        private final int leftInputIndex; // Index of the row for the left side of '>' in inputs[]
        private final int leftAmountFieldIndex; // Field index of amount in the left side row
        private final int rightInputIndex; // Index of the row for the right side of '>' in inputs[]
        private final int rightAmountFieldIndex; // Field index of amount in the right side row

        public AmountGreaterThanConditionImpl(
                int leftInputIndex,
                int leftAmountFieldIndex,
                int rightInputIndex,
                int rightAmountFieldIndex) {
            this.leftInputIndex = leftInputIndex;
            this.leftAmountFieldIndex = leftAmountFieldIndex;
            this.rightInputIndex = rightInputIndex;
            this.rightAmountFieldIndex = rightAmountFieldIndex;
        }

        @Override
        public boolean apply(RowData[] inputs) {
            if (inputs == null
                    || inputs.length <= Math.max(leftInputIndex, rightInputIndex)
                    || inputs[leftInputIndex] == null
                    || inputs[rightInputIndex] == null) {
                return false;
            }
            if (inputs[leftInputIndex].isNullAt(leftAmountFieldIndex)
                    || inputs[rightInputIndex].isNullAt(rightAmountFieldIndex)) {
                return false;
            }
            return inputs[leftInputIndex].getLong(leftAmountFieldIndex)
                    > inputs[rightInputIndex].getLong(rightAmountFieldIndex);
        }
    }

    /** Condition for u.user_id = o.user_id */
    private static class UserIdEqualsConditionImpl extends AbstractRichFunction
            implements MultiJoinCondition {
        private final int usersInputIndex; // e.g., 0 if inputs[0] is User row
        private final int usersIdField; // e.g., 0 for user_id
        private final int ordersInputIndex; // e.g., 1 if inputs[1] is Order row
        private final int ordersIdField; // e.g., 0 for user_id in Order

        public UserIdEqualsConditionImpl(
                int usersInputIndex, int usersIdField, int ordersInputIndex, int ordersIdField) {
            this.usersInputIndex = usersInputIndex;
            this.usersIdField = usersIdField;
            this.ordersInputIndex = ordersInputIndex;
            this.ordersIdField = ordersIdField;
        }

        @Override
        public boolean apply(RowData[] inputs) {
            if (inputs == null
                    || inputs.length <= Math.max(usersInputIndex, ordersInputIndex)
                    || inputs[usersInputIndex] == null
                    || inputs[ordersInputIndex] == null) {
                return false;
            }
            // Assuming IDs are strings and not null for this example.
            // Add comprehensive null checks as per your data model.
            if (inputs[usersInputIndex].isNullAt(usersIdField)
                    || inputs[ordersInputIndex].isNullAt(ordersIdField)) {
                // SQL null semantics: null != null is true, null = null is false.
                // For equals, if either is null, it's not equal unless both are null (which we'd typically treat as false for join keys).
                return false;
            }
            // Assuming user_id is String. Adjust if it's another type.
            String userId = inputs[usersInputIndex].getString(usersIdField).toString();
            String orderUserId = inputs[ordersInputIndex].getString(ordersIdField).toString();
            return userId.equals(orderUserId);
        }
    }

    /** Combines multiple MultiJoinConditions with AND logic. */
    private static class AndMultiJoinConditionImpl extends AbstractRichFunction
            implements MultiJoinCondition {
        private final List<MultiJoinCondition> conditions;

        public AndMultiJoinConditionImpl(MultiJoinCondition... conditions) {
            this.conditions = Arrays.asList(conditions);
        }

        @Override
        public boolean apply(RowData[] inputs) {
            for (MultiJoinCondition condition : conditions) {
                if (!condition.apply(inputs)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Creates a GeneratedMultiJoinCondition that checks Users.amount > Orders.amount.
     * For StreamingMultiJoinOperator, when input 1 (Orders) joins input 0 (Users),
     * in MultiJoinCondition.apply(RowData[] inputs): inputs[0] is User data, inputs[1] is Order data.
     *
     * @param usersInputInArray Index for Users row in the `inputs` array of `apply` method.
     * @param usersAmountField Field index for amount in Users row.
     * @param ordersInputInArray Index for Orders row in the `inputs` array.
     * @param ordersAmountField Field index for amount in Orders row.
     */
    protected static GeneratedMultiJoinCondition createAmountGreaterThanCondition(
            int usersInputInArray,
            int usersAmountField,
            int ordersInputInArray,
            int ordersAmountField) {
        String generatedClassName ="AmountGreaterThanCondition_manual";
        return new GeneratedMultiJoinCondition(generatedClassName, "", new Object[0]) {
            @Override
            public MultiJoinCondition newInstance(ClassLoader classLoader) {
                return new AmountGreaterThanConditionImpl(
                        usersInputInArray, usersAmountField, ordersInputInArray, ordersAmountField);
            }
        };
    }

    // Example of creating a combined condition (for illustration or if joinAttributeMap is not used for equi-join)
    protected static GeneratedMultiJoinCondition createFullJoinCondition(
            int usersInputInArray,
            int usersIdField,
            int usersAmountField,
            int ordersInputInArray,
            int ordersIdField,
            int ordersAmountField) {
        String generatedClassName = "FullJoinCondition_manual";
        return new GeneratedMultiJoinCondition(generatedClassName, "", new Object[0]) {
            @Override
            public MultiJoinCondition newInstance(ClassLoader classLoader) {
                MultiJoinCondition userIdCond = new UserIdEqualsConditionImpl(
                                usersInputInArray, usersIdField, ordersInputInArray, ordersIdField);
                MultiJoinCondition amountCond = new AmountGreaterThanConditionImpl(
                                usersInputInArray, usersAmountField, ordersInputInArray, ordersAmountField);
                return new AndMultiJoinConditionImpl(userIdCond, amountCond);
            }
        };
    }
}
