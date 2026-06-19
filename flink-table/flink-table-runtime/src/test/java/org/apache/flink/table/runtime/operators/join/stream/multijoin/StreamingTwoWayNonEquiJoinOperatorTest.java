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
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests for {@link StreamingMultiJoinOperator} with non-equi join conditions. */
@ExtendWith(ParameterizedTestExtension.class)
class StreamingTwoWayNonEquiJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    private static final GeneratedJoinCondition EqualIdAndGreaterAmountCondition =
            createAndCondition(
                    createJoinCondition(1, 0), // equi-join on user_id
                    createFieldLongGreaterThanCondition(
                            1, 1) // non-equi: users.amount > orders.amount
                    );

    private static final List<GeneratedJoinCondition> customJoinConditions =
            Arrays.asList(null, EqualIdAndGreaterAmountCondition);

    private static final Map<Integer, List<ConditionAttributeRef>> customAttributeMap =
            new HashMap<>();

    static {
        customAttributeMap.put(1, Collections.singletonList(new ConditionAttributeRef(0, 0, 1, 0)));
    }

    public StreamingTwoWayNonEquiJoinOperatorTest(StateBackendMode stateBackendMode) {
        super(
                stateBackendMode,
                2, // numInputs
                List.of(FlinkJoinType.INNER, FlinkJoinType.INNER), // joinTypes
                customJoinConditions,
                customAttributeMap,
                false // isFullOuterJoin
                );
    }

    /**
     * SELECT u.*, o.* FROM Users u INNER JOIN Orders o ON u.user_id = o.user_id AND u.amount >
     * o.amount.
     */
    @TestTemplate
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
    @TestTemplate
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
    @TestTemplate
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
    protected RowType createInputTypeInfo(int inputIndex) {
        if (inputIndex == 0) { // Users: user_id (VARCHAR), amount (BIGINT), name (VARCHAR)
            return RowType.of(
                    new LogicalType[] {
                        VarCharType.STRING_TYPE, new BigIntType(), VarCharType.STRING_TYPE
                    },
                    new String[] {"user_id_0", "amount_0", "name_0"});
        } else { // Orders: order_user_id (VARCHAR), amount (BIGINT), order_id (VARCHAR)
            return RowType.of(
                    new LogicalType[] {
                        VarCharType.STRING_TYPE, new BigIntType(), VarCharType.STRING_TYPE
                    },
                    new String[] {"user_id_1", "amount_1", "order_id_1"});
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
                inputTypeInfos.get(inputIndex).getChildren().toArray(new LogicalType[0]));
    }

    /**
     * Implements the non-equi join condition: inputs[indexToCompare].amount > inputs[index].amount.
     * Assumes amount is at field index 1.
     */
    private static class AmountGreaterThanConditionImpl extends AbstractRichFunction
            implements JoinCondition {
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
        public boolean apply(RowData left, RowData right) {
            if (left == null || right == null) {
                return false;
            }
            if (left.isNullAt(leftAmountFieldIndex) || right.isNullAt(rightAmountFieldIndex)) {
                return false;
            }
            return left.getLong(leftAmountFieldIndex) > right.getLong(rightAmountFieldIndex);
        }
    }

    /** Condition for u.user_id = o.user_id. */
    private static class UserIdEqualsConditionImpl extends AbstractRichFunction
            implements JoinCondition {
        private final int usersInputId; // e.g., 0 if inputs[0] is User row
        private final int usersIdField; // e.g., 0 for user_id
        private final int ordersInputId; // e.g., 1 if inputs[1] is Order row
        private final int ordersIdField; // e.g., 0 for user_id in Order

        public UserIdEqualsConditionImpl(
                int usersInputId, int usersIdField, int ordersInputId, int ordersIdField) {
            this.usersInputId = usersInputId;
            this.usersIdField = usersIdField;
            this.ordersInputId = ordersInputId;
            this.ordersIdField = ordersIdField;
        }

        @Override
        public boolean apply(RowData left, RowData right) {
            if (left == null || right == null) {
                return false;
            }
            // Assuming IDs are strings and not null for this example.
            // Add comprehensive null checks as per your data model.
            if (left.isNullAt(usersIdField) || right.isNullAt(ordersIdField)) {
                // SQL null semantics: null != null is true, null = null is false.
                // For equals, if either is null, it's not equal unless both are null (which we'd
                // typically treat as false for join keys).
                return false;
            }
            // Assuming user_id is String. Adjust if it's another type.
            String userId = left.getString(usersIdField).toString();
            String orderUserId = right.getString(ordersIdField).toString();
            return userId.equals(orderUserId);
        }
    }

    /** Combines multiple JoinConditions with AND logic. */
    private static class AndJoinConditionImpl extends AbstractRichFunction
            implements JoinCondition {
        private final List<JoinCondition> conditions;

        public AndJoinConditionImpl(JoinCondition... conditions) {
            this.conditions = Arrays.asList(conditions);
        }

        @Override
        public boolean apply(RowData left, RowData right) {
            for (JoinCondition condition : conditions) {
                if (!condition.apply(left, right)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Creates a GeneratedJoinCondition that checks Users.amount > Orders.amount. For
     * StreamingMultiJoinOperator, when input 1 (Orders) joins input 0 (Users), in
     * JoinCondition.apply(RowData[] inputs): inputs[0] is User data, inputs[1] is Order data.
     *
     * @param usersInputInArray Index for Users row in the `inputs` array of `apply` method.
     * @param usersAmountField Field index for amount in Users row.
     * @param ordersInputInArray Index for Orders row in the `inputs` array.
     * @param ordersAmountField Field index for amount in Orders row.
     */
    protected static GeneratedJoinCondition createAmountGreaterThanCondition(
            int usersInputInArray,
            int usersAmountField,
            int ordersInputInArray,
            int ordersAmountField) {
        String generatedClassName = "AmountGreaterThanCondition_manual";
        return new GeneratedJoinCondition(generatedClassName, "", new Object[0]) {
            @Override
            public JoinCondition newInstance(ClassLoader classLoader) {
                return new AmountGreaterThanConditionImpl(
                        usersInputInArray, usersAmountField, ordersInputInArray, ordersAmountField);
            }
        };
    }
}
