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
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator}
 * with four inputs, demonstrating different {@link
 * org.apache.flink.table.runtime.operators.join.stream.state.MultiJoinStateView} behaviors using
 * INNER joins. Input 0 (Users): PK user_id_0. Uses
 * JoinInputSideSpec.withUniqueKeyContainedByJoinKey. Input 1 (Orders): PK o_id_1. Uses
 * JoinInputSideSpec.withUniqueKey (as per recent user edits removing explicit override). Input 2
 * (Payments): PK pay_id_2. Uses JoinInputSideSpec.withUniqueKey. Input 3 (Shipments): No PK. Uses
 * JoinInputSideSpec.withoutUniqueKey. All inputs join on user_id_X fields.
 */
@ExtendWith(ParameterizedTestExtension.class)
class StreamingFourWayMixedInnerJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    // Join Conditions:
    // Input 1 (Orders) with Input 0 (Users) on user_id
    // Input 2 (Payments) with Input 0 (Users) on user_id
    // Input 3 (Shipments) with Input 0 (Users) on user_id
    private static final List<GeneratedJoinCondition> customJoinConditions =
            Arrays.asList(
                    null, // No condition for the first input (Users)
                    createJoinCondition(1, 0), // Orders.user_id_1 = Users.user_id_0
                    createJoinCondition(2, 0), // Payments.user_id_2 = Users.user_id_0
                    createJoinCondition(3, 0) // Shipments.user_id_3 = Users.user_id_0
                    );

    // Attribute Map for partitioning keys (all join on user_id, which is field 0)
    private static final Map<Integer, List<ConditionAttributeRef>> customAttributeMap =
            new HashMap<>();

    static {
        // Input 1 (Orders) with Input 0 (Users)
        customAttributeMap.put(1, Collections.singletonList(new ConditionAttributeRef(0, 0, 1, 0)));

        // Input 2 (Payments) with Input 0 (Users)
        customAttributeMap.put(2, Collections.singletonList(new ConditionAttributeRef(0, 0, 2, 0)));

        // Input 3 (Shipments) with Input 0 (Users)
        customAttributeMap.put(3, Collections.singletonList(new ConditionAttributeRef(0, 0, 3, 0)));
    }

    public StreamingFourWayMixedInnerJoinOperatorTest(StateBackendMode stateBackendMode) {
        super(
                stateBackendMode,
                4, // numInputs
                List.of(
                        FlinkJoinType.INNER,
                        FlinkJoinType.INNER,
                        FlinkJoinType.INNER,
                        FlinkJoinType.INNER), // joinTypes (first is placeholder for input0)
                customJoinConditions,
                customAttributeMap,
                false // isFullOuterJoin
                );

        // Note: User edits removed explicit overrides for inputSpec[0,1,2].
        // They will now default to:
        // Input 0: withUniqueKeyContainedByJoinKey (from base class logic)
        // Input 1: withUniqueKey (from base class logic for index > 0)
        // Input 2: withUniqueKey (from base class logic for index > 0)

        // Input 3 (Shipments): Must be withoutUniqueKey, explicitly set as per user edit.
        this.inputSpecs.set(3, JoinInputSideSpec.withoutUniqueKey());
    }

    /**
     * Test simple append-only data for the 4-way inner join.
     *
     * <p>SQL Equivalent:
     *
     * <pre>
     * SELECT u.*, o.*, p.*, s.*
     * FROM Users u
     * INNER JOIN Orders o ON u.user_id_0 = o.user_id_1
     * INNER JOIN Payments p ON u.user_id_0 = p.user_id_2
     * INNER JOIN Shipments s ON u.user_id_0 = s.user_id_3
     * </pre>
     */
    @TestTemplate
    void testFourWayMixedInnerJoin() throws Exception {
        // Schema:
        // Users(user_id_0, u_id_0, u_details_0)
        // Orders(user_id_1, o_id_1, o_details_1)
        // Payments(user_id_2, pay_id_2, pay_details_2)
        // Shipments(user_id_3, s_id_3, s_details_3)

        /* -------- INITIAL INSERTS - NO EMIT UNTIL ALL FOUR JOIN PARTNERS ARRIVE ----------- */
        insertUser("uid1", "u1", "User 1");
        emitsNothing();

        insertOrder("uid1", "o1", "Order 1");
        emitsNothing();

        insertPayment("uid1", "pay1", "Payment 1");
        emitsNothing();

        insertShipment("uid1", "s1", "Shipment 1");
        emits(
                INSERT,
                "uid1",
                "u1",
                "User 1",
                "uid1",
                "o1",
                "Order 1",
                "uid1",
                "pay1",
                "Payment 1",
                "uid1",
                "s1",
                "Shipment 1");

        /* -------- SECOND SET OF DATA ----------- */
        insertUser("uid2", "u2", "User 2");
        insertOrder("uid2", "o2", "Order 2");
        insertPayment("uid2", "pay2", "Payment 2");
        emitsNothing();
        insertShipment("uid2", "s2", "Shipment 2");
        emits(
                INSERT,
                "uid2",
                "u2",
                "User 2",
                "uid2",
                "o2",
                "Order 2",
                "uid2",
                "pay2",
                "Payment 2",
                "uid2",
                "s2",
                "Shipment 2");

        /* -------- ADDITIONAL DATA FOR EXISTING JOIN KEYS ----------- */
        // Insert a second order for uid1, should join with existing U1, P1, S1
        insertOrder("uid1", "o1_extra", "Order 1 Extra");
        emits(
                INSERT,
                "uid1",
                "u1",
                "User 1",
                "uid1",
                "o1_extra",
                "Order 1 Extra",
                "uid1",
                "pay1",
                "Payment 1",
                "uid1",
                "s1",
                "Shipment 1");

        // Insert a second shipment for uid1 (no unique key on shipment)
        // This should join with U1, O1, P1 and also with U1, O1_extra, P1
        insertShipment("uid1", "s1_another", "Shipment 1 Another");
        emits(
                INSERT,
                r(
                        "uid1",
                        "u1",
                        "User 1",
                        "uid1",
                        "o1",
                        "Order 1",
                        "uid1",
                        "pay1",
                        "Payment 1",
                        "uid1",
                        "s1_another",
                        "Shipment 1 Another"),
                INSERT,
                r(
                        "uid1",
                        "u1",
                        "User 1",
                        "uid1",
                        "o1_extra",
                        "Order 1 Extra",
                        "uid1",
                        "pay1",
                        "Payment 1",
                        "uid1",
                        "s1_another",
                        "Shipment 1 Another"));
    }

    /**
     * Test changelog operations (updates, deletes) for the 4-way inner join. Despite the method
     * name containing "LeftJoin", this test currently uses INNER joins as defined in the class
     * constructor. It demonstrates various data transitions.
     *
     * <p>SQL Equivalent: (Same as inner join test)
     *
     * <pre>
     * SELECT u.*, o.*, p.*, s.*
     * FROM Users u
     * INNER JOIN Orders o ON u.user_id_0 = o.user_id_1
     * INNER JOIN Payments p ON u.user_id_0 = p.user_id_2
     * INNER JOIN Shipments s ON u.user_id_0 = s.user_id_3
     * </pre>
     */
    @TestTemplate
    void testFourWayMixedInnerJoinUpdating() throws Exception {
        /* -------- SETUP INITIAL JOIN ----------- */
        insertUser("uid1", "u1", "User 1");
        insertOrder("uid1", "o1", "Order 1");
        insertPayment("uid1", "pay1", "Payment 1");
        insertShipment("uid1", "s1", "Shipment 1");
        emits(
                INSERT,
                "uid1",
                "u1",
                "User 1",
                "uid1",
                "o1",
                "Order 1",
                "uid1",
                "pay1",
                "Payment 1",
                "uid1",
                "s1",
                "Shipment 1");

        /* -------- 1. USER (INPUT 0) DELETION AND RE-INSERTION ----------- */
        deleteUser("uid1", "u1", "User 1");
        emits(
                DELETE,
                "uid1",
                "u1",
                "User 1",
                "uid1",
                "o1",
                "Order 1",
                "uid1",
                "pay1",
                "Payment 1",
                "uid1",
                "s1",
                "Shipment 1");

        insertUser("uid1", "u1_new", "User 1 New");
        emits(
                INSERT,
                "uid1",
                "u1_new",
                "User 1 New",
                "uid1",
                "o1",
                "Order 1",
                "uid1",
                "pay1",
                "Payment 1",
                "uid1",
                "s1",
                "Shipment 1");

        /* -------- 2. ORDER (INPUT 1) DELETION AND RE-INSERTION ----------- */
        deleteOrder("uid1", "o1", "Order 1");
        emits(
                DELETE,
                "uid1",
                "u1_new",
                "User 1 New",
                "uid1",
                "o1",
                "Order 1",
                "uid1",
                "pay1",
                "Payment 1",
                "uid1",
                "s1",
                "Shipment 1");

        insertOrder("uid1", "o1_new", "Order 1 New");
        emits(
                INSERT,
                "uid1",
                "u1_new",
                "User 1 New",
                "uid1",
                "o1_new",
                "Order 1 New",
                "uid1",
                "pay1",
                "Payment 1",
                "uid1",
                "s1",
                "Shipment 1");

        /* -------- 3. PAYMENT (INPUT 2) DELETION AND RE-INSERTION ----------- */
        deletePayment("uid1", "pay1", "Payment 1");
        emits(
                DELETE,
                "uid1",
                "u1_new",
                "User 1 New",
                "uid1",
                "o1_new",
                "Order 1 New",
                "uid1",
                "pay1",
                "Payment 1",
                "uid1",
                "s1",
                "Shipment 1");

        insertPayment("uid1", "pay1_new", "Payment 1 New");
        emits(
                INSERT,
                "uid1",
                "u1_new",
                "User 1 New",
                "uid1",
                "o1_new",
                "Order 1 New",
                "uid1",
                "pay1_new",
                "Payment 1 New",
                "uid1",
                "s1",
                "Shipment 1");

        /* -------- 4. SHIPMENT (INPUT 3 - NO UNIQUE KEY) DELETION AND RE-INSERTION ----------- */
        deleteShipment("uid1", "s1", "Shipment 1"); // Simulates UB for no-PK
        emits(
                DELETE,
                "uid1",
                "u1_new",
                "User 1 New",
                "uid1",
                "o1_new",
                "Order 1 New",
                "uid1",
                "pay1_new",
                "Payment 1 New",
                "uid1",
                "s1",
                "Shipment 1");

        insertShipment("uid1", "s1_new", "Shipment 1 New"); // Simulates UA for no-PK
        emits(
                INSERT,
                "uid1",
                "u1_new",
                "User 1 New",
                "uid1",
                "o1_new",
                "Order 1 New",
                "uid1",
                "pay1_new",
                "Payment 1 New",
                "uid1",
                "s1_new",
                "Shipment 1 New");

        /* -------- 5. UPDATES TO EACH INPUT ----------- */
        updateBeforeUser("uid1", "u1_new", "User 1 New");
        emits(
                UPDATE_BEFORE,
                "uid1",
                "u1_new",
                "User 1 New", // Old User
                "uid1",
                "o1_new",
                "Order 1 New",
                "uid1",
                "pay1_new",
                "Payment 1 New",
                "uid1",
                "s1_new",
                "Shipment 1 New");
        updateAfterUser("uid1", "u1_updated", "User 1 Updated");
        emits(
                UPDATE_AFTER,
                "uid1",
                "u1_updated",
                "User 1 Updated", // New User
                "uid1",
                "o1_new",
                "Order 1 New",
                "uid1",
                "pay1_new",
                "Payment 1 New",
                "uid1",
                "s1_new",
                "Shipment 1 New");

        updateBeforeOrder("uid1", "o1_new", "Order 1 New");
        emits(
                UPDATE_BEFORE,
                "uid1",
                "u1_updated",
                "User 1 Updated",
                "uid1",
                "o1_new",
                "Order 1 New", // Old Order
                "uid1",
                "pay1_new",
                "Payment 1 New",
                "uid1",
                "s1_new",
                "Shipment 1 New");
        updateAfterOrder("uid1", "o1_updated", "Order 1 Updated");
        emits(
                UPDATE_AFTER,
                "uid1",
                "u1_updated",
                "User 1 Updated",
                "uid1",
                "o1_updated",
                "Order 1 Updated", // New Order
                "uid1",
                "pay1_new",
                "Payment 1 New",
                "uid1",
                "s1_new",
                "Shipment 1 New");

        updateBeforePayment("uid1", "pay1_new", "Payment 1 New");
        emits(
                UPDATE_BEFORE,
                "uid1",
                "u1_updated",
                "User 1 Updated",
                "uid1",
                "o1_updated",
                "Order 1 Updated",
                "uid1",
                "pay1_new",
                "Payment 1 New", // Old Payment
                "uid1",
                "s1_new",
                "Shipment 1 New");
        updateAfterPayment("uid1", "pay1_updated", "Payment 1 Updated");
        emits(
                UPDATE_AFTER,
                "uid1",
                "u1_updated",
                "User 1 Updated",
                "uid1",
                "o1_updated",
                "Order 1 Updated",
                "uid1",
                "pay1_updated",
                "Payment 1 Updated", // New Payment
                "uid1",
                "s1_new",
                "Shipment 1 New");

        // Update for Shipment (Input 3 - no unique key)
        // Treated as Delete + Insert
        deleteShipment("uid1", "s1_new", "Shipment 1 New");
        emits(
                DELETE,
                "uid1",
                "u1_updated",
                "User 1 Updated",
                "uid1",
                "o1_updated",
                "Order 1 Updated",
                "uid1",
                "pay1_updated",
                "Payment 1 Updated",
                "uid1",
                "s1_new",
                "Shipment 1 New"); // Old Shipment (deleted)

        insertShipment("uid1", "s1_updated", "Shipment 1 Updated");
        emits(
                INSERT,
                "uid1",
                "u1_updated",
                "User 1 Updated",
                "uid1",
                "o1_updated",
                "Order 1 Updated",
                "uid1",
                "pay1_updated",
                "Payment 1 Updated",
                "uid1",
                "s1_updated",
                "Shipment 1 Updated"); // New Shipment (inserted)

        /* -------- 6. MULTIPLE RECORDS FOR NON-UNIQUE KEY INPUT (SHIPMENTS) ----------- */
        insertShipment("uid1", "s2_another", "Shipment 2 Another");
        emits(
                INSERT,
                "uid1",
                "u1_updated",
                "User 1 Updated",
                "uid1",
                "o1_updated",
                "Order 1 Updated",
                "uid1",
                "pay1_updated",
                "Payment 1 Updated",
                "uid1",
                "s2_another",
                "Shipment 2 Another");

        // Now we have U1-O1-P1-S_updated and U1-O1-P1-S2_another
        // Delete Payment (Input 2)
        deletePayment("uid1", "pay1_updated", "Payment 1 Updated");
        emits(
                DELETE,
                r(
                        "uid1",
                        "u1_updated",
                        "User 1 Updated",
                        "uid1",
                        "o1_updated",
                        "Order 1 Updated",
                        "uid1",
                        "pay1_updated",
                        "Payment 1 Updated",
                        "uid1",
                        "s1_updated",
                        "Shipment 1 Updated"),
                DELETE,
                r(
                        "uid1",
                        "u1_updated",
                        "User 1 Updated",
                        "uid1",
                        "o1_updated",
                        "Order 1 Updated",
                        "uid1",
                        "pay1_updated",
                        "Payment 1 Updated",
                        "uid1",
                        "s2_another",
                        "Shipment 2 Another"));

        // Re-insert Payment, should join with both shipments again
        insertPayment("uid1", "pay1_final", "Payment 1 Final");
        emits(
                INSERT,
                r(
                        "uid1",
                        "u1_updated",
                        "User 1 Updated",
                        "uid1",
                        "o1_updated",
                        "Order 1 Updated",
                        "uid1",
                        "pay1_final",
                        "Payment 1 Final",
                        "uid1",
                        "s1_updated",
                        "Shipment 1 Updated"),
                INSERT,
                r(
                        "uid1",
                        "u1_updated",
                        "User 1 Updated",
                        "uid1",
                        "o1_updated",
                        "Order 1 Updated",
                        "uid1",
                        "pay1_final",
                        "Payment 1 Final",
                        "uid1",
                        "s2_another",
                        "Shipment 2 Another"));
    }
}
