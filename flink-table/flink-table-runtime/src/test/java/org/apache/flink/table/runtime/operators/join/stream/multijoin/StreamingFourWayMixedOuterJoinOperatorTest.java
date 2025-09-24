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
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
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
 * with four inputs using a mix of LEFT and INNER JOINs, demonstrating different {@link
 * org.apache.flink.table.runtime.operators.join.stream.state.MultiJoinStateView} behaviors.
 *
 * <p>Join Structure: Users u LEFT JOIN Orders o ON u.uid = o.uid INNER JOIN Payments p ON u.uid =
 * p.uid LEFT JOIN Shipments s ON u.uid = s.uid
 *
 * <p>Input 0 (Users): PK user_id_0. Uses JoinInputSideSpec.withUniqueKeyContainedByJoinKey.
 *
 * <p>Input 1 (Orders): PK o_id_1. Uses JoinInputSideSpec.withUniqueKey.
 *
 * <p>Input 2 (Payments): PK pay_id_2. Uses JoinInputSideSpec.withUniqueKey.
 *
 * <p>Input 3 (Shipments): No PK. Uses JoinInputSideSpec.withoutUniqueKey.
 *
 * <p>All inputs join on their respective user_id_X fields with Users.user_id_0.
 */
@ExtendWith(ParameterizedTestExtension.class)
class StreamingFourWayMixedOuterJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    private static final List<GeneratedJoinCondition> customJoinConditions;

    static {
        // Condition for Input 3 (Shipments) LEFT JOIN Input 0 (Users)
        // ON u.user_id_0 = s.user_id_3 AND u.details_0 > s.details_3
        GeneratedJoinCondition shipmentsJoinCondition =
                createAndCondition(
                        createJoinCondition(
                                3, 0), // equi-join on user_id (field 0 of input 3 with field 0 of
                        // input 0)
                        createFieldLongGreaterThanCondition(
                                2, 2) // non-equi: users.details_0 (field 2 from left side) >
                        // shipments.details_3 (field 2 from right side)
                        // (field 2)
                        );

        customJoinConditions =
                Arrays.asList(
                        null, // Users (Input 0)
                        createJoinCondition(
                                1, 0), // Orders (Input 1) JOIN Users (Input 0) on user_id
                        createJoinCondition(
                                2, 0), // Payments (Input 2) JOIN Users (Input 0) on user_id
                        shipmentsJoinCondition // Shipments (Input 3) JOIN Users (Input 0) with
                        // combined condition
                        );
    }

    private static final Map<Integer, List<ConditionAttributeRef>> customAttributeMap =
            new HashMap<>();

    static {
        customAttributeMap.put(1, Collections.singletonList(new ConditionAttributeRef(0, 0, 1, 0)));

        customAttributeMap.put(2, Collections.singletonList(new ConditionAttributeRef(0, 0, 2, 0)));

        customAttributeMap.put(3, Collections.singletonList(new ConditionAttributeRef(0, 0, 3, 0)));
    }

    public StreamingFourWayMixedOuterJoinOperatorTest(StateBackendMode stateBackendMode) {
        super(
                stateBackendMode,
                4, // numInputs
                List.of(
                        FlinkJoinType.INNER, // Placeholder for Users (Input 0)
                        FlinkJoinType.LEFT, // Orders (Input 1)
                        FlinkJoinType.INNER, // Payments (Input 2)
                        FlinkJoinType.LEFT // Shipments (Input 3)
                        ),
                customJoinConditions,
                customAttributeMap,
                false // isFullOuterJoin
                );

        // Input 0 (Users): Defaults to withUniqueKeyContainedByJoinKey (Base class logic)
        // Input 1 (Orders): Defaults to withUniqueKey (Base class logic)
        // Input 2 (Payments): Defaults to withUniqueKey (Base class logic)
        // Input 3 (Shipments): Explicitly set to withoutUniqueKey
        this.inputSpecs.set(3, JoinInputSideSpec.withoutUniqueKey());
    }

    /**
     * Test various transitions for a 4-way mixed JOIN (LEFT, INNER, LEFT).
     *
     * <p>SQL Equivalent:
     *
     * <pre>
     * SELECT u.*, o.*, p.*, s.*
     * FROM Users u
     * LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1
     * INNER JOIN Payments p ON u.user_id_0 = p.user_id_2
     * LEFT JOIN Shipments s ON u.user_id_0 = s.user_id_3 AND u.details_0 > s.details_3
     * </pre>
     */
    @TestTemplate
    void testFourWayMixedJoin() throws Exception {
        // Schemas:
        // User(joinKey:String, pk:String, details:Long)
        // Order(joinKey:String, pk:String, details:String)
        // Payment(joinKey:String, pk:String, details:String)
        // Shipment(joinKey:String, pk:String, details:Long)
        // Output row: u1,u2,u3, o1,o2,o3, p1,p2,p3, s1,s2,s3 (12 fields total)

        /* -------- 1. INITIAL USER INSERTION ----------- */
        insertUser("1", "user1", 100L); // User Join Key "1", PK "user1", details 100
        emitsNothing(); // Payment is INNER JOIN, nothing emits yet

        /* -------- 2. INSERTING OTHER INPUTS (NO MATCHING USER OR REQUIRED INNER PARTNER) ----------- */
        insertOrder("99", "order99", "Order 99"); // Different join key
        emitsNothing();
        insertPayment("99", "payment99", "Payment 99"); // Different join key
        emitsNothing(); // No matching user for this payment
        insertShipment("99", "shipment99", 50L); // Different join key, details 50
        emitsNothing();

        /* -------- 3. JOINING OTHER INPUTS TO EXISTING USER ----------- */
        // User: ("1", "user1", 100L)
        // Add Order for User "1"
        insertOrder("1", "order1", "Order 1"); // Order Join Key "1", PK "order1"
        emitsNothing(); // Payment for User "1" still missing

        // Add Payment for User "1" (Order "order1" already present for User "1")
        // This completes User INNER JOIN Payment. Order is LEFT JOIN. Shipment is null (no shipment
        // yet).
        insertPayment("1", "payment1", "Payment 1"); // Payment Join Key "1", PK "payment1"
        emits(
                INSERT, // Emit (User, Order, Payment, null_Shipment)
                r(
                        "1",
                        "user1",
                        100L,
                        "1",
                        "order1",
                        "Order 1",
                        "1",
                        "payment1",
                        "Payment 1",
                        null,
                        null,
                        null));

        // Add Shipment for User "1" with details_3 = 50. (User.details_0 = 100 > Shipment.details_3
        // = 50 is TRUE)
        insertShipment("1", "shipment1", 50L); // Shipment Join Key "1", PK "shipment1", details 50
        emits(
                DELETE, // Retract (User, Order, Payment, null_Shipment)
                r(
                        "1",
                        "user1",
                        100L,
                        "1",
                        "order1",
                        "Order 1",
                        "1",
                        "payment1",
                        "Payment 1",
                        null,
                        null,
                        null),
                INSERT, // Emit (User, Order, Payment, Shipment_shipment1)
                r(
                        "1",
                        "user1",
                        100L,
                        "1",
                        "order1",
                        "Order 1",
                        "1",
                        "payment1",
                        "Payment 1",
                        "1",
                        "shipment1",
                        50L));

        // Add another Shipment for User "1" with details_3 = 150. (User.details_0 = 100 >
        // Shipment.details_3 = 150 is FALSE)
        // This shipment should not join due to non-equi condition. The existing join with shipment1
        // remains.
        insertShipment("1", "shipment2_no_join", 150L);
        emitsNothing(); // No change to the output as shipment2 does not satisfy u.details >
        // s.details

        /* -------- 4. DELETING JOINED INPUTS ----------- */
        // Current state: (U("1","user1",100L), O("1","order1"), P("1","payment1"),
        // S("1","shipment1",50L))
        // Shipment shipment2_no_join (150L) is in state but not joined.

        // Delete Shipment shipment1 (details 50L) (LEFT JOIN)
        deleteShipment("1", "shipment1", 50L);
        emits(
                DELETE, // Retract (U, O, P, S_shipment1)
                r(
                        "1",
                        "user1",
                        100L,
                        "1",
                        "order1",
                        "Order 1",
                        "1",
                        "payment1",
                        "Payment 1",
                        "1",
                        "shipment1",
                        50L),
                INSERT, // Emit (U, O, P, null_Shipment) as shipment2_no_join (150L) doesn't match
                // 100L > 150L
                r(
                        "1",
                        "user1",
                        100L,
                        "1",
                        "order1",
                        "Order 1",
                        "1",
                        "payment1",
                        "Payment 1",
                        null,
                        null,
                        null));

        // Current state: (U("1","user1",100L), O("1","order1"), P("1","payment1"), null_S)
        // Shipment shipment2_no_join (150L) is still in state.

        // Delete Payment (INNER JOIN)
        deletePayment("1", "payment1", "Payment 1");
        emits(
                DELETE, // Retract (U, O, P, null_S)
                r(
                        "1",
                        "user1",
                        100L,
                        "1",
                        "order1",
                        "Order 1",
                        "1",
                        "payment1",
                        "Payment 1",
                        null,
                        null,
                        null));

        // Current state: User "1" (user1, 100L), Order "1" (order1) exist. Payment "1" is gone. No
        // active join.
        // Shipment shipment2_no_join (150L) is still in state.
        // Delete Order (LEFT JOIN)
        deleteOrder("1", "order1", "Order 1");
        emitsNothing(); // No active join to modify as Payment "1" is gone.

        /* -------- 5. RE-ADDING AND USER DELETE ----------- */
        // Current state: User "1" (user1, 100L) exists. Order, Payment for key "1" are gone.
        // Shipment shipment2_no_join ("1", "shipment2_no_join", 150L) is in Shipments state.

        insertOrder("1", "order2", "Order 2 for User 1"); // PK "order2"
        emitsNothing(); // Payment is still missing for INNER JOIN

        insertPayment("1", "payment2", "Payment 2 for User 1"); // PK "payment2"
        // U("1","user1",100L), O("1","order2") (LEFT), P("1","payment2") (INNER)
        // Shipment shipment2_no_join (150L) is present but 100L > 150L is false, so S=null
        emits(
                INSERT,
                r(
                        "1",
                        "user1",
                        100L,
                        "1",
                        "order2",
                        "Order 2 for User 1",
                        "1",
                        "payment2",
                        "Payment 2 for User 1",
                        null,
                        null,
                        null));

        // Add a new shipment shipment3 that *does* satisfy u.details > s.details (100L > 75L)
        insertShipment("1", "shipment3_joins", 75L); // PK "shipment3_joins", details 75L
        emits(
                DELETE,
                        r(
                                "1",
                                "user1",
                                100L,
                                "1",
                                "order2",
                                "Order 2 for User 1",
                                "1",
                                "payment2",
                                "Payment 2 for User 1",
                                null,
                                null,
                                null),
                INSERT,
                        r(
                                "1",
                                "user1",
                                100L,
                                "1",
                                "order2",
                                "Order 2 for User 1",
                                "1",
                                "payment2",
                                "Payment 2 for User 1",
                                "1",
                                "shipment3_joins",
                                75L));

        // Delete User - retracts the full join with shipment3_joins
        deleteUser("1", "user1", 100L);
        emits(
                DELETE,
                r(
                        "1",
                        "user1",
                        100L,
                        "1",
                        "order2",
                        "Order 2 for User 1",
                        "1",
                        "payment2",
                        "Payment 2 for User 1",
                        "1",
                        "shipment3_joins",
                        75L));
        // shipment2_no_join and shipment3_joins remain in Shipment state but no longer join with
        // deleted user.

        /* -------- 6. UPDATES ----------- */
        // Setup new join: User("2","user2",200L), Order("2","order3"), Payment("2","payment3")
        // Shipment("2","shipment_low_detail", 50L) -> joins (200L > 50L)
        // Shipment("2","shipment_high_detail", 250L) -> no join (200L > 250L is false)

        insertUser("2", "user2", 200L);
        emitsNothing();
        insertOrder("2", "order3", "Order 3 for User 2");
        emitsNothing(); // Payment "2" still missing
        insertPayment("2", "payment3", "Payment 3 for User 2");
        // At this point, User, Order, Payment are matched. No shipment yet that joins.
        emits(
                INSERT,
                r(
                        "2",
                        "user2",
                        200L,
                        "2",
                        "order3",
                        "Order 3 for User 2",
                        "2",
                        "payment3",
                        "Payment 3 for User 2",
                        null,
                        null,
                        null));

        insertShipment("2", "shipment_low_detail", 50L); // Joins (200L > 50L)
        emits(
                DELETE,
                        r(
                                "2",
                                "user2",
                                200L,
                                "2",
                                "order3",
                                "Order 3 for User 2",
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                null,
                                null,
                                null),
                INSERT,
                        r(
                                "2",
                                "user2",
                                200L,
                                "2",
                                "order3",
                                "Order 3 for User 2",
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_low_detail",
                                50L));

        insertShipment("2", "shipment_high_detail", 250L); // Does not join (200L > 250L is false)
        emitsNothing(); // No change in output as this shipment does not satisfy the non-equi
        // condition

        // Current state: (U("2","user2",200L), O("2","order3"), P("2","payment3"),
        // S("2","shipment_low_detail",50L))
        // Shipment shipment_high_detail (250L) is in state but not joined.

        // Update User details: u.details_0 from 200L to 300L
        // Now u.details_0 (300L) > shipment_low_detail.details_3 (50L) -> remains joined
        // Now u.details_0 (300L) > shipment_high_detail.details_3 (250L) -> shipment_high_detail
        // now joins!
        updateBeforeUser("2", "user2", 200L);
        emits(
                UPDATE_BEFORE,
                r(
                        "2",
                        "user2",
                        200L,
                        "2",
                        "order3",
                        "Order 3 for User 2",
                        "2",
                        "payment3",
                        "Payment 3 for User 2",
                        "2",
                        "shipment_low_detail",
                        50L));
        updateAfterUser("2", "user2_updated", 300L); // PK change also, new user details 300L
        emits(
                UPDATE_AFTER,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3",
                                "Order 3 for User 2",
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_low_detail",
                                50L),
                UPDATE_AFTER,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3",
                                "Order 3 for User 2",
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_high_detail",
                                250L));

        // Current state: (U("2","user2_updated",300L), O("2","order3"), P("2","payment3"),
        // S_low(50L), S_high(250L))
        // Both shipments are joined as 300 > 50 and 300 > 250.

        // Update Order (LEFT JOIN) - no change to shipment joins
        updateBeforeOrder("2", "order3", "Order 3 for User 2");
        emits(
                UPDATE_BEFORE,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3",
                                "Order 3 for User 2",
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_low_detail",
                                50L),
                UPDATE_BEFORE,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3",
                                "Order 3 for User 2",
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_high_detail",
                                250L),
                INSERT,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                null,
                                null,
                                null,
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_low_detail",
                                50L),
                INSERT,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                null,
                                null,
                                null,
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_high_detail",
                                250L));
        updateAfterOrder("2", "order3_updated", "Order 3 Updated");
        emits(
                DELETE,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                null,
                                null,
                                null,
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_low_detail",
                                50L),
                DELETE,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                null,
                                null,
                                null,
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_high_detail",
                                250L),
                UPDATE_AFTER,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_low_detail",
                                50L),
                UPDATE_AFTER,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_high_detail",
                                250L));

        // Current state: (U("2","user2_updated",300L), O("2","order3_updated"), P("2","payment3"),
        // S_low(50L), S_high(250L))
        // Update Payment (INNER JOIN) - no change to shipment joins as long as payment is present
        updateBeforePayment("2", "payment3", "Payment 3 for User 2");
        emits(
                UPDATE_BEFORE,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_low_detail",
                                50L),
                UPDATE_BEFORE,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3",
                                "Payment 3 for User 2",
                                "2",
                                "shipment_high_detail",
                                250L));
        updateAfterPayment("2", "payment3_updated", "Payment 3 Updated");
        emits(
                UPDATE_AFTER,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3_updated",
                                "Payment 3 Updated",
                                "2",
                                "shipment_low_detail",
                                50L),
                UPDATE_AFTER,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3_updated",
                                "Payment 3 Updated",
                                "2",
                                "shipment_high_detail",
                                250L));

        // Current state: (U("2","user2_updated",300L), O("2","order3_updated"),
        // P("2","payment3_updated"), S_low(50L), S_high(250L))
        // Update one of the Shipments (shipment_low_detail) so its detail becomes > u.details_0
        // (50L -> 350L)
        // This specific shipment (shipment_low_detail) should stop joining. shipment_high_detail
        // (250L) remains joined (300L > 250L).
        deleteShipment(
                "2", "shipment_low_detail", 50L); // This is effectively an update for non-UK input
        emits(
                DELETE,
                r(
                        "2",
                        "user2_updated",
                        300L,
                        "2",
                        "order3_updated",
                        "Order 3 Updated",
                        "2",
                        "payment3_updated",
                        "Payment 3 Updated",
                        "2",
                        "shipment_low_detail",
                        50L)
                // S_high remains joined, so no new INSERT with null S_low is emitted immediately,
                // output reflects remaining S_high join
                );
        insertShipment(
                "2",
                "shipment_low_now_high_detail",
                350L); // New PK, details 350L. (300L > 350L is FALSE)
        emitsNothing(); // This new shipment does not join. Output still reflects only S_high join.

        /* -------- 7. MULTIPLE RECORDS FOR NON-UNIQUE KEY INPUT (SHIPMENTS) ----------- */
        // Current state: U("2","user2_updated",300L), O("2","order3_updated"),
        // P("2","payment3_updated"), S_high(250L)
        // shipment_low_now_high_detail(350L) is in state but not joined.

        // Add another shipment that joins: shipment_another_low_detail (details 100L). (300L > 100L
        // is TRUE)
        insertShipment("2", "shipment_another_low_detail", 100L);
        emits(
                INSERT,
                r(
                        "2",
                        "user2_updated",
                        300L,
                        "2",
                        "order3_updated",
                        "Order 3 Updated",
                        "2",
                        "payment3_updated",
                        "Payment 3 Updated",
                        "2",
                        "shipment_another_low_detail",
                        100L));
        // Now User "2" (details 300L) is joined with:
        // - shipment_high_detail (details 250L)
        // - shipment_another_low_detail (details 100L)
        // shipment_low_now_high_detail (details 350L) is in state but not joined.

        /* -------- 8. ADDITIONAL CHANGELOG TRANSITIONS ----------- */
        // Test update of shipment details that affects join condition
        // Current state: U("2","user2_updated",300L), O("2","order3_updated"),
        // P("2","payment3_updated")
        // Joined with: shipment_high_detail(250L), shipment_another_low_detail(100L)
        // Not joined: shipment_low_now_high_detail(350L)

        // Update shipment_high_detail details from 250L to 350L (should stop joining)
        deleteShipment("2", "shipment_high_detail", 250L);
        emits(
                DELETE,
                r(
                        "2",
                        "user2_updated",
                        300L,
                        "2",
                        "order3_updated",
                        "Order 3 Updated",
                        "2",
                        "payment3_updated",
                        "Payment 3 Updated",
                        "2",
                        "shipment_high_detail",
                        250L));
        insertShipment("2", "shipment_high_detail_updated", 350L);
        emitsNothing(); // No change as 300L > 350L is false

        // Update shipment_another_low_detail details from 100L to 50L (should remain joined)
        deleteShipment("2", "shipment_another_low_detail", 100L);
        emits(
                DELETE,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3_updated",
                                "Payment 3 Updated",
                                "2",
                                "shipment_another_low_detail",
                                100L),
                INSERT,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3_updated",
                                "Payment 3 Updated",
                                null,
                                null,
                                null));
        insertShipment("2", "shipment_another_low_detail_updated", 50L);
        emits(
                DELETE,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3_updated",
                                "Payment 3 Updated",
                                null,
                                null,
                                null),
                INSERT,
                        r(
                                "2",
                                "user2_updated",
                                300L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3_updated",
                                "Payment 3 Updated",
                                "2",
                                "shipment_another_low_detail_updated",
                                50L));

        // Test update of user details that affects multiple shipment joins
        // Current state: U("2","user2_updated",300L), O("2","order3_updated"),
        // P("2","payment3_updated")
        // Joined with: shipment_another_low_detail_updated(50L)
        // Not joined: shipment_high_detail_updated(350L), shipment_low_now_high_detail(350L)

        // Update user details from 300L to 400L (should join with all shipments)
        updateBeforeUser("2", "user2_updated", 300L);
        emits(
                UPDATE_BEFORE,
                r(
                        "2",
                        "user2_updated",
                        300L,
                        "2",
                        "order3_updated",
                        "Order 3 Updated",
                        "2",
                        "payment3_updated",
                        "Payment 3 Updated",
                        "2",
                        "shipment_another_low_detail_updated",
                        50L));
        updateAfterUser("2", "user2_updated_again", 400L);
        emits(
                UPDATE_AFTER,
                        r(
                                "2",
                                "user2_updated_again",
                                400L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3_updated",
                                "Payment 3 Updated",
                                "2",
                                "shipment_another_low_detail_updated",
                                50L),
                UPDATE_AFTER,
                        r(
                                "2",
                                "user2_updated_again",
                                400L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3_updated",
                                "Payment 3 Updated",
                                "2",
                                "shipment_high_detail_updated",
                                350L),
                UPDATE_AFTER,
                        r(
                                "2",
                                "user2_updated_again",
                                400L,
                                "2",
                                "order3_updated",
                                "Order 3 Updated",
                                "2",
                                "payment3_updated",
                                "Payment 3 Updated",
                                "2",
                                "shipment_low_now_high_detail",
                                350L));

        /* -------- 9. JOIN KEY UPDATES ----------- */
        // Setup: U(jk1,user1,100), O(jk1,order1), P(jk1,payment1) -> Initial join
        insertUser("jk1", "user1", 100L);
        emitsNothing(); // P missing
        insertOrder("jk1", "order1", "Order 1");
        emitsNothing(); // P missing
        insertPayment("jk1", "payment1", "Payment 1");
        emits(
                INSERT,
                r(
                        "jk1",
                        "user1",
                        100L,
                        "jk1",
                        "order1",
                        "Order 1",
                        "jk1",
                        "payment1",
                        "Payment 1",
                        null,
                        null,
                        null));

        // Update User's join key from jk1 to jk2. PK user1 remains.
        deleteUser("jk1", "user1", 100L);
        emits(
                DELETE,
                r(
                        "jk1",
                        "user1",
                        100L,
                        "jk1",
                        "order1",
                        "Order 1",
                        "jk1",
                        "payment1",
                        "Payment 1",
                        null,
                        null,
                        null));
        insertUser("jk2", "user1", 100L); // JK changes, PK is still "user1"
        // The previous join for jk1 is retracted. No new join for jk2 yet as O, P for jk2 don't
        // exist.
        emitsNothing();

        // Insert O and P for jk2 to form a new join with the updated user
        insertOrder("jk2", "order2", "Order 2");
        emitsNothing(); // P for jk2 missing
        insertPayment("jk2", "payment2", "Payment 2");
        // Now U(jk2,user1,100) joins with O(jk2,order2) and P(jk2,payment2)
        emits(
                INSERT,
                r(
                        "jk2",
                        "user1",
                        100L,
                        "jk2",
                        "order2",
                        "Order 2",
                        "jk2",
                        "payment2",
                        "Payment 2",
                        null,
                        null,
                        null));

        // Update Order's join key from jk2 to jk3. PK order2 remains.
        updateBeforeOrder("jk2", "order2", "Order 2");
        emits(
                UPDATE_BEFORE,
                        r(
                                "jk2",
                                "user1",
                                100L,
                                "jk2",
                                "order2",
                                "Order 2",
                                "jk2",
                                "payment2",
                                "Payment 2",
                                null,
                                null,
                                null),
                INSERT,
                        r(
                                "jk2",
                                "user1",
                                100L,
                                null,
                                null,
                                null,
                                "jk2",
                                "payment2",
                                "Payment 2",
                                null,
                                null,
                                null) // U, P remain joined, O becomes null
                );
        updateAfterOrder("jk3", "order2", "Order 2 Updated"); // JK changes to jk3
        // The join with U(jk2) now has null for Order. No new join for O(jk3) with U(jk2).
        emitsNothing();

        // Update Payment's join key from jk2 to jk4. PK payment2 remains.
        updateBeforePayment("jk2", "payment2", "Payment 2");
        emits(
                UPDATE_BEFORE,
                r(
                        "jk2",
                        "user1",
                        100L,
                        null,
                        null,
                        null,
                        "jk2",
                        "payment2",
                        "Payment 2",
                        null,
                        null,
                        null));
        updateAfterPayment("jk4", "payment2", "Payment 2 Updated"); // JK changes to jk4
        // The previous join for U(jk2) is fully retracted because P(jk2) is gone (moved to jk4) and
        // P is INNER.
        // U(jk2) and O(jk3) remain in state but do not form an output.
        emitsNothing();

        /* -------- 10. MULTIPLE SHIPMENT RECORDS AND DELETIONS ----------- */
        // Setup: U(user3,100L), O(order3), P(payment3)
        insertUser("3", "user3", 100L);
        emitsNothing();
        insertOrder("3", "order3", "Order 3");
        emitsNothing();
        insertPayment("3", "payment3", "Payment 3");
        emits(
                INSERT,
                r(
                        "3",
                        "user3",
                        100L,
                        "3",
                        "order3",
                        "Order 3",
                        "3",
                        "payment3",
                        "Payment 3",
                        null,
                        null,
                        null));

        // Insert two shipments that both match U.details (100L)
        insertShipment("3", "shipment1", 50L); // 100L > 50L is TRUE
        emits(
                DELETE,
                        r(
                                "3",
                                "user3",
                                100L,
                                "3",
                                "order3",
                                "Order 3",
                                "3",
                                "payment3",
                                "Payment 3",
                                null,
                                null,
                                null),
                INSERT,
                        r(
                                "3",
                                "user3",
                                100L,
                                "3",
                                "order3",
                                "Order 3",
                                "3",
                                "payment3",
                                "Payment 3",
                                "3",
                                "shipment1",
                                50L));
        insertShipment("3", "shipment2", 60L); // 100L > 60L is TRUE
        emits(
                INSERT,
                r(
                        "3",
                        "user3",
                        100L,
                        "3",
                        "order3",
                        "Order 3",
                        "3",
                        "payment3",
                        "Payment 3",
                        "3",
                        "shipment2",
                        60L));

        // Delete the first shipment (shipment1)
        deleteShipment("3", "shipment1", 50L);
        emits(
                DELETE,
                r(
                        "3",
                        "user3",
                        100L,
                        "3",
                        "order3",
                        "Order 3",
                        "3",
                        "payment3",
                        "Payment 3",
                        "3",
                        "shipment1",
                        50L));
        // The join with shipment2 should remain. No null padding for Shipments yet.

        // Delete the second (and now last) matching shipment (shipment2)
        deleteShipment("3", "shipment2", 60L);
        emits(
                DELETE,
                        r(
                                "3",
                                "user3",
                                100L,
                                "3",
                                "order3",
                                "Order 3",
                                "3",
                                "payment3",
                                "Payment 3",
                                "3",
                                "shipment2",
                                60L),
                INSERT,
                        r(
                                "3",
                                "user3",
                                100L,
                                "3",
                                "order3",
                                "Order 3",
                                "3",
                                "payment3",
                                "Payment 3",
                                null,
                                null,
                                null) // Now transitions to null Shipment
                );

        // Insert a shipment that does NOT match non-equi
        insertShipment("3", "shipment3", 150L); // 100L > 150L is FALSE
        emitsNothing(); // Output remains with null_Shipment

        // Update User details so shipment3 now matches
        updateBeforeUser("3", "user3", 100L);
        emits(
                UPDATE_BEFORE,
                r(
                        "3",
                        "user3",
                        100L,
                        "3",
                        "order3",
                        "Order 3",
                        "3",
                        "payment3",
                        "Payment 3",
                        null,
                        null,
                        null));
        updateAfterUser("3", "user3", 200L); // Details 200L. Now 200L > 150L is TRUE.
        emits(
                UPDATE_AFTER,
                r(
                        "3",
                        "user3",
                        200L,
                        "3",
                        "order3",
                        "Order 3",
                        "3",
                        "payment3",
                        "Payment 3",
                        "3",
                        "shipment3",
                        150L) // Join with shipment3
                );
    }

    // We need to a custom input type because we use big int for details in Users and Shipments
    // and our base tests only use strings.
    @Override
    protected RowType createInputTypeInfo(int inputIndex) {
        String userIdFieldName = String.format("user_id_%d", inputIndex);
        String pkFieldName = String.format("pk_%d", inputIndex); // Generic PK name

        if (inputIndex == 0) { // Users: user_id (CHAR NOT NULL), pk (VARCHAR), details (BIGINT)
            return RowType.of(
                    new LogicalType[] {
                        new CharType(false, 20), VarCharType.STRING_TYPE, new BigIntType()
                    },
                    new String[] {
                        userIdFieldName, pkFieldName, String.format("details_%d", inputIndex)
                    });
        } else if (inputIndex
                == 3) { // Shipments: user_id (CHAR NOT NULL), pk (VARCHAR), details (BIGINT)
            return RowType.of(
                    new LogicalType[] {
                        new CharType(false, 20), VarCharType.STRING_TYPE, new BigIntType()
                    },
                    new String[] {
                        userIdFieldName, pkFieldName, String.format("details_%d", inputIndex)
                    });
        } else { // Orders (1), Payments (2): user_id (CHAR NOT NULL), pk (CHAR NOT NULL), details
            // (VARCHAR)
            return super.createInputTypeInfo(inputIndex);
        }
    }

    @Override
    protected InternalTypeInfo<RowData> createUniqueKeyType(int inputIndex) {
        if (inputIndex == 3) { // Shipments has no unique key
            return null;
        }
        return super.createUniqueKeyType(inputIndex);
    }
}
