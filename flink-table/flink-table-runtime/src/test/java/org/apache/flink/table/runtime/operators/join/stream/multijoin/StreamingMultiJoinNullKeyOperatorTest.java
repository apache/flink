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

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the multi-way join doesn't store records with a NULL in a field in which a NULL means
 * that they can never match, and that its output doesn't change.
 *
 * <p>SQL: SELECT u.*, o.*, p.* FROM Users u JOIN Orders o ON u.user_id = o.user_id AND u.details =
 * o.details LEFT JOIN Payments p ON o.user_id = p.user_id
 *
 * <p>Schema: Users(user_id PRIMARY KEY, name, details), Orders(user_id, order_id PRIMARY KEY,
 * details), Payments(user_id, payment_id PRIMARY KEY, details)
 */
@ExtendWith(ParameterizedTestExtension.class)
class StreamingMultiJoinNullKeyOperatorTest extends StreamingMultiJoinOperatorTestBase {

    // The inner join filters user_id and details of Users and Orders, the left join filters
    // user_id of Payments.
    private static final int[][] NULL_FILTER_FIELDS = {{0, 2}, {0, 2}, {0}};

    private boolean filterNulls = true;

    public StreamingMultiJoinNullKeyOperatorTest(StateBackendMode stateBackendMode) {
        super(
                stateBackendMode,
                3,
                List.of(FlinkJoinType.INNER, FlinkJoinType.INNER, FlinkJoinType.LEFT),
                Arrays.asList(
                        null,
                        createAndCondition(createJoinCondition(1, 0), createDetailsCondition()),
                        createJoinCondition(2, 1)),
                Map.of(
                        1,
                        List.of(
                                new ConditionAttributeRef(0, 0, 1, 0),
                                new ConditionAttributeRef(0, 2, 1, 2)),
                        2,
                        List.of(new ConditionAttributeRef(1, 0, 2, 0))),
                false);
    }

    @TestTemplate
    void testDoesNotStoreRecordsThatCanNeverMatch() throws Exception {
        insertUser(null, "Gus", "EU");
        insertUser("1", "Bob", null);
        insertOrder(null, "order_1", "EU");
        insertOrder("1", "order_2", null);
        insertPayment(null, "payment_1", "EU");
        emitsNothing();
        assertThat(testHarness.numKeyedStateEntries()).isZero();

        deleteUser(null, "Gus", "EU");
        deleteUser("1", "Bob", null);
        deleteOrder(null, "order_1", "EU");
        deleteOrder("1", "order_2", null);
        deletePayment(null, "payment_1", "EU");
        emitsNothing();
        assertThat(testHarness.numKeyedStateEntries()).isZero();
    }

    @TestTemplate
    void testStoresRecordsThatCanMatch() throws Exception {
        insertUser("1", "Gus", "EU");
        insertOrder(null, "order_1", "EU");
        insertOrder("1", "order_2", null);
        emitsNothing();
        insertOrder("1", "order_3", "EU");
        emits(INSERT, "1", "Gus", "EU", "1", "order_3", "EU", null, null, null);
        insertPayment(null, "payment_1", "EU");
        emitsNothing();
        insertPayment("1", "payment_2", null);
        emits(
                DELETE,
                r("1", "Gus", "EU", "1", "order_3", "EU", null, null, null),
                INSERT,
                r("1", "Gus", "EU", "1", "order_3", "EU", "1", "payment_2", null));
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(3);
    }

    @TestTemplate
    void testUpdateAfterReplacesStoredRecord() throws Exception {
        // Without UPDATE_BEFORE, as for an input whose unique key contains the common join key
        insertUser("1", "Gus", "EU");
        assertThat(testHarness.numKeyedStateEntries()).isOne();
        updateAfterUser("1", "Gus", null);
        emitsNothing();
        assertThat(testHarness.numKeyedStateEntries()).isZero();

        insertOrder("1", "order_1", "EU");
        emitsNothing();
    }

    @TestTemplate
    void testInsertReplacesStoredRecord() throws Exception {
        insertUser("1", "Gus", "EU");
        assertThat(testHarness.numKeyedStateEntries()).isOne();
        insertUser("1", "Gus", null);
        emitsNothing();
        assertThat(testHarness.numKeyedStateEntries()).isZero();

        insertOrder("1", "order_1", "EU");
        emitsNothing();
    }

    @TestTemplate
    void testRetractsRecordStoredByEarlierVersion() throws Exception {
        recreateHarness(null, false);
        insertOrder(null, "order_1", "EU");
        emitsNothing();
        assertThat(testHarness.numKeyedStateEntries()).isOne();
        final OperatorSubtaskState snapshot = testHarness.snapshot(1L, 1L);

        recreateHarness(snapshot, true);
        deleteOrder(null, "order_1", "EU");
        emitsNothing();
        assertThat(testHarness.numKeyedStateEntries()).isZero();
    }

    @Override
    protected int[][] nullFilterFields() {
        return filterNulls ? NULL_FILTER_FIELDS : super.nullFilterFields();
    }

    /** Makes user_id nullable, so that records can have a NULL join key. */
    @Override
    protected RowType createInputTypeInfo(int inputIndex) {
        return RowType.of(
                new LogicalType[] {
                    new CharType(true, 20), new CharType(false, 20), VarCharType.STRING_TYPE
                },
                new String[] {
                    String.format("user_id_%d", inputIndex),
                    String.format("id_%d", inputIndex),
                    String.format("details_%d", inputIndex)
                });
    }

    @Override
    protected InternalTypeInfo<RowData> createUniqueKeyType(int inputIndex) {
        return InternalTypeInfo.of(
                RowType.of(
                        new LogicalType[] {new CharType(inputIndex == 0, 20)},
                        new String[] {
                            String.format(inputIndex == 0 ? "user_id_%d" : "id_%d", inputIndex)
                        }));
    }

    /** u.details = o.details, where details is field 2 of both inputs. */
    private static GeneratedJoinCondition createDetailsCondition() {
        return new GeneratedJoinCondition("DetailsCondition_manual", "", new Object[0]) {
            @Override
            public JoinCondition newInstance(ClassLoader classLoader) {
                return new SpecificInputsEquiKeyCondition(2, 2);
            }
        };
    }

    private void recreateHarness(OperatorSubtaskState state, boolean filterNulls) throws Exception {
        final MockEnvironment environment = testHarness.getEnvironment();
        testHarness.close();
        environment.close();
        this.filterNulls = filterNulls;
        testHarness = createTestHarness();
        testHarness.setup();
        if (state != null) {
            testHarness.initializeState(state);
        }
        testHarness.open();
    }
}
