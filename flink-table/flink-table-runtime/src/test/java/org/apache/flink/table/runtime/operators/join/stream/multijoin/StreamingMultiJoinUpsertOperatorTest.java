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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RuntimeChangelogMode;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the multi-way join handles inputs that update a record without UPDATE_BEFORE and
 * delete it with only its unique key. Only the joined rows and null padding that the old version of
 * a record produces are retracted.
 *
 * <p>SQL: SELECT * FROM Users u JOIN Orders o ON u.user_id = o.user_id AND u.details = o.details
 * LEFT JOIN Payments p ON o.user_id = p.user_id AND o.details <> p.details LEFT JOIN Shipments s ON
 * p.user_id = s.user_id AND p.details = s.details
 *
 * <p>Schema: Users(user_id PRIMARY KEY, name, details), Orders, Payments and Shipments(user_id, id,
 * details, PRIMARY KEY (user_id, id)). All inputs are upsert inputs.
 */
@ExtendWith(ParameterizedTestExtension.class)
class StreamingMultiJoinUpsertOperatorTest extends StreamingMultiJoinOperatorTestBase {

    private static final Object[] NO_ROW = {null, null, null};
    private static final Object[] USER = {"1", "Gus", "EU"};
    private static final Object[] ORDER = {"1", "o1", "EU"};

    public StreamingMultiJoinUpsertOperatorTest(StateBackendMode stateBackendMode) {
        super(
                stateBackendMode,
                4,
                List.of(
                        FlinkJoinType.INNER,
                        FlinkJoinType.INNER,
                        FlinkJoinType.LEFT,
                        FlinkJoinType.LEFT),
                Arrays.asList(
                        null,
                        createAndCondition(equalTo(0, 0), equalTo(2, 2)),
                        createAndCondition(equalTo(3, 0), notEqualTo(5, 2)),
                        createAndCondition(equalTo(6, 0), equalTo(8, 2))),
                Map.of(
                        1,
                        List.of(
                                new ConditionAttributeRef(0, 0, 1, 0),
                                new ConditionAttributeRef(0, 2, 1, 2)),
                        2,
                        List.of(new ConditionAttributeRef(1, 0, 2, 0)),
                        3,
                        List.of(
                                new ConditionAttributeRef(2, 0, 3, 0),
                                new ConditionAttributeRef(2, 2, 3, 2))),
                false);
    }

    @TestTemplate
    void testUpdateThatMovesJoinKeyRetractsOldVersion() throws Exception {
        insertUserAndOrder();

        updateAfterOrder("1", "o1", "US");
        emits(DELETE, row(USER, ORDER, NO_ROW, NO_ROW));

        // the old version of the order is no longer stored and doesn't join again
        updateAfterUser("1", "Gus", "US");
        emits(UPDATE_AFTER, row(r("1", "Gus", "US"), r("1", "o1", "US"), NO_ROW, NO_ROW));
        updateAfterUser("1", "Gus", "EU");
        emits(DELETE, row(r("1", "Gus", "US"), r("1", "o1", "US"), NO_ROW, NO_ROW));
    }

    @TestTemplate
    void testInsertThatReplacesStoredRecordRetractsOldVersion() throws Exception {
        insertUserAndOrder();

        insertOrder("1", "o1", "US");
        emits(DELETE, row(USER, ORDER, NO_ROW, NO_ROW));
    }

    @TestTemplate
    void testUpdateOfFirstInputThatLosesMatchRetractsJoinedRow() throws Exception {
        insertUserAndOrder();

        updateAfterUser("1", "Gus", "US");
        emits(DELETE, row(USER, ORDER, NO_ROW, NO_ROW));
    }

    @TestTemplate
    void testUpdateWithSameConditionFieldsUpdatesJoinedRow() throws Exception {
        insertUserAndOrder();

        updateAfterUser("1", "Bob", "EU");
        emits(UPDATE_AFTER, row(r("1", "Bob", "EU"), ORDER, NO_ROW, NO_ROW));
    }

    @TestTemplate
    void testKeyOnlyDeleteRetractsStoredRecord() throws Exception {
        insertUserAndOrder();

        deleteOrder("1", "o1", null);
        emits(DELETE, row(USER, ORDER, NO_ROW, NO_ROW));
        assertThat(testHarness.numKeyedStateEntries()).isOne();

        updateAfterUser("1", "Bob", "EU");
        emitsNothing();
    }

    @TestTemplate
    void testUpdateThatLosesNonEquiMatchInsertsNullPadding() throws Exception {
        insertUserAndOrderAndPayment();

        updateAfterPayment("1", "p1", "EU");
        emits(
                DELETE,
                row(USER, ORDER, r("1", "p1", "X"), NO_ROW),
                INSERT,
                row(USER, ORDER, NO_ROW, NO_ROW));
    }

    @TestTemplate
    void testKeyOnlyDeleteWithNonEquiConditionInsertsNullPadding() throws Exception {
        insertUserAndOrderAndPayment();

        deletePayment("1", "p1", null);
        emits(
                DELETE,
                row(USER, ORDER, r("1", "p1", "X"), NO_ROW),
                INSERT,
                row(USER, ORDER, NO_ROW, NO_ROW));
    }

    @TestTemplate
    void testUpdateThatKeepsNonEquiMatchUpdatesJoinedRow() throws Exception {
        insertUserAndOrderAndPayment();

        updateAfterPayment("1", "p1", "Y");
        emits(UPDATE_AFTER, row(USER, ORDER, r("1", "p1", "Y"), NO_ROW));
    }

    @TestTemplate
    void testUpdateThatGainsMatchRetractsNullPaddingOfOldVersion() throws Exception {
        insertUserAndOrderAndPayment();
        insertShipment("1", "s1", "Y");
        emitsNothing();

        updateAfterPayment("1", "p1", "Y");
        emits(
                DELETE,
                row(USER, ORDER, r("1", "p1", "X"), NO_ROW),
                UPDATE_AFTER,
                row(USER, ORDER, r("1", "p1", "Y"), r("1", "s1", "Y")));
    }

    private void insertUserAndOrder() throws Exception {
        insertUser(USER);
        insertOrder(ORDER);
        emits(INSERT, row(USER, ORDER, NO_ROW, NO_ROW));
    }

    private void insertUserAndOrderAndPayment() throws Exception {
        insertUserAndOrder();
        insertPayment("1", "p1", "X");
        emits(
                DELETE,
                row(USER, ORDER, NO_ROW, NO_ROW),
                INSERT,
                row(USER, ORDER, r("1", "p1", "X"), NO_ROW));
    }

    private static Object[] row(Object[]... inputRows) {
        return Stream.of(inputRows).flatMap(Arrays::stream).toArray();
    }

    @Override
    protected List<RuntimeChangelogMode> inputChangelogModes() {
        return Collections.nCopies(
                inputSpecs.size(), RuntimeChangelogMode.serialize(ChangelogMode.upsert(true)));
    }

    /** Payments join on user_id only, which their unique key contains. */
    @Override
    protected boolean uniqueKeyContainsJoinKey(int inputIndex) {
        return inputIndex == 2;
    }

    /** Outside the unique keys, the conditions read details of all inputs. */
    @Override
    protected int[][] conditionFields() {
        return new int[][] {{2}, {2}, {2}, {2}};
    }

    /** All inputs but the first are keyed by (user_id, id), which contains the common join key. */
    @Override
    protected InternalTypeInfo<RowData> createUniqueKeyType(int inputIndex) {
        if (inputIndex == 0) {
            return super.createUniqueKeyType(inputIndex);
        }
        return InternalTypeInfo.of(
                RowType.of(
                        new LogicalType[] {new CharType(false, 20), new CharType(false, 20)},
                        new String[] {
                            String.format("user_id_%d", inputIndex),
                            String.format("id_%d", inputIndex)
                        }));
    }

    @Override
    protected RowDataKeySelector createKeySelector(int inputIndex) {
        if (inputIndex == 0) {
            return super.createKeySelector(inputIndex);
        }
        return HandwrittenSelectorUtil.getRowDataSelector(
                new int[] {0, 1},
                inputTypeInfos.get(inputIndex).getChildren().toArray(new LogicalType[0]));
    }

    /** Field leftField of the left side equals field rightField of the right side. */
    private static GeneratedJoinCondition equalTo(int leftField, int rightField) {
        return new GeneratedJoinCondition("EqualTo_manual", "", new Object[0]) {
            @Override
            public JoinCondition newInstance(ClassLoader classLoader) {
                return new SpecificInputsEquiKeyCondition(leftField, rightField);
            }
        };
    }

    /** Field leftField of the left side differs from field rightField of the right side. */
    private static GeneratedJoinCondition notEqualTo(int leftField, int rightField) {
        return new GeneratedJoinCondition("NotEqualTo_manual", "", new Object[0]) {
            @Override
            public JoinCondition newInstance(ClassLoader classLoader) {
                return new NotEqualToCondition(leftField, rightField);
            }
        };
    }

    private static class NotEqualToCondition extends AbstractRichFunction implements JoinCondition {
        private final int leftField;
        private final int rightField;

        private NotEqualToCondition(int leftField, int rightField) {
            this.leftField = leftField;
            this.rightField = rightField;
        }

        @Override
        public boolean apply(RowData left, RowData right) {
            return !left.isNullAt(leftField)
                    && !right.isNullAt(rightField)
                    && !left.getString(leftField).equals(right.getString(rightField));
        }
    }
}
