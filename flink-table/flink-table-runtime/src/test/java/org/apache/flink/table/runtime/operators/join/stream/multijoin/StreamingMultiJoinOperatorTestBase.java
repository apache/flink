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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperatorFactory;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.runtime.util.StateParameterizedHarnessTestBase;
import org.apache.flink.table.runtime.util.StreamRecordUtils;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class for testing the StreamingMultiJoinOperator. Provides common functionality and helper
 * methods for testing multi-way joins.
 */
public abstract class StreamingMultiJoinOperatorTestBase extends StateParameterizedHarnessTestBase {

    // ==========================================================================
    // Constants
    // ==========================================================================

    protected static final RowKind INSERT = RowKind.INSERT;
    protected static final RowKind UPDATE_BEFORE = RowKind.UPDATE_BEFORE;
    protected static final RowKind UPDATE_AFTER = RowKind.UPDATE_AFTER;
    protected static final RowKind DELETE = RowKind.DELETE;

    // ==========================================================================
    // Test Configuration
    // ==========================================================================

    protected final List<RowType> inputTypeInfos;
    protected final List<RowDataKeySelector> keySelectors;
    protected final List<JoinInputSideSpec> inputSpecs;
    protected final List<FlinkJoinType> joinTypes;
    protected final List<GeneratedJoinCondition> joinConditions;
    protected final boolean isFullOuterJoin;
    protected final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap;
    protected final JoinKeyExtractor keyExtractor;

    // ==========================================================================
    // Test State
    // ==========================================================================

    protected RowDataHarnessAssertor asserter;
    protected KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> testHarness;

    // ==========================================================================
    // Constructor
    // ==========================================================================

    protected StreamingMultiJoinOperatorTestBase(
            StateBackendMode stateBackendMode,
            int numInputs,
            List<FlinkJoinType> joinTypes,
            List<GeneratedJoinCondition> joinConditions,
            boolean isFullOuterJoin) {
        super(stateBackendMode);
        this.inputTypeInfos = new ArrayList<>(numInputs);
        this.keySelectors = new ArrayList<>(numInputs);
        this.inputSpecs = new ArrayList<>(numInputs);
        this.joinTypes = joinTypes;
        this.isFullOuterJoin = isFullOuterJoin;
        this.joinConditions = joinConditions;
        this.joinAttributeMap = new HashMap<>();

        initializeInputs(numInputs);
        initializeJoinConditions();

        this.keyExtractor =
                new AttributeBasedJoinKeyExtractor(this.joinAttributeMap, this.inputTypeInfos);
    }

    /** Constructor allowing explicit provision of joinAttributeMap for custom conditions. */
    protected StreamingMultiJoinOperatorTestBase(
            StateBackendMode stateBackendMode,
            int numInputs,
            List<FlinkJoinType> joinTypes,
            List<GeneratedJoinCondition> joinConditions,
            Map<Integer, List<ConditionAttributeRef>> joinAttributeMap,
            boolean isFullOuterJoin) {
        super(stateBackendMode);
        this.inputTypeInfos = new ArrayList<>(numInputs);
        this.keySelectors = new ArrayList<>(numInputs);
        this.inputSpecs = new ArrayList<>(numInputs);
        this.joinTypes = joinTypes;
        this.isFullOuterJoin = isFullOuterJoin;
        this.joinConditions = joinConditions;
        this.joinAttributeMap = joinAttributeMap;

        initializeInputs(numInputs);

        this.keyExtractor =
                new AttributeBasedJoinKeyExtractor(this.joinAttributeMap, this.inputTypeInfos);
    }

    // ==========================================================================
    // Test Lifecycle
    // ==========================================================================

    @BeforeEach
    protected void beforeEach() throws Exception {
        testHarness = createTestHarness();
        setupKeySelectorsForTestHarness(testHarness);
        testHarness.setup();
        testHarness.open();
        asserter =
                new RowDataHarnessAssertor(
                        getOutputType().getChildren().toArray(new LogicalType[0]));
    }

    @AfterEach
    protected void afterEach() throws Exception {
        if (testHarness != null) {
            testHarness.close();
        }
    }

    // ==========================================================================
    // Helper Methods for Test Data
    // ==========================================================================

    protected void insertUser(Object... fields) throws Exception {
        testHarness.processElement(
                0, new StreamRecord<>(StreamRecordUtils.rowOfKind(INSERT, fields)));
    }

    protected void insertOrder(Object... fields) throws Exception {
        testHarness.processElement(
                1, new StreamRecord<>(StreamRecordUtils.rowOfKind(INSERT, fields)));
    }

    protected void insertPayment(Object... fields) throws Exception {
        testHarness.processElement(
                2, new StreamRecord<>(StreamRecordUtils.rowOfKind(INSERT, fields)));
    }

    protected void insertShipment(Object... fields) throws Exception {
        testHarness.processElement(
                3, new StreamRecord<>(StreamRecordUtils.rowOfKind(INSERT, fields)));
    }

    protected void updateBeforeUser(Object... fields) throws Exception {
        testHarness.processElement(
                0, new StreamRecord<>(StreamRecordUtils.rowOfKind(UPDATE_BEFORE, fields)));
    }

    protected void updateAfterUser(Object... fields) throws Exception {
        testHarness.processElement(
                0, new StreamRecord<>(StreamRecordUtils.rowOfKind(UPDATE_AFTER, fields)));
    }

    protected void updateBeforeOrder(Object... fields) throws Exception {
        testHarness.processElement(
                1, new StreamRecord<>(StreamRecordUtils.rowOfKind(UPDATE_BEFORE, fields)));
    }

    protected void updateAfterOrder(Object... fields) throws Exception {
        testHarness.processElement(
                1, new StreamRecord<>(StreamRecordUtils.rowOfKind(UPDATE_AFTER, fields)));
    }

    protected void updateBeforePayment(Object... fields) throws Exception {
        testHarness.processElement(
                2, new StreamRecord<>(StreamRecordUtils.rowOfKind(UPDATE_BEFORE, fields)));
    }

    protected void updateAfterPayment(Object... fields) throws Exception {
        testHarness.processElement(
                2, new StreamRecord<>(StreamRecordUtils.rowOfKind(UPDATE_AFTER, fields)));
    }

    protected void deleteUser(Object... fields) throws Exception {
        testHarness.processElement(
                0, new StreamRecord<>(StreamRecordUtils.rowOfKind(DELETE, fields)));
    }

    protected void deleteOrder(Object... fields) throws Exception {
        testHarness.processElement(
                1, new StreamRecord<>(StreamRecordUtils.rowOfKind(DELETE, fields)));
    }

    protected void deletePayment(Object... fields) throws Exception {
        testHarness.processElement(
                2, new StreamRecord<>(StreamRecordUtils.rowOfKind(DELETE, fields)));
    }

    protected void deleteShipment(Object... fields) throws Exception {
        testHarness.processElement(
                3, new StreamRecord<>(StreamRecordUtils.rowOfKind(DELETE, fields)));
    }

    protected static List<GeneratedJoinCondition> defaultConditions() {
        return new ArrayList<>();
    }

    // ==========================================================================
    // Assertion Methods
    // ==========================================================================

    protected void emits(RowKind kind, Object... fields) throws Exception {
        asserter.shouldEmitAll(testHarness, rowOfKind(kind, fields));
    }

    protected void emitsNothing() {
        asserter.shouldEmitNothing(testHarness);
    }

    protected void emits(RowKind kind1, Object[] fields1, RowKind kind2, Object[] fields2)
            throws Exception {
        asserter.shouldEmitAll(testHarness, rowOfKind(kind1, fields1), rowOfKind(kind2, fields2));
    }

    protected void emits(
            RowKind kind1,
            Object[] fields1,
            RowKind kind2,
            Object[] fields2,
            RowKind kind3,
            Object[] fields3)
            throws Exception {
        asserter.shouldEmitAll(
                testHarness,
                rowOfKind(kind1, fields1),
                rowOfKind(kind2, fields2),
                rowOfKind(kind3, fields3));
    }

    protected void emits(
            RowKind kind1,
            Object[] fields1,
            RowKind kind2,
            Object[] fields2,
            RowKind kind3,
            Object[] fields3,
            RowKind kind4,
            Object[] fields4)
            throws Exception {
        asserter.shouldEmitAll(
                testHarness,
                rowOfKind(kind1, fields1),
                rowOfKind(kind2, fields2),
                rowOfKind(kind3, fields3),
                rowOfKind(kind4, fields4));
    }

    // ==========================================================================
    // Private Helper Methods
    // ==========================================================================

    /*
    For the tests, we have a default setup:
    1. Our first input will also be the one with the unique key as a join key.
    2. Further tests will have unique keys but not contained in the join key.
    * */
    private void initializeInputs(int numInputs) {
        if (numInputs < 2) {
            throw new IllegalArgumentException("Number of inputs must be a" + "t least 2");
        }

        // In our tests, the first input is always the one with the unique key as a join key
        inputTypeInfos.add(createInputTypeInfo(0));
        keySelectors.add(createKeySelector(0));
        inputSpecs.add(
                JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                        createUniqueKeyType(0), keySelectors.get(0)));

        // Following tables contain a unique key but are not contained in the join key
        for (int i = 1; i < numInputs; i++) {
            inputTypeInfos.add(createInputTypeInfo(i));
            keySelectors.add(createKeySelector(i));

            var uniqueKeyRowType = createUniqueKeyType(i);
            if (uniqueKeyRowType != null) {
                inputSpecs.add(
                        JoinInputSideSpec.withUniqueKey(uniqueKeyRowType, keySelectors.get(i)));
            } else {
                inputSpecs.add(JoinInputSideSpec.withoutUniqueKey());
            }
        }
    }

    private void initializeJoinConditions() {
        // If the map is already populated, it means conditions and map were provided explicitly.
        // Validation happened in the specific constructor.
        if (!joinAttributeMap.isEmpty()) {
            return;
        }

        // Proceed with default generation only if conditions AND map were not provided.
        if (joinConditions.isEmpty()) {
            // First input doesn't have a left input to join with
            joinConditions.add(null);
            for (int i = 1; i < inputSpecs.size(); i++) {
                // Add the join condition comparing current input (i) with previous (i-1)
                GeneratedJoinCondition condition = createJoinCondition(i, i - 1);
                joinConditions.add(condition);

                joinAttributeMap.put(
                        i, Collections.singletonList(new ConditionAttributeRef(i - 1, 0, i, 0)));
            }
        } else if (joinConditions.size() != inputSpecs.size()) {
            throw new IllegalArgumentException(
                    "The number of provided join conditions must match the number of inputs ("
                            + inputSpecs.size()
                            + "), but got "
                            + joinConditions.size());
        }
    }

    /** A serializable KeySelector that uses a JoinKeyExtractor to extract keys from RowData. */
    private static class SerializableKeySelector
            implements KeySelector<RowData, RowData>, Serializable {
        private static final long serialVersionUID = 1L;
        private final JoinKeyExtractor keyExtractor;
        private final int inputIndex;

        public SerializableKeySelector(JoinKeyExtractor keyExtractor, int inputIndex) {
            this.keyExtractor = keyExtractor;
            this.inputIndex = inputIndex;
        }

        @Override
        public RowData getKey(RowData value) {
            return keyExtractor.getCommonJoinKey(value, inputIndex);
        }
    }

    private void setupKeySelectorsForTestHarness(
            KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> harness) {
        for (int i = 0; i < this.inputSpecs.size(); i++) {
            SerializableKeySelector keySelector = new SerializableKeySelector(this.keyExtractor, i);
            harness.setKeySelector(i, keySelector);
        }
    }

    protected KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> createTestHarness()
            throws Exception {
        var joinKeyType = this.keyExtractor.getCommonJoinKeyType();
        InternalTypeInfo<RowData> partitionKeyTypeInfo = InternalTypeInfo.of(joinKeyType);

        final GeneratedJoinCondition[] generatedJoinConditions =
                this.joinConditions.toArray(new GeneratedJoinCondition[0]);
        final long[] retentionTime = new long[inputSpecs.size()];
        Arrays.fill(retentionTime, 9999999L);
        final List<InternalTypeInfo<RowData>> internalTypeInfos =
                this.inputTypeInfos.stream().map(InternalTypeInfo::of).collect(Collectors.toList());

        StreamingMultiJoinOperatorFactory factory =
                new StreamingMultiJoinOperatorFactory(
                        internalTypeInfos,
                        this.inputSpecs,
                        this.joinTypes,
                        null,
                        retentionTime,
                        generatedJoinConditions,
                        this.keyExtractor,
                        this.joinAttributeMap);

        KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> harness =
                new KeyedMultiInputStreamOperatorTestHarness<>(factory, partitionKeyTypeInfo);

        setupKeySelectorsForTestHarness(harness);

        harness.setStateBackend(getStateBackend());
        harness.setCheckpointStorage(getCheckpointStorage());
        return harness;
    }

    protected RowType getOutputType() {
        var typesStream =
                inputTypeInfos.stream().flatMap(typeInfo -> typeInfo.getChildren().stream());
        var namesStream =
                inputTypeInfos.stream().flatMap(typeInfo -> typeInfo.getFieldNames().stream());

        return RowType.of(
                typesStream.toArray(LogicalType[]::new), namesStream.toArray(String[]::new));
    }

    protected RowData rowOfKind(RowKind kind, Object... fields) {
        return StreamRecordUtils.rowOfKind(kind, fields);
    }

    protected Object[] r(Object... values) {
        return values;
    }

    // ==========================================================================
    // Type Creation Methods
    // ==========================================================================

    protected RowType createInputTypeInfo(int inputIndex) {
        return RowType.of(
                new LogicalType[] {
                    new CharType(false, 20), new CharType(false, 20), VarCharType.STRING_TYPE
                },
                new String[] {
                    String.format("user_id_%d", inputIndex),
                    String.format("id_%d", inputIndex),
                    String.format("details_%d", inputIndex)
                });
    }

    protected InternalTypeInfo<RowData> createUniqueKeyType(int inputIndex) {
        return InternalTypeInfo.of(
                RowType.of(
                        new LogicalType[] {
                            new CharType(false, 20),
                        },
                        new String[] {
                            String.format(inputIndex == 0 ? "user_id_%d" : "id_%d", inputIndex),
                        }));
    }

    protected RowDataKeySelector createKeySelector(int inputIndex) {
        return HandwrittenSelectorUtil.getRowDataSelector(
                /* Unique key for Input 0 is field 0 (user_id_0).
                 * Unique key for Inputs 1, 2, 3 is field 1 (id_1, id_2, id_3 respectively). */
                new int[] {inputIndex == 0 ? 0 : 1},
                inputTypeInfos.get(inputIndex).getChildren().toArray(new LogicalType[0]));
    }

    protected static GeneratedJoinCondition createJoinCondition(int rightInputId, int leftInputId) {
        if (rightInputId <= 0 || leftInputId < 0 || rightInputId == leftInputId) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid indices for creating join condition. rightInputId: %d, leftInputId: %d",
                            rightInputId, leftInputId));
        }

        String generatedClassName =
                String.format(
                        "SpecificInputsEquiKeyCondition_manual_%d_%d", rightInputId, leftInputId);
        return new GeneratedJoinCondition(generatedClassName, "", new Object[0]) {
            @Override
            public JoinCondition newInstance(ClassLoader classLoader) {
                // Field 0 of the left side and field 0 of the right side is assumed for equality
                // key comparison in this default test condition.
                // We do leftInputId * 3 because each input has 3 fields in our test setup.
                return new SpecificInputsEquiKeyCondition(leftInputId * 3, 0);
            }
        };
    }

    // ==========================================================================
    // Concrete MultiJoinCondition Implementations for TestBase
    // ==========================================================================

    /**
     * Checks if `inputs[leftInputIndex].getString(leftKeyFieldIndex)` is equal to
     * `inputs[rightInputIndex].getString(rightKeyFieldIndex)`. Used for specific join step
     * conditions.
     */
    protected static class SpecificInputsEquiKeyCondition extends AbstractRichFunction
            implements JoinCondition {
        private final int leftKeyFieldIndex;
        private final int rightKeyFieldIndex;

        public SpecificInputsEquiKeyCondition(int leftKeyFieldIndex, int rightKeyFieldIndex) {
            this.leftKeyFieldIndex = leftKeyFieldIndex;
            this.rightKeyFieldIndex = rightKeyFieldIndex;
        }

        @Override
        public boolean apply(RowData left, RowData right) {
            if (left == null || right == null) {
                return false;
            }

            if (left.isNullAt(leftKeyFieldIndex) || right.isNullAt(rightKeyFieldIndex)) {
                return false;
            }
            String keyLeft = left.getString(leftKeyFieldIndex).toString();
            String keyRight = right.getString(rightKeyFieldIndex).toString();
            return keyLeft.equals(keyRight);
        }

        private int calculateActualFieldIndex(int inputIndex, int fieldIndex) {
            int actualIndex = fieldIndex;
            // Add the arity of all previous inputs
            for (int i = 0; i < inputIndex; i++) {
                actualIndex += 3; // all our inputs in our test setup have 3 fields
            }
            return actualIndex;
        }
    }

    /**
     * Condition for comparing a Long field from the left input as greater than a Long field from
     * the right input. Example: leftInput.field > rightInput.field
     */
    protected static class FieldLongGreaterThanConditionImpl extends AbstractRichFunction
            implements JoinCondition {
        private final int leftFieldIdx;
        private final int rightFieldIdx;

        public FieldLongGreaterThanConditionImpl(int leftFieldIdx, int rightFieldIdx) {
            this.leftFieldIdx = leftFieldIdx;
            this.rightFieldIdx = rightFieldIdx;
        }

        @Override
        public boolean apply(RowData left, RowData right) {
            if (left == null || right == null) {
                return false;
            }
            if (left.isNullAt(leftFieldIdx) || right.isNullAt(rightFieldIdx)) {
                return false;
            }
            return left.getLong(leftFieldIdx) > right.getLong(rightFieldIdx);
        }
    }

    /** Combines multiple MultiJoinConditions with AND logic. */
    protected static class AndJoinConditionImpl extends AbstractRichFunction
            implements JoinCondition {
        private final JoinCondition[] conditions;

        public AndJoinConditionImpl(JoinCondition... conditions) {
            this.conditions = conditions;
        }

        @Override
        public boolean apply(RowData left, RowData right) {
            for (JoinCondition condition : conditions) {
                if (condition == null) { // Should not happen if constructed properly via factory
                    return false;
                }
                if (!condition.apply(left, right)) {
                    return false;
                }
            }
            return true;
        }
    }

    // Factory Methods for Conditions
    protected static GeneratedJoinCondition createFieldLongGreaterThanCondition(
            int leftFieldIdx, int rightFieldIdx) {
        String generatedClassName =
                String.format(
                        "FieldLongGreaterThanCondition_manual_%d_%d", leftFieldIdx, rightFieldIdx);
        return new GeneratedJoinCondition(generatedClassName, "", new Object[0]) {
            @Override
            public JoinCondition newInstance(ClassLoader classLoader) {
                return new FieldLongGreaterThanConditionImpl(leftFieldIdx, rightFieldIdx);
            }
        };
    }

    protected static GeneratedJoinCondition createAndCondition(
            GeneratedJoinCondition... generatedConditions) {
        String generatedClassName = "AndJoinCondition_manual";
        return new GeneratedJoinCondition(generatedClassName, "", new Object[0]) {
            @Override
            public JoinCondition newInstance(ClassLoader classLoader) {
                JoinCondition[] actualConditions = new JoinCondition[generatedConditions.length];
                for (int i = 0; i < generatedConditions.length; i++) {
                    if (generatedConditions[i] == null) {
                        // This case should ideally be prevented by how createAndCondition is called
                        // or handled by AndJoinConditionImpl if nulls are permissible for some
                        // reason.
                        // For now, let's assume valid conditions are passed.
                        throw new IllegalArgumentException(
                                "Null GeneratedJoinCondition passed to createAndCondition");
                    }
                    actualConditions[i] = generatedConditions[i].newInstance(classLoader);
                }
                return new AndJoinConditionImpl(actualConditions);
            }
        };
    }
}
