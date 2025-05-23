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
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedMultiJoinCondition;
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator.JoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.AttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for testing the StreamingMultiJoinOperator. Provides common functionality and helper
 * methods for testing multi-way joins.
 */
public abstract class StreamingMultiJoinOperatorTestBase {

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

    protected final List<InternalTypeInfo<RowData>> inputTypeInfos;
    protected final List<RowDataKeySelector> keySelectors;
    protected final List<JoinInputSideSpec> inputSpecs;
    protected final List<JoinType> joinTypes;
    protected final List<GeneratedMultiJoinCondition> joinConditions;
    protected final boolean isFullOuterJoin;
    protected final Map<Integer, Map<AttributeRef, AttributeRef>> joinAttributeMap;
    protected final InternalTypeInfo<RowData> joinKeyTypeInfo;
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
            int numInputs,
            List<JoinType> joinTypes,
            List<GeneratedMultiJoinCondition> joinConditions,
            boolean isFullOuterJoin) {
        this.inputTypeInfos = new ArrayList<>(numInputs);
        this.keySelectors = new ArrayList<>(numInputs);
        this.inputSpecs = new ArrayList<>(numInputs);
        this.joinTypes = joinTypes;
        this.isFullOuterJoin = isFullOuterJoin;
        this.joinConditions = joinConditions;
        this.joinAttributeMap = new HashMap<>();

        initializeInputs(numInputs);
        initializeJoinConditions();

        this.joinKeyTypeInfo = InternalTypeInfo.of(new CharType(false, 20));
        this.keyExtractor =
                new AttributeBasedJoinKeyExtractor(this.joinAttributeMap, this.inputTypeInfos);
    }

    /** Constructor allowing explicit provision of joinAttributeMap for custom conditions. */
    protected StreamingMultiJoinOperatorTestBase(
            int numInputs,
            List<JoinType> joinTypes,
            List<GeneratedMultiJoinCondition> joinConditions,
            Map<Integer, Map<AttributeRef, AttributeRef>> joinAttributeMap,
            boolean isFullOuterJoin) {
        this.inputTypeInfos = new ArrayList<>(numInputs);
        this.keySelectors = new ArrayList<>(numInputs);
        this.inputSpecs = new ArrayList<>(numInputs);
        this.joinTypes = joinTypes;
        this.isFullOuterJoin = isFullOuterJoin;
        this.joinConditions = joinConditions; // Use provided conditions
        this.joinAttributeMap = joinAttributeMap; // Use provided map

        initializeInputs(numInputs);

        this.joinKeyTypeInfo = InternalTypeInfo.of(new CharType(false, 20));
        this.keyExtractor =
                new AttributeBasedJoinKeyExtractor(this.joinAttributeMap, this.inputTypeInfos);
    }

    // ==========================================================================
    // Test Lifecycle
    // ==========================================================================

    @BeforeEach
    void beforeEach() throws Exception {
        testHarness = createTestHarness();
        setupKeySelectorsForTestHarness(testHarness);
        testHarness.setup();
        testHarness.open();
        asserter =
                new RowDataHarnessAssertor(
                        getOutputType().getChildren().toArray(new LogicalType[0]));
    }

    @AfterEach
    void afterEach() throws Exception {
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

    protected static List<GeneratedMultiJoinCondition> defaultConditions() {
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

            var uniqueKeyType = createUniqueKeyType(i);
            if (uniqueKeyType != null) {
                inputSpecs.add(
                        JoinInputSideSpec.withUniqueKey(
                                createUniqueKeyType(i), keySelectors.get(i)));
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
            for (int i = 0; i < inputSpecs.size(); i++) { // Iterate based on number of inputs
                // Add the join condition comparing current input (i) with previous (i-1)
                if (i > 0) {
                    GeneratedMultiJoinCondition condition = createJoinCondition(i, i - 1);
                    joinConditions.add(condition);
                }

                // Populate the attribute map based on the condition's logic (field 0 <-> field 0)
                Map<AttributeRef, AttributeRef> currentJoinMap = new HashMap<>();
                // Left side attribute (previous input, field 0)
                AttributeRef leftAttr = new AttributeRef(i - 1, 0);
                // Right side attribute (current input, field 0)
                AttributeRef rightAttr = new AttributeRef(i, 0);
                // Map: right_input_id -> { left_attr -> right_attr }
                currentJoinMap.put(leftAttr, rightAttr);
                joinAttributeMap.put(i, currentJoinMap);
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
        // Get the expected partition key type (assuming it's the same for all inputs derived by the
        // extractor)
        InternalTypeInfo<RowData> partitionKeyTypeInfo = this.keyExtractor.getJoinKeyType(0);
        if (partitionKeyTypeInfo == null) {
            throw new IllegalStateException(
                    "Could not determine partition key type from keyExtractor for input 0.");
        }

        for (int i = 0; i < this.inputSpecs.size(); i++) {
            // Use the serializable keySelector
            SerializableKeySelector keySelector = new SerializableKeySelector(this.keyExtractor, i);
            // Provide the derived key type information to the harness
            harness.setKeySelector(i, keySelector);
        }
    }

    protected KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> createTestHarness()
            throws Exception {
        // Determine the partition key type using the keyExtractor
        // Assume the key type derived for the first input is representative of the partition key
        InternalTypeInfo<RowData> partitionKeyTypeInfo = this.keyExtractor.getJoinKeyType(0);
        if (partitionKeyTypeInfo == null) {
            throw new IllegalStateException(
                    "Could not determine partition key type from keyExtractor for input 0.");
        }

        KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> harness =
                new KeyedMultiInputStreamOperatorTestHarness<>(
                        new MultiStreamingJoinOperatorFactory(
                                inputSpecs,
                                inputTypeInfos,
                                joinTypes,
                                joinConditions,
                                joinAttributeMap),
                        partitionKeyTypeInfo); // Use the derived key type information

        // Setup key selectors for each input (this now uses the extractor)
        setupKeySelectorsForTestHarness(harness);
        return harness;
    }

    protected RowType getOutputType() {
        var typesStream =
                inputTypeInfos.stream()
                        .flatMap(typeInfo -> typeInfo.toRowType().getChildren().stream());
        var namesStream =
                inputTypeInfos.stream()
                        .flatMap(typeInfo -> typeInfo.toRowType().getFieldNames().stream());

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
    // Factory Class
    // ==========================================================================

    private static class MultiStreamingJoinOperatorFactory
            extends AbstractStreamOperatorFactory<RowData> {

        private static final long serialVersionUID = 1L;
        private final List<JoinInputSideSpec> inputSpecs;
        private final List<InternalTypeInfo<RowData>> inputTypeInfos;
        private final List<JoinType> joinTypes;
        private final List<GeneratedMultiJoinCondition> joinConditions;
        private final JoinKeyExtractor keyExtractor;

        public MultiStreamingJoinOperatorFactory(
                List<JoinInputSideSpec> inputSpecs,
                List<InternalTypeInfo<RowData>> inputTypeInfos,
                List<JoinType> joinTypes,
                List<GeneratedMultiJoinCondition> joinConditions,
                Map<Integer, Map<AttributeRef, AttributeRef>> joinAttributeMap) {
            this.inputSpecs = inputSpecs;
            this.inputTypeInfos = inputTypeInfos;
            this.joinTypes = joinTypes;
            this.joinConditions = joinConditions;
            this.keyExtractor =
                    new AttributeBasedJoinKeyExtractor(joinAttributeMap, inputTypeInfos);
        }

        @Override
        public <T extends StreamOperator<RowData>> T createStreamOperator(
                StreamOperatorParameters<RowData> parameters) {
            StreamingMultiJoinOperator op =
                    createJoinOperator(
                            parameters,
                            inputSpecs,
                            inputTypeInfos,
                            joinTypes,
                            joinConditions,
                            keyExtractor);

            @SuppressWarnings("unchecked")
            T operator = (T) op;
            return operator;
        }

        @Override
        public Class<? extends StreamOperator<RowData>> getStreamOperatorClass(
                ClassLoader classLoader) {
            return StreamingMultiJoinOperator.class;
        }

        private StreamingMultiJoinOperator createJoinOperator(
                StreamOperatorParameters<RowData> parameters,
                List<JoinInputSideSpec> inputSpecs,
                List<InternalTypeInfo<RowData>> inputTypeInfos,
                List<JoinType> joinTypes,
                List<GeneratedMultiJoinCondition> joinConditions,
                JoinKeyExtractor keyExtractor) {

            long[] retentionTime = new long[inputSpecs.size()];
            Arrays.fill(retentionTime, 9999999L);

            MultiJoinCondition multiJoinCondition =
                    createMultiJoinCondition(inputSpecs.size())
                            .newInstance(getClass().getClassLoader());
            MultiJoinCondition[] createdJoinConditions = createJoinConditions(joinConditions);

            return new StreamingMultiJoinOperator(
                    parameters,
                    inputTypeInfos,
                    inputSpecs,
                    joinTypes,
                    multiJoinCondition,
                    retentionTime,
                    createdJoinConditions,
                    keyExtractor);
        }

        private MultiJoinCondition[] createJoinConditions(
                List<GeneratedMultiJoinCondition> generatedJoinConditions) {
            MultiJoinCondition[] conditions = new MultiJoinCondition[inputSpecs.size()];
            // We expect generatedJoinConditions size to match inputSpecs size (or joinTypes size)
            if (generatedJoinConditions.size() != inputSpecs.size()) {
                throw new IllegalArgumentException(
                        "The number of generated join conditions must match the number of inputs/joins."
                                + "This might be due to an incorrect joinAttributeMap or incorrect joinConditions. All parameters derived from join conditions have to match!");
            }
            for (int i = 0; i < inputSpecs.size(); i++) {
                GeneratedMultiJoinCondition generatedCondition = generatedJoinConditions.get(i);
                if (generatedCondition
                        != null) { // Allow null conditions (e.g., for INNER joins without specific
                    // cond)
                    try {
                        conditions[i] = generatedCondition.newInstance(getClass().getClassLoader());
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to instantiate join condition for input " + i, e);
                    }
                } else {
                    conditions[i] = null; // Explicitly set to null if no condition provided
                }
            }
            return conditions;
        }
    }

    // ==========================================================================
    // Type Creation Methods
    // ==========================================================================

    protected InternalTypeInfo<RowData> createInputTypeInfo(int inputIndex) {
        return InternalTypeInfo.of(
                RowType.of(
                        new LogicalType[] {
                            new CharType(false, 20),
                            new CharType(false, 20),
                            VarCharType.STRING_TYPE
                        },
                        new String[] {
                            String.format("user_id_%d", inputIndex),
                            String.format("id_%d", inputIndex),
                            String.format("details_%d", inputIndex)
                        }));
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
                inputTypeInfos
                        .get(inputIndex)
                        .toRowType()
                        .getChildren()
                        .toArray(new LogicalType[0]));
    }

    protected static GeneratedMultiJoinCondition createMultiJoinCondition(int numInputs) {
        String generatedClassName = "DefaultGlobalEquiKeyCondition_manual";
        return new GeneratedMultiJoinCondition(generatedClassName, "", new Object[0]) {
            @Override
            public MultiJoinCondition newInstance(ClassLoader classLoader) {
                return new DefaultGlobalEquiKeyCondition(numInputs);
            }
        };
    }

    /**
     * Creates a default GeneratedMultiJoinCondition that compares the join key (field 0) between
     * the input at `index` and the input at `indexToCompare`. This is typically used for the ON
     * clause of a specific join step (e.g., A LEFT JOIN B **ON A.key = B.key**).
     *
     * @param rightInputInArray The index of the current input stream (the right side of the
     *     conceptual join step) in the `inputs` array of `MultiJoinCondition.apply()`.
     * @param leftInputInArray The index of the input stream to compare against (the left side) in
     *     the `inputs` array.
     * @return A GeneratedMultiJoinCondition representing the equality check on field 0.
     */
    protected static GeneratedMultiJoinCondition createJoinCondition(
            int rightInputInArray, int leftInputInArray) {
        // Ensure indices are valid for comparison
        if (rightInputInArray <= 0
                || leftInputInArray < 0
                || rightInputInArray == leftInputInArray) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid indices for creating join condition. rightInputInArray: %d, leftInputInArray: %d",
                            rightInputInArray, leftInputInArray));
        }

        String generatedClassName =
                String.format(
                        "SpecificInputsEquiKeyCondition_manual_%d_%d",
                        rightInputInArray, leftInputInArray);
        return new GeneratedMultiJoinCondition(generatedClassName, "", new Object[0]) {
            @Override
            public MultiJoinCondition newInstance(ClassLoader classLoader) {
                // Field 0 is assumed for key comparison in this default test condition
                return new SpecificInputsEquiKeyCondition(
                        leftInputInArray, 0, rightInputInArray, 0);
            }
        };
    }

    // ==========================================================================
    // Concrete MultiJoinCondition Implementations for TestBase
    // ==========================================================================

    /**
     * Checks if all inputs (from `inputs[0]` to `inputs[numInputs-1]`) are non-null, have a
     * non-null string at field 0, and these strings are all equal to `inputs[0].getString(0)`. This
     * is used as a global filter condition in some tests.
     */
    private static class DefaultGlobalEquiKeyCondition extends AbstractRichFunction
            implements MultiJoinCondition {
        private final int numInputs;

        public DefaultGlobalEquiKeyCondition(int numInputs) {
            this.numInputs = numInputs;
        }

        @Override
        public boolean apply(RowData[] inputs) {
            if (inputs == null || inputs.length < numInputs) {
                return false; // Not enough inputs provided to the condition
            }

            if (numInputs <= 1) {
                return numInputs != 1 || (inputs[0] != null && !inputs[0].isNullAt(0));
            }

            if (inputs[0] == null || inputs[0].isNullAt(0)) {
                return false; // Reference key (inputs[0].getString(0)) would be null
            }

            // Compare inputs[0] (field 0) with inputs[i] (field 0) for i = 1 to numInputs - 1
            for (int i = 1; i < numInputs; i++) {
                // We reuse SpecificInputsEquiKeyCondition for each pair comparison.
                // leftInputIndex = 0, leftKeyFieldIndex = 0
                // rightInputIndex = i, rightKeyFieldIndex = 0
                SpecificInputsEquiKeyCondition pairCondition =
                        new SpecificInputsEquiKeyCondition(0, 0, i, 0);
                if (!pairCondition.apply(inputs)) {
                    return false; // Found a pair that doesn't match on key field 0
                }
            }
            return true;
        }
    }

    /**
     * Checks if `inputs[leftInputIndex].getString(leftKeyFieldIndex)` is equal to
     * `inputs[rightInputIndex].getString(rightKeyFieldIndex)`. Used for specific join step
     * conditions.
     */
    private static class SpecificInputsEquiKeyCondition extends AbstractRichFunction
            implements MultiJoinCondition {
        private final int leftInputIndex;
        private final int leftKeyFieldIndex;
        private final int rightInputIndex;
        private final int rightKeyFieldIndex;

        public SpecificInputsEquiKeyCondition(
                int leftInputIndex,
                int leftKeyFieldIndex,
                int rightInputIndex,
                int rightKeyFieldIndex) {
            this.leftInputIndex = leftInputIndex;
            this.leftKeyFieldIndex = leftKeyFieldIndex;
            this.rightInputIndex = rightInputIndex;
            this.rightKeyFieldIndex = rightKeyFieldIndex;
        }

        @Override
        public boolean apply(RowData[] inputs) {
            // Basic null checks for safety
            if (inputs == null
                    || inputs.length <= Math.max(leftInputIndex, rightInputIndex)
                    || inputs[leftInputIndex] == null
                    || inputs[rightInputIndex] == null) {
                return false;
            }
            // Check null keys
            if (inputs[leftInputIndex].isNullAt(leftKeyFieldIndex)
                    || inputs[rightInputIndex].isNullAt(rightKeyFieldIndex)) {
                return false;
            }
            // Compare the join keys
            String keyLeft = inputs[leftInputIndex].getString(leftKeyFieldIndex).toString();
            String keyRight = inputs[rightInputIndex].getString(rightKeyFieldIndex).toString();
            return keyLeft.equals(keyRight);
        }
    }

    /**
     * Condition for comparing a Long field from the left input as greater than a Long field from
     * the right input. Example: leftInput.field > rightInput.field
     */
    protected static class FieldLongGreaterThanConditionImpl extends AbstractRichFunction
            implements MultiJoinCondition {
        private final int leftInputIndex;
        private final int leftFieldIdx;
        private final int rightInputIndex;
        private final int rightFieldIdx;

        public FieldLongGreaterThanConditionImpl(
                int leftInputIndex, int leftFieldIdx, int rightInputIndex, int rightFieldIdx) {
            this.leftInputIndex = leftInputIndex;
            this.leftFieldIdx = leftFieldIdx;
            this.rightInputIndex = rightInputIndex;
            this.rightFieldIdx = rightFieldIdx;
        }

        @Override
        public boolean apply(RowData[] inputs) {
            if (inputs == null
                    || inputs.length <= Math.max(leftInputIndex, rightInputIndex)
                    || inputs[leftInputIndex] == null
                    || inputs[rightInputIndex] == null) {
                return false;
            }
            if (inputs[leftInputIndex].isNullAt(leftFieldIdx)
                    || inputs[rightInputIndex].isNullAt(rightFieldIdx)) {
                return false;
            }
            return inputs[leftInputIndex].getLong(leftFieldIdx)
                    > inputs[rightInputIndex].getLong(rightFieldIdx);
        }
    }

    /** Combines multiple MultiJoinConditions with AND logic. */
    protected static class AndMultiJoinConditionImpl extends AbstractRichFunction
            implements MultiJoinCondition {
        private final MultiJoinCondition[] conditions;

        public AndMultiJoinConditionImpl(MultiJoinCondition... conditions) {
            this.conditions = conditions;
        }

        @Override
        public boolean apply(RowData[] inputs) {
            for (MultiJoinCondition condition : conditions) {
                if (condition == null) { // Should not happen if constructed properly via factory
                    return false;
                }
                if (!condition.apply(inputs)) {
                    return false;
                }
            }
            return true;
        }
    }

    // Factory Methods for Conditions
    protected static GeneratedMultiJoinCondition createFieldLongGreaterThanCondition(
            int leftInputIdx, int leftFieldIdx, int rightInputIdx, int rightFieldIdx) {
        String generatedClassName =
                String.format(
                        "FieldLongGreaterThanCondition_manual_%d_%d_%d_%d",
                        leftInputIdx, leftFieldIdx, rightInputIdx, rightFieldIdx);
        return new GeneratedMultiJoinCondition(generatedClassName, "", new Object[0]) {
            @Override
            public MultiJoinCondition newInstance(ClassLoader classLoader) {
                return new FieldLongGreaterThanConditionImpl(
                        leftInputIdx, leftFieldIdx, rightInputIdx, rightFieldIdx);
            }
        };
    }

    protected static GeneratedMultiJoinCondition createAndCondition(
            GeneratedMultiJoinCondition... generatedConditions) {
        String generatedClassName =
                "AndMultiJoinCondition_manual"; // Name can be made more unique if needed
        return new GeneratedMultiJoinCondition(generatedClassName, "", new Object[0]) {
            @Override
            public MultiJoinCondition newInstance(ClassLoader classLoader) {
                MultiJoinCondition[] actualConditions =
                        new MultiJoinCondition[generatedConditions.length];
                for (int i = 0; i < generatedConditions.length; i++) {
                    if (generatedConditions[i] == null) {
                        // This case should ideally be prevented by how createAndCondition is called
                        // or handled by AndMultiJoinConditionImpl if nulls are permissible for some
                        // reason.
                        // For now, let's assume valid conditions are passed.
                        throw new IllegalArgumentException(
                                "Null GeneratedMultiJoinCondition passed to createAndCondition");
                    }
                    actualConditions[i] = generatedConditions[i].newInstance(classLoader);
                }
                return new AndMultiJoinConditionImpl(actualConditions);
            }
        };
    }
}
