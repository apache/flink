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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedMultiJoinCondition;
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
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

    protected RowDataHarnessAssertor assertor;
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
        assertor =
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

    protected void insertUser(String userId, String userName, String details) throws Exception {
        processRecord(0, INSERT, userId, userName, details);
    }

    protected void insertOrder(String userId, String orderId, String details) throws Exception {
        processRecord(1, INSERT, userId, orderId, details);
    }

    protected void insertPayment(String userId, String paymentId, String details) throws Exception {
        processRecord(2, INSERT, userId, paymentId, details);
    }

    protected void updateBeforeUser(String userId, String userName, String details)
            throws Exception {
        processRecord(0, UPDATE_BEFORE, userId, userName, details);
    }

    protected void updateAfterUser(String userId, String userName, String details)
            throws Exception {
        processRecord(0, UPDATE_AFTER, userId, userName, details);
    }

    protected void updateBeforeOrder(String userId, String orderId, String details)
            throws Exception {
        processRecord(1, UPDATE_BEFORE, userId, orderId, details);
    }

    protected void updateAfterOrder(String userId, String orderId, String details)
            throws Exception {
        processRecord(1, UPDATE_AFTER, userId, orderId, details);
    }

    protected void updateBeforePayment(String userId, String paymentId, String details)
            throws Exception {
        processRecord(2, UPDATE_BEFORE, userId, paymentId, details);
    }

    protected void updateAfterPayment(String userId, String paymentId, String details)
            throws Exception {
        processRecord(2, UPDATE_AFTER, userId, paymentId, details);
    }

    protected void deleteUser(String userId, String userName, String details) throws Exception {
        processRecord(0, DELETE, userId, userName, details);
    }

    protected void deleteOrder(String userId, String orderId, String details) throws Exception {
        processRecord(1, DELETE, userId, orderId, details);
    }

    protected void deletePayment(String userId, String paymentId, String details) throws Exception {
        processRecord(2, DELETE, userId, paymentId, details);
    }

    protected static List<GeneratedMultiJoinCondition> defaultConditions() {
        return new ArrayList<>();
    }

    // ==========================================================================
    // Assertion Methods
    // ==========================================================================

    protected void emits(RowKind kind, String... fields) throws Exception {
        assertor.shouldEmit(testHarness, rowOfKind(kind, fields));
    }

    protected void emitsNothing() {
        assertor.shouldEmitNothing(testHarness);
    }

    protected void emits(RowKind kind1, String[] fields1, RowKind kind2, String[] fields2)
            throws Exception {
        assertor.shouldEmitAll(testHarness, rowOfKind(kind1, fields1), rowOfKind(kind2, fields2));
    }

    protected void emits(
            RowKind kind1,
            String[] fields1,
            RowKind kind2,
            String[] fields2,
            RowKind kind3,
            String[] fields3)
            throws Exception {
        assertor.shouldEmitAll(
                testHarness,
                rowOfKind(kind1, fields1),
                rowOfKind(kind2, fields2),
                rowOfKind(kind3, fields3));
    }

    protected void emits(
            RowKind kind1,
            String[] fields1,
            RowKind kind2,
            String[] fields2,
            RowKind kind3,
            String[] fields3,
            RowKind kind4,
            String[] fields4)
            throws Exception {
        assertor.shouldEmitAll(
                testHarness,
                rowOfKind(kind1, fields1),
                rowOfKind(kind2, fields2),
                rowOfKind(kind3, fields3),
                rowOfKind(kind4, fields4));
    }

    // ==========================================================================
    // Private Helper Methods
    // ==========================================================================

    private void initializeInputs(int numInputs) {
        if (numInputs < 2) {
            throw new IllegalArgumentException("Number of inputs must be a" + "t least 2");
        }

        // In our test, the first input is always the one with the unique key as a join key
        inputTypeInfos.add(createInputTypeInfo(0));
        keySelectors.add(createKeySelector(0));
        inputSpecs.add(
                JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                        createUniqueKeyType(0), keySelectors.get(0)));

        // Following tables contain a unique key but are not contained in the join key
        for (int i = 1; i < numInputs; i++) {
            inputTypeInfos.add(createInputTypeInfo(i));
            keySelectors.add(createKeySelector(i));
            inputSpecs.add(
                    JoinInputSideSpec.withUniqueKey(createUniqueKeyType(i), keySelectors.get(i)));
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

    private void processRecord(int inputIndex, RowKind kind, String... fields) throws Exception {
        StreamRecord<RowData> record;
        switch (kind) {
            case INSERT:
                record = StreamRecordUtils.insertRecord((Object[]) fields);
                break;
            case UPDATE_BEFORE:
                record = StreamRecordUtils.updateBeforeRecord((Object[]) fields);
                break;
            case UPDATE_AFTER:
                record = StreamRecordUtils.updateAfterRecord((Object[]) fields);
                break;
            case DELETE:
                record = StreamRecordUtils.deleteRecord((Object[]) fields);
                break;
            default:
                throw new IllegalArgumentException("Unsupported RowKind: " + kind);
        }
        testHarness.processElement(inputIndex, record);
    }

    private void setupKeySelectorsForTestHarness(
            KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> harness) {
        for (int i = 0; i < this.inputSpecs.size(); i++) {
            /* Testcase: our join key is always the first key for all tables and that's why 0 */
            KeySelector<RowData, RowData> keySelector = row -> GenericRowData.of(row.getString(0));
            harness.setKeySelector(i, keySelector);
        }
    }

    protected KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> createTestHarness()
            throws Exception {
        KeyedMultiInputStreamOperatorTestHarness<RowData, RowData> harness =
                new KeyedMultiInputStreamOperatorTestHarness<>(
                        new MultiStreamingJoinOperatorFactory(
                                inputSpecs,
                                inputTypeInfos,
                                joinTypes,
                                joinConditions,
                                joinAttributeMap),
                        TypeInformation.of(RowData.class));

        // Setup key selectors for each input
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

    protected RowData rowOfKind(RowKind kind, String... fields) {
        return StreamRecordUtils.rowOfKind(kind, (Object[]) fields);
    }

    protected String[] r(String... values) {
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
        private final Map<Integer, Map<AttributeRef, AttributeRef>> joinAttributeMap;
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
            this.joinAttributeMap = joinAttributeMap;
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
                        "The number of generated join conditions must match the number of inputs/joins.");
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
                            String.format("id_%d", inputIndex),
                        }));
    }

    protected RowDataKeySelector createKeySelector(int inputIndex) {
        return HandwrittenSelectorUtil.getRowDataSelector(
                /* Testcase: primary key is 0 for the first table and 1 for all others */
                new int[] {inputIndex == 0 ? 0 : 1},
                inputTypeInfos
                        .get(inputIndex)
                        .toRowType()
                        .getChildren()
                        .toArray(new LogicalType[0]));
    }

    protected static GeneratedMultiJoinCondition createMultiJoinCondition(int numInputs) {
        String funcCode =
                "public class MultiConditionFunction extends org.apache.flink.api.common.functions.AbstractRichFunction "
                        + "implements org.apache.flink.table.runtime.generated.MultiJoinCondition {\n"
                        + "    public MultiConditionFunction(Object[] reference) {}\n"
                        + "    @Override\n"
                        + "    public boolean apply(org.apache.flink.table.data.RowData[] inputs) {\n"
                        + "        if (inputs == null || inputs.length < "
                        + numInputs
                        + ") {\n"
                        + "            return false;\n"
                        + "        }\n"
                        + "        for (org.apache.flink.table.data.RowData input : inputs) {\n"
                        + "            if (input == null || input.isNullAt(0)) {\n"
                        + "                return false;\n"
                        + "            }\n"
                        + "        }\n"
                        + "        String referenceKey = inputs[0].getString(0).toString();\n"
                        + "        for (int i = 1; i < inputs.length; i++) {\n"
                        + "            if (!referenceKey.equals(inputs[i].getString(0).toString())) {\n"
                        + "                return false;\n"
                        + "            }\n"
                        + "        }\n"
                        + "        return true;\n"
                        + "    }\n"
                        + "    @Override\n"
                        + "    public void close() throws Exception {\n"
                        + "        super.close();\n"
                        + "    }\n"
                        + "}\n";
        return new GeneratedMultiJoinCondition("MultiConditionFunction", funcCode, new Object[0]);
    }

    /**
     * Creates a default GeneratedMultiJoinCondition that compares the join key (field 0) between
     * the input at `index` and the input at `indexToCompare`. This is typically used for the ON
     * clause of a specific join step (e.g., A LEFT JOIN B **ON A.key = B.key**).
     *
     * @param index The index of the current input stream (the right side of the conceptual join
     *     step).
     * @param indexToCompare The index of the input stream to compare against (the left side).
     * @return A GeneratedMultiJoinCondition representing the equality check.
     */
    protected static GeneratedMultiJoinCondition createJoinCondition(
            int index, int indexToCompare) {
        // Ensure indices are valid for comparison
        if (index <= 0 || indexToCompare < 0 || index == indexToCompare) {
            throw new IllegalArgumentException("Invalid indices for creating join condition.");
        }

        String funcCode =
                "public class JoinConditionFunction_"
                        + index
                        + "_"
                        + indexToCompare
                        + " extends org.apache.flink.api.common.functions.AbstractRichFunction "
                        + "implements org.apache.flink.table.runtime.generated.MultiJoinCondition {\n"
                        + "    private final int index = "
                        + index
                        + ";\n"
                        + "    private final int indexToCompare = "
                        + indexToCompare
                        + ";\n"
                        + "    public JoinConditionFunction_"
                        + index
                        + "_"
                        + indexToCompare
                        + "(Object[] reference) {}\n"
                        + "    @Override\n"
                        + "    public boolean apply(org.apache.flink.table.data.RowData[] inputs) {\n"
                        + "        // Basic null checks for safety\n"
                        + "        if (inputs == null || inputs.length <= Math.max(index, indexToCompare) || inputs[indexToCompare] == null || inputs[index] == null) {\n"
                        + "            return false;\n"
                        + "        }\n"
                        + "        // Check null keys\n"
                        + "        if (inputs[indexToCompare].isNullAt(0) || inputs[index].isNullAt(0)) {\n"
                        + "            return false;\n"
                        + "        }\n"
                        + "        // Compare the join keys (field 0 in this test setup)\n"
                        + "        String keyToCompare = inputs[indexToCompare].getString(0).toString();\n"
                        + "        String currentKey = inputs[index].getString(0).toString();\n"
                        + "        return keyToCompare.equals(currentKey);\n"
                        + "    }\n"
                        + "    @Override\n"
                        + "    public void close() throws Exception {\n"
                        + "        super.close();\n"
                        + "    }\n"
                        + "}\n";
        // Use unique class name to avoid conflicts if multiple conditions are generated
        return new GeneratedMultiJoinCondition(
                "JoinConditionFunction_" + index + "_" + indexToCompare, funcCode, new Object[0]);
    }
}
