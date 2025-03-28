package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedMultiJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class StreamingMultiJoinOperatorTestBase {

    protected final List<InternalTypeInfo<RowData>> inputTypeInfos;
    protected final List<RowDataKeySelector> keySelectors;
    protected final List<JoinInputSideSpec> inputSpecs;

    protected final InternalTypeInfo<RowData> joinKeyTypeInfo;
    protected RowDataHarnessAssertor assertor;
    protected KeyedMultiInputStreamOperatorTestHarness<Integer, RowData> testHarness;

    protected StreamingMultiJoinOperatorTestBase(int numInputs) {
        // Initialize collections
        this.inputTypeInfos = new ArrayList<>(numInputs);
        this.keySelectors = new ArrayList<>(numInputs);
        this.inputSpecs = new ArrayList<>(numInputs);

        // Initialize default types for each input
        for (int i = 0; i < numInputs; i++) {
            inputTypeInfos.add(createInputTypeInfo(i));
            keySelectors.add(createKeySelector(i));
        }

        // Create input specs
        for (int i = 0; i < numInputs; i++) {
            inputSpecs.add(
                    JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                            inputTypeInfos.get(i), keySelectors.get(i)));
        }

        // Join key type (assuming common key type across all inputs)
        this.joinKeyTypeInfo = InternalTypeInfo.of(new CharType(false, 20));
    }

    protected InternalTypeInfo<RowData> createInputTypeInfo(int inputIndex) {
        return InternalTypeInfo.of(
                RowType.of(
                        new LogicalType[] {
                            new CharType(false, 20),
                            new CharType(false, 20),
                            VarCharType.STRING_TYPE
                        },
                        new String[] {
                            String.format("id_%d", inputIndex),
                            String.format("key_%d", inputIndex),
                            String.format("payload_%d", inputIndex)
                        }));
    }

    protected RowDataKeySelector createKeySelector(int inputIndex) {
        return HandwrittenSelectorUtil.getRowDataSelector(
                new int[] {1}, // Assuming key is always second column
                inputTypeInfos
                        .get(inputIndex)
                        .toRowType()
                        .getChildren()
                        .toArray(new LogicalType[0]));
    }

    @BeforeEach
    void beforeEach() throws Exception {
        testHarness = createTestHarness();
        setupKeySelectors(testHarness);
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

    /** Get the output row type of join operator. */
    protected RowType getOutputType() {
        var typesStream =
                this.inputTypeInfos.stream()
                        .flatMap(typeInfo -> typeInfo.toRowType().getChildren().stream());
        var namesStream =
                this.inputTypeInfos.stream()
                        .flatMap(typeInfo -> typeInfo.toRowType().getFieldNames().stream());

        return RowType.of(
                typesStream.toArray(LogicalType[]::new), namesStream.toArray(String[]::new));
    }

    /** Factory class for creating StreamingMultiWayJoinOperator instances. */
    private static class MultiStreamingJoinOperatorFactory
            extends AbstractStreamOperatorFactory<RowData> {

        private final List<JoinInputSideSpec> inputSpecs;
        protected final List<InternalTypeInfo<RowData>> inputTypeInfos;

        public MultiStreamingJoinOperatorFactory(
                List<JoinInputSideSpec> inputSpecs,
                List<InternalTypeInfo<RowData>> inputTypeInfos) {
            this.inputSpecs = inputSpecs;
            this.inputTypeInfos = inputTypeInfos;
        }

        @Override
        public <T extends StreamOperator<RowData>> T createStreamOperator(
                StreamOperatorParameters<RowData> parameters) {
            StreamingMultiJoinOperator op =
                    createJoinOperator(parameters, inputSpecs, inputTypeInfos);
            return (T) op;
        }

        @Override
        public Class<? extends StreamOperator<RowData>> getStreamOperatorClass(
                ClassLoader classLoader) {
            return StreamingMultiJoinOperator.class;
        }

        protected StreamingMultiJoinOperator createJoinOperator(
                StreamOperatorParameters<RowData> parameters,
                List<JoinInputSideSpec> inputSpecs,
                List<InternalTypeInfo<RowData>> inputTypeInfos) {
            // Create join conditions (for now just using a simple condition that always returns
            // true)
            List<GeneratedJoinCondition> generatedConditions = new ArrayList<>();
            for (int i = 0; i < inputSpecs.size(); i++) {
                generatedConditions.add(createJoinCondition());
            }

            // Convert generated conditions to runtime conditions
            List<JoinCondition> joinConditions = new ArrayList<>();
            for (GeneratedJoinCondition genCond : generatedConditions) {
                try {
                    joinConditions.add(genCond.newInstance(getClass().getClassLoader()));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to instantiate join condition", e);
                }
            }

            // Create filter nulls array (for now setting all to false)
            boolean[] filterNulls = new boolean[inputSpecs.size()];
            Arrays.fill(filterNulls, false);

            // Create multi-join condition if we're using multiple inputs
            org.apache.flink.table.runtime.generated.MultiJoinCondition multiJoinCondition = null;
            if (inputSpecs.size() > 1) {
                try {
                    multiJoinCondition =
                            createMultiJoinCondition(inputSpecs.size())
                                    .newInstance(getClass().getClassLoader());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to instantiate multi-join condition", e);
                }
            }

            // array filled with max rentention time
            long[] retentionTime = new long[inputSpecs.size()];
            for (int i = 0; i < inputSpecs.size(); i++) {
                retentionTime[i] = 9999999L;
            }

            return new StreamingMultiJoinOperator(
                    parameters,
                    inputTypeInfos,
                    inputSpecs,
                    joinConditions,
                    multiJoinCondition,
                    filterNulls,
                    retentionTime);
        }

        // In the real multi join, we'll receive a list of rexnodes with the join condition to
        // evaluate
        private GeneratedJoinCondition createJoinCondition() {
            String funcCode =
                    "public class ConditionFunction extends org.apache.flink.api.common.functions.AbstractRichFunction "
                            + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                            + "\n"
                            + "    public ConditionFunction(Object[] reference) {\n"
                            + "    }\n"
                            + "\n"
                            + "    @Override\n"
                            + "    public boolean apply(org.apache.flink.table.data.RowData in1, org.apache.flink.table.data.RowData in2) {\n"
                            + "        // Compare the key fields (second column, index 1)\n"
                            + "        if (in1.isNullAt(1) || in2.isNullAt(1)) {\n"
                            + "            return false;\n"
                            + "        }\n"
                            + "        return in1.getString(1).toString().equals(in2.getString(1).toString());\n"
                            + "    }\n"
                            + "\n"
                            + "    @Override\n"
                            + "    public void close() throws Exception {\n"
                            + "        super.close();\n"
                            + "    }\n"
                            + "}\n";
            return new GeneratedJoinCondition("ConditionFunction", funcCode, new Object[0]);
        }

        // Create a dummy MultiJoinCondition that checks if all inputs have the same join key value
        private GeneratedMultiJoinCondition createMultiJoinCondition(int numInputs) {
            String funcCode =
                    "public class MultiConditionFunction extends org.apache.flink.api.common.functions.AbstractRichFunction "
                            + "implements org.apache.flink.table.runtime.generated.MultiJoinCondition {\n"
                            + "\n"
                            + "    public MultiConditionFunction(Object[] reference) {\n"
                            + "    }\n"
                            + "\n"
                            + "    @Override\n"
                            + "    public boolean apply(org.apache.flink.table.data.RowData[] inputs) {\n"
                            + "        // If any input doesn't exist yet, we can't evaluate\n"
                            + "        if (inputs == null || inputs.length < "
                            + numInputs
                            + ") {\n"
                            + "            return false;\n"
                            + "        }\n"
                            + "\n"
                            + "        // First, check for nulls\n"
                            + "        for (org.apache.flink.table.data.RowData input : inputs) {\n"
                            + "            if (input == null || input.isNullAt(1)) {\n"
                            + "                return false;\n"
                            + "            }\n"
                            + "        }\n"
                            + "\n"
                            + "        // Get the first key as reference\n"
                            + "        String referenceKey = inputs[0].getString(1).toString();\n"
                            + "\n"
                            + "        // Check if all keys match the reference\n"
                            + "        for (int i = 1; i < inputs.length; i++) {\n"
                            + "            String currentKey = inputs[i].getString(1).toString();\n"
                            + "            if (!referenceKey.equals(currentKey)) {\n"
                            + "                return false;\n"
                            + "            }\n"
                            + "        }\n"
                            + "\n"
                            + "        return true;\n"
                            + "    }\n"
                            + "\n"
                            + "    @Override\n"
                            + "    public void close() throws Exception {\n"
                            + "        super.close();\n"
                            + "    }\n"
                            + "}\n";
            return new org.apache.flink.table.runtime.generated.GeneratedMultiJoinCondition(
                    "MultiConditionFunction", funcCode, new Object[0]);
        }
    }

    private void setupKeySelectors(
            KeyedMultiInputStreamOperatorTestHarness<Integer, RowData> harness) {
        for (int i = 0; i < this.inputSpecs.size(); i++) {
            KeySelector<RowData, Integer> keySelector =
                    row -> Integer.parseInt(row.getString(1).toString());
            harness.setKeySelector(i, keySelector);
        }
    }

    protected KeyedMultiInputStreamOperatorTestHarness<Integer, RowData> createTestHarness()
            throws Exception {
        return new KeyedMultiInputStreamOperatorTestHarness<>(
                new MultiStreamingJoinOperatorFactory(inputSpecs, inputTypeInfos),
                TypeInformation.of(Integer.class));
    }
}
