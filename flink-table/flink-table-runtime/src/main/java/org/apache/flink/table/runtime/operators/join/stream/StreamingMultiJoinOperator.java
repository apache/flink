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

import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Streaming multi-way join operator which supports inner join and left/right/full outer join. It
 * eliminates the intermediate state necessary for a chain of multiple binary joins. In other words,
 * it reduces the total amount of state necessary for chained joins. As of time complexity, it
 * performs better in the worst cases where the number of records in the intermediate state is large
 * but worst than reordered binary joins when the number of records in the intermediate state is
 * small.
 *
 * <p>The logic of this operator is similar to the one in
 */
public class StreamingMultiJoinOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData> {
    private static final long serialVersionUID = 1L;

    /** List of supported join types. */
    public enum JoinType {
        INNER,
        LEFT
    }

    private final List<JoinInputSideSpec> inputSpecs;
    private final List<JoinType> joinTypes;
    private final List<InternalTypeInfo<RowData>> inputTypes;
    private final MultiJoinCondition multiJoinCondition;
    private final long[] stateRetentionTime;
    private final List<Input<RowData>> typedInputs;
    private final MultiJoinCondition[] outerJoinConditions;

    private transient List<JoinRecordStateView> stateHandlers;
    private transient TimestampedCollector<RowData> collector;
    private transient List<RowData> nullRows;

    /** Represents the different phases of the join process. */
    private enum JoinPhase {
        /** Phase where we calculate match counts (associations) without emitting results. */
        CALCULATE_MATCHES,
        /** Phase where we emit the actual join results. */
        EMIT_RESULTS
    }

    public StreamingMultiJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            List<InternalTypeInfo<RowData>> inputTypes,
            List<JoinInputSideSpec> inputSpecs,
            List<JoinType> joinTypes,
            MultiJoinCondition multiJoinCondition,
            long[] stateRetentionTime,
            MultiJoinCondition[] outerJoinConditions) {
        super(parameters, inputSpecs.size());
        this.inputTypes = inputTypes;
        this.inputSpecs = inputSpecs;
        this.joinTypes = joinTypes;
        this.multiJoinCondition = multiJoinCondition;
        this.stateRetentionTime = stateRetentionTime;
        this.outerJoinConditions = outerJoinConditions;
        this.typedInputs = new ArrayList<>(inputSpecs.size());
    }

    @Override
    public void open() throws Exception {
        super.open();
        initializeCollector();
        initializeNullRows();
        initializeStateHandlers();
    }

    @Override
    public void close() throws Exception {
        closeConditions();
        super.close();
    }

    public void processElement(int inputId, StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();

        // We perform the multi-way join for the input streams
        performMultiJoin(input, inputId);

        addRecordToState(input, inputId);
    }

    private void performMultiJoin(RowData input, int inputId) throws Exception {
        if (input == null) {
            return;
        }

        int[] associations = createInitialAssociations();
        RowData[] currentRows = new RowData[inputSpecs.size()];

        recursiveMultiJoin(
                0, input, inputId, currentRows, associations, JoinPhase.CALCULATE_MATCHES);
    }

    /*
        TODO: I'll have a second pass here to try to make this more detailed as easy to understand
        as I can.
        - We'll not add the input to the state directly
        - We process records recursively, handling each join input one by one
        - We maintain an array of "associations" that tracks the number of matching records
        - For each record from the state at the current depth, we:
          1. Set currentRows[depth] to the current record
          2. For left joins, check if the outer join condition matches
          3. For left joins, update the association count for the previous depth
          4. Reset associations[depth] to 0 for left joins
          5. Continue recursion to the next depth
        - For left joins, if no matches were found and associations[depth-1] is 0, we:
          1. Process with null padding by setting currentRows[depth] to a null row
          2. Continue recursion to the next depth
        - When depth equals inputId, we process the input record specifically by:
          1. Handling retraction before input for upserts with left join
          2. Setting currentRows[depth] to the input
          3. Checking outer join conditions and updating associations for left joins
          4. Continuing recursion with the EMIT_RESULTS phase
          5. Handling insertion after input for non-upserts with left join
        - At max depth (depth == inputSpecs.size()), we check join conditions and emit results
    */
    private boolean recursiveMultiJoin(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            int[] associations,
            JoinPhase phase)
            throws Exception {
        if (depth == inputSpecs.size()) {
            return processJoinAtMaxDepth(depth, input, currentRows, phase);
        }

        boolean isLeftJoin = isLeftJoinAtDepth(depth);
        boolean matched =
                processRecords(depth, input, inputId, currentRows, associations, phase, isLeftJoin);

        if (isLeftJoin && !matched && associations[depth - 1] == 0) {
            matched =
                    processWithNullPadding(depth, input, inputId, currentRows, associations, phase);
        }

        if (depth == inputId) {
            matched = processInputRecord(depth, input, inputId, currentRows, associations);
        }

        return matched;
    }

    private boolean processJoinAtMaxDepth(
            int depth, RowData input, RowData[] currentRows, JoinPhase phase) {

        boolean isLeftJoin = isLeftJoinAtLastLevel(depth);

        if (!isLeftJoin && !multiJoinCondition.apply(currentRows)) {
            return false;
        }

        if (phase == JoinPhase.CALCULATE_MATCHES) {
            return true;
        }

        emitRow(input.getRowKind(), currentRows);
        return true;
    }

    private boolean processRecords(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            int[] associations,
            JoinPhase phase,
            boolean isLeftJoin)
            throws Exception {
        boolean matched = false;
        Iterable<RowData> records = stateHandlers.get(depth).getRecords();
        for (RowData record : records) {
            currentRows[depth] = record;

            if (isLeftJoin) {
                if (noMatch(depth, currentRows)) {
                    continue;
                }

                updateAssociationCount(
                        depth, associations, shouldIncrementAssociation(phase, input));
            }

            if (isLeftJoin) {
                associations[depth] = 0;
            }

            matched =
                    recursiveMultiJoin(depth + 1, input, inputId, currentRows, associations, phase);
        }

        return matched;
    }

    private boolean processWithNullPadding(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            int[] associations,
            JoinPhase phase)
            throws Exception {

        currentRows[depth] = nullRows.get(depth);
        return recursiveMultiJoin(depth + 1, input, inputId, currentRows, associations, phase);
    }

    private boolean processInputRecord(
            int depth, RowData input, int inputId, RowData[] currentRows, int[] associations)
            throws Exception {

        boolean matched;
        boolean isLeftJoin = isLeftJoinAtDepth(depth);
        RowKind inputRowKind = input.getRowKind();

        if (isUpsert(input) && isLeftJoin && associations[depth - 1] == 0) {
            handleRetractBeforeInput(depth, input, inputId, currentRows, associations);
        }

        currentRows[depth] = input;

        if (isLeftJoin) {
            if (noMatch(depth, currentRows)) {
                return false;
            }
            updateAssociationCount(
                    depth, associations, shouldIncrementAssociation(JoinPhase.EMIT_RESULTS, input));
        }

        input.setRowKind(inputRowKind);
        matched =
                recursiveMultiJoin(
                        depth + 1,
                        input,
                        inputId,
                        currentRows,
                        associations,
                        JoinPhase.EMIT_RESULTS);

        if (!isUpsert(input) && isLeftJoin && associations[depth - 1] == 0) {
            matched = handleInsertAfterInput(depth, input, inputId, currentRows, associations);
        }

        input.setRowKind(inputRowKind);
        return matched;
    }

    private void handleRetractBeforeInput(
            int depth, RowData input, int inputId, RowData[] currentRows, int[] associations)
            throws Exception {

        currentRows[depth] = nullRows.get(depth);
        RowKind originalKind = input.getRowKind();
        input.setRowKind(RowKind.DELETE);

        recursiveMultiJoin(
                depth + 1, input, inputId, currentRows, associations, JoinPhase.EMIT_RESULTS);

        input.setRowKind(originalKind);
    }

    private boolean handleInsertAfterInput(
            int depth, RowData input, int inputId, RowData[] currentRows, int[] associations)
            throws Exception {

        currentRows[depth] = nullRows.get(depth);
        RowKind originalKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT);

        boolean matched =
                recursiveMultiJoin(
                        depth + 1,
                        input,
                        inputId,
                        currentRows,
                        associations,
                        JoinPhase.EMIT_RESULTS);

        input.setRowKind(originalKind);
        return matched;
    }

    private void addRecordToState(RowData input, int inputId) throws Exception {
        if (isRetraction(input)) {
            stateHandlers.get(inputId).retractRecord(input);
        } else {
            stateHandlers.get(inputId).addRecord(input);
        }
    }

    private void initializeCollector() {
        this.collector = new TimestampedCollector<>(output);
    }

    private void initializeNullRows() {
        this.nullRows = new ArrayList<>(inputTypes.size());
        for (InternalTypeInfo<RowData> inputType : inputTypes) {
            this.nullRows.add(new GenericRowData(inputType.toRowType().getFieldCount()));
        }
    }

    private void initializeStateHandlers() {
        if (this.stateHandler.getKeyedStateStore().isPresent()) {
            getRuntimeContext().setKeyedStateStore(this.stateHandler.getKeyedStateStore().get());
        } else {
            throw new RuntimeException(
                    "Keyed state store not found when initializing keyed state store handlers.");
        }

        this.stateHandlers = new ArrayList<>(inputSpecs.size());
        for (int i = 0; i < inputSpecs.size(); i++) {
            JoinRecordStateView stateView;
            String stateName = "multi-join-input-" + i;
            stateView =
                    JoinRecordStateViews.create(
                            getRuntimeContext(),
                            stateName,
                            inputSpecs.get(i),
                            inputTypes.get(i),
                            stateRetentionTime[i]);
            stateHandlers.add(stateView);
            typedInputs.add(createInput(i + 1));
        }
    }

    private void closeConditions() throws Exception {
        if (multiJoinCondition != null) {
            multiJoinCondition.close();
        }
    }

    private Input<RowData> createInput(int idx) {
        return new AbstractInput<>(this, idx) {
            @Override
            public void processElement(StreamRecord<RowData> element) throws Exception {
                ((StreamingMultiJoinOperator) owner).processElement(
                        idx - 1, // simplify id so all logic is 0-based inside the operator
                        element);
            }
        };
    }

    private void emitRow(RowKind rowKind, RowData[] rows) {
        RowData joinedRow = rows[0];
        for (int i = 1; i < rows.length; i++) {
            joinedRow = new JoinedRowData(rowKind, joinedRow, rows[i]);
        }
        collector.collect(joinedRow);
    }

    private boolean isUpsert(RowData row) {
        return row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER;
    }

    private boolean isRetraction(RowData row) {
        return row.getRowKind() == RowKind.DELETE || row.getRowKind() == RowKind.UPDATE_BEFORE;
    }

    private boolean isLeftJoinAtDepth(int depth) {
        return depth > 0 && joinTypes.get(depth) == JoinType.LEFT;
    }

    private boolean isLeftJoinAtLastLevel(int depth) {
        return depth > 0 && joinTypes.get(depth - 1) == JoinType.LEFT;
    }

    private boolean noMatch(int depth, RowData[] currentRows) {
        return !outerJoinConditions[depth].apply(currentRows);
    }

    private void updateAssociationCount(int depth, int[] associations, boolean isUpsert) {
        if (isUpsert) {
            associations[depth - 1]++;
        } else {
            associations[depth - 1]--;
        }
    }

    private boolean shouldIncrementAssociation(JoinPhase phase, RowData input) {
        return phase == JoinPhase.CALCULATE_MATCHES || isUpsert(input);
    }

    private int[] createInitialAssociations() {
        int[] associations = new int[inputSpecs.size()];
        Arrays.fill(associations, 0);
        return associations;
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    public List<Input> getInputs() {
        // Instead of casting (which fails), create a new raw type list
        @SuppressWarnings({"rawtypes"})
        List<Input> rawInputs = new ArrayList<>(typedInputs.size());
        // Add all elements - this works because Input<RowData> is a subtype of the raw Input type
        rawInputs.addAll(typedInputs);
        return rawInputs;
    }
}
