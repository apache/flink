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

package org.apache.flink.table.runtime.operators.join.deltajoin;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.lookup.CalcCollectionCollector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A shared buffer used in delta join to manage data for all binary inputs (source tables). It
 * maintains one slot per binary input, indexed by input ordinal.
 *
 * <p>The buffer provides the following capabilities:
 *
 * <ol>
 *   <li><b>Store data for each binary input</b>: data is placed into slots either by the stream
 *       side via {@link #prepareDataInBuffer} or by lookup handlers via {@link #setRowData} after a
 *       lookup completes.
 *   <li><b>Compute and save data on demand</b>: when a {@link LookupHandlerBase} needs source data
 *       that spans multiple binary inputs (i.e., the result of a sub-join in the {@link
 *       DeltaJoinRuntimeTree}), the internal {@link DataProvider} joins the individual slot data
 *       on-the-fly using the join tree, saving the result, and returns it.
 * </ol>
 *
 * <p>For example, given the following job topology:
 *
 * <pre>{@code
 *         DeltaJoin2
 *       /              \
 *  DeltaJoin1       #2 C
 *    /      \
 * #0 A    #1 B
 * }</pre>
 *
 * <p>The buffer has 3 slots (slot 0 for A, slot 1 for B, slot 2 for C). When processing data from
 * the left stream side ({@code DeltaJoin1}) in {@code DeltaJoin2}:
 *
 * <ol>
 *   <li>The joined result of A and B from the upstream delta join is placed into slots 0 and 1 via
 *       {@link #prepareDataInBuffer}.
 *   <li>The lookup handler then looks up C, and the result is placed into slot 2 via {@link
 *       #setRowData}.
 * </ol>
 *
 * <p>Note:
 *
 * <ol>
 *   <li>This buffer is not thread safe.
 *   <li>The data of each slot can be set only once per processing cycle. Call {@link #reset()}
 *       before the next cycle.
 * </ol>
 */
public class MultiInputRowDataBuffer {

    private final int inputCnt;
    private final Collection<RowData>[] subRowsPerInput;
    private final DeltaJoinRuntimeTree joinTree;
    // <input ordinals>
    private final Set<Set<Integer>> allDrivenInputsWhenLookup;
    private final DataProvider dataProvider;

    public MultiInputRowDataBuffer(
            int[] eachInputFieldSize,
            DeltaJoinRuntimeTree joinTree,
            Set<Set<Integer>> allDrivenInputsWhenLookup) {
        this.inputCnt = eachInputFieldSize.length;
        this.subRowsPerInput = new Collection[inputCnt];

        this.joinTree = joinTree;
        this.allDrivenInputsWhenLookup = allDrivenInputsWhenLookup;
        this.dataProvider = new DataProvider();
    }

    public void open(OpenContext openContext, RuntimeContext runtimeContext) throws Exception {
        joinTree.open(runtimeContext, openContext);
    }

    public void close() throws Exception {
        joinTree.close();
    }

    public Collection<RowData> getData(int[] inputOrdinals) throws Exception {
        return dataProvider.getData(inputOrdinals);
    }

    public void setRowData(Collection<RowData> data, int inputIdx) {
        validateInputIsNotSet(inputIdx);
        this.subRowsPerInput[inputIdx] = data;
    }

    public void prepareDataInBuffer(RowData data, Set<Integer> inputIdxes) {
        // set a fake data set to tag the input is ready
        inputIdxes.forEach(input -> setRowData(Collections.emptySet(), input));
        dataProvider.providedData.put(inputIdxes, Collections.singleton(data));
    }

    public void reset() {
        for (Collection<RowData> rows : subRowsPerInput) {
            if (rows != null) {
                rows.clear();
            }
        }
        Arrays.fill(subRowsPerInput, null);
        dataProvider.reset();
    }

    public void validateInputIsSet(int inputIdx) {
        Preconditions.checkArgument(inputIdx >= 0 && inputIdx < inputCnt);
        Preconditions.checkArgument(
                subRowsPerInput[inputIdx] != null, "This input row has not been set: " + inputIdx);
    }

    @VisibleForTesting
    public DeltaJoinRuntimeTree getJoinTree() {
        return joinTree;
    }

    private void validateInputIsNotSet(int inputIdx) {
        Preconditions.checkArgument(inputIdx >= 0 && inputIdx < inputCnt);
        Preconditions.checkArgument(
                subRowsPerInput[inputIdx] == null, "This input row has been set");
    }

    private Collection<RowData> getBinaryInputRowData(int inputIdx) {
        validateInputIsSet(inputIdx);
        return this.subRowsPerInput[inputIdx];
    }

    /**
     * This {@link DataProvider} is used to directly get or compute source data on the spot for each
     * node on {@link DeltaJoinRuntimeTree}.
     *
     * <p>For {@link DeltaJoinRuntimeTree.BinaryInputNode}, the data is retrieved directly from the
     * node. For {@link DeltaJoinRuntimeTree.JoinNode}, if it has been computed previously, the data
     * is retrieved from the {@link #providedData}; if it has not been computed before, it is
     * calculated on-site.
     */
    private class DataProvider {

        private final Map<Set<Integer>, Collection<RowData>> providedData = new HashMap<>();

        public void reset() {
            providedData.clear();
        }

        /**
         * Get the data for the given input ordinals.
         *
         * <p>Each node on {@link DeltaJoinRuntimeTree} represents a collection of different input
         * ordinals. The method first determines which node's data to retrieve using {@code
         * caresInputOrdinals}, and then retrieves or calculates the data from that node.
         */
        public Collection<RowData> getData(int[] caresInputOrdinals) throws Exception {
            Preconditions.checkArgument(caresInputOrdinals.length > 0);

            DeltaJoinRuntimeTree.Node node =
                    findNodeToGetData(
                            joinTree.root,
                            Arrays.stream(caresInputOrdinals).boxed().collect(Collectors.toSet()));
            return visit(node);
        }

        private DeltaJoinRuntimeTree.Node findNodeToGetData(
                DeltaJoinRuntimeTree.Node node, Set<Integer> caresInputOrdinals) {
            Set<Integer> allInputOrdinalsInThisSubTree = node.getAllInputOrdinals();
            Preconditions.checkArgument(
                    allInputOrdinalsInThisSubTree.containsAll(caresInputOrdinals));

            if (allInputOrdinalsInThisSubTree.equals(caresInputOrdinals)) {
                return node;
            }

            Preconditions.checkArgument(node instanceof DeltaJoinRuntimeTree.JoinNode);
            DeltaJoinRuntimeTree.JoinNode joinNode = (DeltaJoinRuntimeTree.JoinNode) node;
            if (joinNode.left.getAllInputOrdinals().containsAll(caresInputOrdinals)) {
                return findNodeToGetData(joinNode.left, caresInputOrdinals);
            }

            Preconditions.checkArgument(
                    joinNode.right.getAllInputOrdinals().containsAll(caresInputOrdinals));
            return findNodeToGetData(joinNode.right, caresInputOrdinals);
        }

        private Collection<RowData> visit(DeltaJoinRuntimeTree.Node node) throws Exception {
            Set<Integer> inputOrdinals = node.getAllInputOrdinals();
            if (providedData.containsKey(inputOrdinals)) {
                return providedData.get(inputOrdinals);
            }

            if (node instanceof DeltaJoinRuntimeTree.BinaryInputNode) {
                return visit((DeltaJoinRuntimeTree.BinaryInputNode) node);
            }
            if (node instanceof DeltaJoinRuntimeTree.JoinNode) {
                return visit((DeltaJoinRuntimeTree.JoinNode) node);
            }
            throw new IllegalStateException(
                    "Unknown node type: " + node.getClass().getSimpleName());
        }

        private Collection<RowData> visit(DeltaJoinRuntimeTree.BinaryInputNode node) {
            Collection<RowData> results = getBinaryInputRowData(node.inputOrdinal);
            // the data from binary input is no need to be processed again by calc
            providedData.put(Collections.singleton(node.inputOrdinal), results);
            return results;
        }

        private Collection<RowData> visit(DeltaJoinRuntimeTree.JoinNode node) throws Exception {
            boolean lookupRight;
            if (allDrivenInputsWhenLookup.contains(node.left.getAllInputOrdinals())) {
                lookupRight = true;
            } else if (allDrivenInputsWhenLookup.contains(node.right.getAllInputOrdinals())) {
                lookupRight = false;
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Can't find driven side inputs %s in Join Node. "
                                        + "All inputs in left are %s, and those in right are %s",
                                allDrivenInputsWhenLookup,
                                node.left.getAllInputOrdinals(),
                                node.right.getAllInputOrdinals()));
            }

            Preconditions.checkArgument(
                    node.joinType == FlinkJoinType.INNER, "Only INNER JOIN is supported");

            Collection<RowData> leftData = visit(node.left);
            Collection<RowData> rightData = visit(node.right);

            if (leftData.isEmpty() && rightData.isEmpty()) {
                return Collections.emptySet();
            }

            Collection<RowData> inputData = lookupRight ? leftData : rightData;
            Collection<RowData> lookupData = lookupRight ? rightData : leftData;

            FilterCondition condition = node.joinCondition;

            Collection<RowData> results = new ArrayList<>();
            for (RowData input : inputData) {
                for (RowData lookup : lookupData) {
                    JoinedRowData joinedRowData =
                            lookupRight
                                    ? new JoinedRowData(input.getRowKind(), input, lookup)
                                    : new JoinedRowData(input.getRowKind(), lookup, input);
                    if (!condition.apply(FilterCondition.Context.INVALID_CONTEXT, joinedRowData)) {
                        continue;
                    }
                    results.add(joinedRowData);
                }
            }

            if (node.calc != null) {
                CalcCollectionCollector calcCollector =
                        new CalcCollectionCollector(node.rowDataSerializerPassThroughCalc);
                calcCollector.reset();
                for (RowData row : results) {
                    node.calc.flatMap(row, calcCollector);
                }
                results = calcCollector.getCollection();
            }

            Set<Integer> allInvolvingInputs = node.getAllInputOrdinals();
            providedData.put(allInvolvingInputs, results);
            return results;
        }
    }
}
