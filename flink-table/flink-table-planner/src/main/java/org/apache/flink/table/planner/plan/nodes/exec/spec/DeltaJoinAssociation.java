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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A class to store the join association between source (stream side) and dest (lookup side) for
 * delta join.
 */
public class DeltaJoinAssociation {

    /** The info of the binary input. */
    private final Map<Integer, BinaryInputInfo> binaryInputInfos;

    /**
     * The join association from one binary input to one binary input.
     *
     * <p>For example, the join tree is like:
     *
     * <pre>{@code
     *    DeltaJoin
     *   /        \
     * #0 A      #1 B
     * }</pre>
     *
     * <p>This map will contain '<0, <1, association>>' and '<1, <0, association>>'.
     */
    private final Map<Integer, Map<Integer, Association>> binary2BinaryJoinAssociation;

    /**
     * The join association from multi binary inputs to one binary input.
     *
     * <p>For example, the join tree is like:
     *
     * <pre>{@code
     *       DeltaJoin
     *      /        \
     *   DeltaJoin  #2 C
     *    /      \
     * #0 A     #1 B
     * }</pre>
     *
     * <p>This map will contain '<[0, 1], <2, association>>'.
     */
    private final Map<Set<Integer>, Map<Integer, Association>>
            compositeBinary2BinaryJoinAssociation;

    private final DeltaJoinTree joinTree;

    private DeltaJoinAssociation(
            Map<Integer, BinaryInputInfo> binaryInputInfos,
            Map<Integer, Map<Integer, Association>> binary2BinaryJoinAssociation,
            Map<Set<Integer>, Map<Integer, Association>> compositeBinary2BinaryJoinAssociation,
            DeltaJoinTree joinTree) {
        this.binaryInputInfos = binaryInputInfos;
        this.binary2BinaryJoinAssociation = binary2BinaryJoinAssociation;
        this.compositeBinary2BinaryJoinAssociation = compositeBinary2BinaryJoinAssociation;
        this.joinTree = joinTree;

        // check binaryInputInfos
        // should begin with 0 to binaryInputInfos.size() - 1
        Preconditions.checkArgument(
                IntStream.range(0, binaryInputInfos.size())
                        .allMatch(binaryInputInfos::containsKey));

        // check binary2BinaryJoinAssociation
        // all source and dest should be in binaryInputTables
        int totalInputBinaryTableSize = binaryInputInfos.size();
        Preconditions.checkArgument(
                binary2BinaryJoinAssociation.keySet().stream()
                        .allMatch(source -> source >= 0 && source < totalInputBinaryTableSize));
        Preconditions.checkArgument(
                binary2BinaryJoinAssociation.values().stream()
                        .flatMap(v -> v.keySet().stream())
                        .allMatch(target -> target >= 0 && target < totalInputBinaryTableSize));
        Preconditions.checkArgument(
                binary2BinaryJoinAssociation.values().stream().allMatch(v -> v.size() == 1),
                "Currently, each binary input can only be directly associated with at most one binary input.");

        // check compositeBinary2BinaryJoinAssociation
        // all source and dest should be in binaryInputTables
        Preconditions.checkArgument(
                compositeBinary2BinaryJoinAssociation.keySet().stream()
                        .allMatch(s -> s.size() > 1));
        Preconditions.checkArgument(
                compositeBinary2BinaryJoinAssociation.keySet().stream()
                        .allMatch(
                                s ->
                                        s.stream()
                                                .allMatch(
                                                        i ->
                                                                i >= 0
                                                                        && i
                                                                                < totalInputBinaryTableSize)));
        Preconditions.checkArgument(
                compositeBinary2BinaryJoinAssociation.values().stream()
                        .flatMap(v -> v.keySet().stream())
                        .allMatch(target -> target >= 0 && target < totalInputBinaryTableSize));
    }

    public static DeltaJoinAssociation create(
            StreamPhysicalTableSourceScan binaryInputTable, @Nullable RexProgram calcOnTable) {
        DeltaJoinTree.BinaryInputNode inputNode =
                new DeltaJoinTree.BinaryInputNode(
                        0,
                        FlinkTypeFactory.toLogicalRowType(binaryInputTable.getRowType()),
                        calcOnTable);
        return new DeltaJoinAssociation(
                Collections.singletonMap(0, BinaryInputInfo.of(binaryInputTable, calcOnTable)),
                new HashMap<>(),
                new HashMap<>(),
                new DeltaJoinTree(inputNode));
    }

    public static DeltaJoinAssociation create(
            FlinkJoinType joinType,
            RexNode joinCondition,
            int[] leftJoinKey,
            int[] rightJoinKey,
            StreamPhysicalTableSourceScan leftBinaryInputTable,
            @Nullable RexProgram calcOnLeftTable,
            StreamPhysicalTableSourceScan rightBinaryInputTable,
            @Nullable RexProgram calcOnRightTable,
            Association left2RightAssociation,
            Association right2LeftAssociation) {
        Map<Integer, BinaryInputInfo> binaryInputTables = new HashMap<>();
        binaryInputTables.put(0, BinaryInputInfo.of(leftBinaryInputTable, calcOnLeftTable));
        binaryInputTables.put(1, BinaryInputInfo.of(rightBinaryInputTable, calcOnRightTable));

        Map<Integer, Map<Integer, Association>> allJoinAssociation = new HashMap<>();
        allJoinAssociation.put(0, new HashMap<>());
        allJoinAssociation.get(0).put(1, left2RightAssociation);
        allJoinAssociation.put(1, new HashMap<>());
        allJoinAssociation.get(1).put(0, right2LeftAssociation);

        DeltaJoinTree.BinaryInputNode leftInputNode =
                new DeltaJoinTree.BinaryInputNode(
                        0,
                        FlinkTypeFactory.toLogicalRowType(leftBinaryInputTable.getRowType()),
                        calcOnLeftTable);
        DeltaJoinTree.BinaryInputNode rightInputNode =
                new DeltaJoinTree.BinaryInputNode(
                        1,
                        FlinkTypeFactory.toLogicalRowType(rightBinaryInputTable.getRowType()),
                        calcOnRightTable);
        DeltaJoinTree.JoinNode root =
                new DeltaJoinTree.JoinNode(
                        joinType,
                        joinCondition,
                        leftJoinKey,
                        rightJoinKey,
                        leftInputNode,
                        rightInputNode,
                        null);

        return new DeltaJoinAssociation(
                binaryInputTables, allJoinAssociation, new HashMap<>(), new DeltaJoinTree(root));
    }

    public DeltaJoinAssociation merge(
            DeltaJoinAssociation other,
            FlinkJoinType joinType,
            RexNode condition,
            int[] leftJoinKey,
            int[] rightJoinKey,
            @Nullable RexProgram calcOnLeftBottomDeltaJoin,
            @Nullable RexProgram calcOnRightBottomDeltaJoin) {
        int shift = this.getBinaryInputCount();
        Map<Integer, BinaryInputInfo> newBinaryInputInfos = new HashMap<>(this.binaryInputInfos);
        newBinaryInputInfos.putAll(
                other.binaryInputInfos.entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey() + shift, Map.Entry::getValue)));

        Map<Integer, Map<Integer, Association>> newAllJoinAssociation =
                new HashMap<>(this.binary2BinaryJoinAssociation);
        for (Map.Entry<Integer, Map<Integer, Association>> entryOnEachSource :
                other.binary2BinaryJoinAssociation.entrySet()) {
            Map<Integer, Association> newAssociation = new HashMap<>();
            for (Map.Entry<Integer, Association> entryOnEachDest :
                    entryOnEachSource.getValue().entrySet()) {
                newAssociation.put(entryOnEachDest.getKey() + shift, entryOnEachDest.getValue());
            }

            newAllJoinAssociation.put(entryOnEachSource.getKey() + shift, newAssociation);
        }

        Map<Set<Integer>, Map<Integer, Association>> newCompositeBinary2BinaryJoinAssociation =
                new HashMap<>(this.compositeBinary2BinaryJoinAssociation);
        for (Map.Entry<Set<Integer>, Map<Integer, Association>> entryOnEachComposite :
                other.compositeBinary2BinaryJoinAssociation.entrySet()) {
            Set<Integer> newComposite =
                    entryOnEachComposite.getKey().stream()
                            .map(i -> i + shift)
                            .collect(Collectors.toSet());
            Map<Integer, Association> newAssociation = new HashMap<>();
            for (Map.Entry<Integer, Association> entryOnEachDest :
                    entryOnEachComposite.getValue().entrySet()) {
                newAssociation.put(entryOnEachDest.getKey() + shift, entryOnEachDest.getValue());
            }
            newCompositeBinary2BinaryJoinAssociation.put(newComposite, newAssociation);
        }

        DeltaJoinTree.Node thisRootNode = this.joinTree.root;
        if (calcOnLeftBottomDeltaJoin != null) {
            Preconditions.checkState(thisRootNode instanceof DeltaJoinTree.JoinNode);
            thisRootNode =
                    ((DeltaJoinTree.JoinNode) thisRootNode)
                            .addCalcOnJoinNode(calcOnLeftBottomDeltaJoin);
        }

        DeltaJoinTree otherTree = other.joinTree.shiftInputIndex(shift);
        DeltaJoinTree.Node otherRootNode = otherTree.root;
        if (calcOnRightBottomDeltaJoin != null) {
            Preconditions.checkState(otherRootNode instanceof DeltaJoinTree.JoinNode);
            otherRootNode =
                    ((DeltaJoinTree.JoinNode) otherRootNode)
                            .addCalcOnJoinNode(calcOnRightBottomDeltaJoin);
        }

        DeltaJoinTree newTree =
                new DeltaJoinTree(
                        new DeltaJoinTree.JoinNode(
                                joinType,
                                condition,
                                leftJoinKey,
                                rightJoinKey,
                                thisRootNode,
                                otherRootNode,
                                null));

        return new DeltaJoinAssociation(
                newBinaryInputInfos,
                newAllJoinAssociation,
                newCompositeBinary2BinaryJoinAssociation,
                newTree);
    }

    public int getBinaryInputCount() {
        return binaryInputInfos.size();
    }

    public DeltaJoinTree getJoinTree() {
        return joinTree;
    }

    public void addJoinAssociation(int sourceOrdinal, int destOrdinal, Association association) {
        binary2BinaryJoinAssociation
                .computeIfAbsent(sourceOrdinal, k -> new HashMap<>())
                .put(destOrdinal, association);
    }

    public void addJoinAssociation(
            Set<Integer> sourceOrdinals, int destOrdinal, Association association) {
        if (sourceOrdinals.size() == 1) {
            addJoinAssociation(sourceOrdinals.iterator().next(), destOrdinal, association);
            return;
        }
        compositeBinary2BinaryJoinAssociation
                .computeIfAbsent(sourceOrdinals, k -> new HashMap<>())
                .put(destOrdinal, association);
    }

    public Map<Integer, Association> getDestOrdinalAndAssociations(int sourceOrdinal) {
        Map<Integer, Association> destAssociation = binary2BinaryJoinAssociation.get(sourceOrdinal);
        Preconditions.checkArgument(
                destAssociation != null && !destAssociation.isEmpty(),
                String.format("There is no join association for ord %d", sourceOrdinal));
        return destAssociation;
    }

    public Map<Set<Integer>, Map<Integer, Association>> getCompositeBinary2BinaryJoinAssociation() {
        return compositeBinary2BinaryJoinAssociation;
    }

    public List<Integer> getAllBinaryInputOrdinals() {
        return getAllBinaryInputOrdinalsWithOffset(0);
    }

    public List<Integer> getAllBinaryInputOrdinalsWithOffset(int offset) {
        return IntStream.range(0, binaryInputInfos.size())
                .mapToObj(i -> i + offset)
                .collect(Collectors.toList());
    }

    public List<TemporalTableSourceSpec> getAllBinaryInputTableSourceSpecs() {
        return IntStream.range(0, binaryInputInfos.size())
                .mapToObj(
                        i ->
                                new TemporalTableSourceSpec(
                                        binaryInputInfos.get(i).tableScan.getTable()))
                .collect(Collectors.toList());
    }

    /**
     * Returns a concise summary of all source-to-target association pairs in both {@link
     * #binary2BinaryJoinAssociation} and {@link #compositeBinary2BinaryJoinAssociation}.
     *
     * <p>Example output:
     *
     * <pre>{@code
     * binary: {0->1, 1->0}; composite: {[0, 1]->2}
     * }</pre>
     */
    public String getAssociationSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("binary: {");
        sb.append(
                binary2BinaryJoinAssociation.entrySet().stream()
                        .sorted(Comparator.comparingInt(Map.Entry::getKey))
                        .flatMap(
                                e ->
                                        e.getValue().keySet().stream()
                                                .sorted()
                                                .map(t -> e.getKey() + "->" + t))
                        .collect(Collectors.joining(", ")));
        sb.append("}; composite: {");
        sb.append(
                compositeBinary2BinaryJoinAssociation.entrySet().stream()
                        .sorted(Comparator.comparing(e -> e.getKey().toString()))
                        .flatMap(
                                e -> {
                                    String sources =
                                            e.getKey().stream()
                                                    .sorted()
                                                    .map(String::valueOf)
                                                    .collect(Collectors.joining(", ", "[", "]"));
                                    return e.getValue().keySet().stream()
                                            .sorted()
                                            .map(t -> sources + "->" + t);
                                })
                        .collect(Collectors.joining(", ")));
        sb.append("}");
        return sb.toString();
    }

    public BinaryInputInfo getBinaryInputInfo(int binaryInputOrdinal) {
        BinaryInputInfo binaryInputInfo = binaryInputInfos.get(binaryInputOrdinal);
        Preconditions.checkArgument(
                binaryInputInfo != null, "There is no binary input for ord " + binaryInputOrdinal);
        return binaryInputInfo;
    }

    public TableSourceTable getBinaryInputTable(int binaryInputOrdinal) {
        return getBinaryInputInfo(binaryInputOrdinal).tableScan.tableSourceTable();
    }

    /**
     * Represents the join association between a source binary input and a destination binary input
     * in a delta join. It contains the join type and the {@link DeltaJoinSpec} that describes how
     * the source side looks up the destination side.
     */
    public static class Association {

        // join type for source lookup dest table
        public final FlinkJoinType joinType;

        // source lookup dest table
        public final DeltaJoinSpec deltaJoinSpec;

        private Association(FlinkJoinType joinType, DeltaJoinSpec deltaJoinSpec) {
            this.joinType = joinType;
            this.deltaJoinSpec = deltaJoinSpec;
        }

        public static Association of(FlinkJoinType joinType, DeltaJoinSpec deltaJoinSpec) {
            return new Association(joinType, deltaJoinSpec);
        }
    }

    /**
     * Stores the information of a binary input in a delta join, including the physical table source
     * scan and an optional {@link RexProgram} (calc) applied on top of the table scan.
     */
    public static class BinaryInputInfo {

        public final StreamPhysicalTableSourceScan tableScan;
        public final @Nullable RexProgram calcOnTable;

        private BinaryInputInfo(
                StreamPhysicalTableSourceScan tableScan, @Nullable RexProgram calcOnTable) {
            this.tableScan = tableScan;
            this.calcOnTable = calcOnTable;
        }

        public static BinaryInputInfo of(
                StreamPhysicalTableSourceScan tableScan, @Nullable RexProgram calcOnTable) {
            return new BinaryInputInfo(tableScan, calcOnTable);
        }

        public RelDataType getRelDataType() {
            if (calcOnTable == null) {
                return tableScan.getRowType();
            }
            return calcOnTable.getOutputRowType();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BinaryInputInfo inputInfo = (BinaryInputInfo) o;
            return Objects.equals(tableScan, inputInfo.tableScan)
                    && Objects.equals(calcOnTable, inputInfo.calcOnTable);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableScan, calcOnTable);
        }
    }
}
