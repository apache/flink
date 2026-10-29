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

package org.apache.flink.table.planner.plan.metadata;

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMultiJoin;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** A metadata handler for {@link FlinkMetadata.EquiJoinColumnPairs}. */
public class FlinkRelMdEquiJoinColumnPairs
        implements MetadataHandler<FlinkMetadata.EquiJoinColumnPairs> {
    static final FlinkRelMdEquiJoinColumnPairs INSTANCE = new FlinkRelMdEquiJoinColumnPairs();

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    FlinkMetadata.EquiJoinColumnPairs.METHOD, INSTANCE);

    // ~ Constructors -----------------------------------------------------------

    private FlinkRelMdEquiJoinColumnPairs() {}

    // ~ Methods ----------------------------------------------------------------

    public MetadataDef<FlinkMetadata.EquiJoinColumnPairs> getDef() {
        return FlinkMetadata.EquiJoinColumnPairs.DEF;
    }

    public List<EquiJoinColumnPair> getEquiJoinColumnPairs(Join join, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        JoinRelType joinType = join.getJoinType();
        if (!joinType.projectsRight()) {
            // SEMI/ANTI output only the left side, so the cross-side pairs are not in the output
            return fmq.getEquiJoinColumnPairs(join.getLeft());
        }
        boolean outerJoin = joinType.generatesNullsOnLeft() || joinType.generatesNullsOnRight();
        int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        List<EquiJoinColumnPair> pairs = new ArrayList<>();
        for (IntPair pair : join.analyzeCondition().pairs()) {
            pairs.add(EquiJoinColumnPair.of(pair.source, pair.target + leftFieldCount, outerJoin));
        }
        pairs.addAll(fmq.getEquiJoinColumnPairs(join.getLeft()));
        pairs.addAll(shift(fmq.getEquiJoinColumnPairs(join.getRight()), leftFieldCount));
        return List.copyOf(pairs);
    }

    public List<EquiJoinColumnPair> getEquiJoinColumnPairs(
            StreamPhysicalMultiJoin multiJoin, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        List<EquiJoinColumnPair> pairs = new ArrayList<>();
        // A left join's condition may equate two columns of the preserved side, which then differ
        // in the rows padded with nulls, so the condition is only used if all joins are inner.
        if (multiJoin.getJoinTypes().stream().allMatch(type -> type == JoinRelType.INNER)) {
            for (RexNode conjunction : RelOptUtil.conjunctions(multiJoin.getMultiJoinCondition())) {
                if (conjunction.getKind() == SqlKind.EQUALS) {
                    List<RexNode> operands = ((RexCall) conjunction).getOperands();
                    if (operands.get(0) instanceof RexInputRef
                            && operands.get(1) instanceof RexInputRef) {
                        pairs.add(
                                EquiJoinColumnPair.of(
                                        ((RexInputRef) operands.get(0)).getIndex(),
                                        ((RexInputRef) operands.get(1)).getIndex(),
                                        false));
                    }
                }
            }
        }
        int offset = 0;
        for (RelNode input : multiJoin.getInputs()) {
            pairs.addAll(shift(fmq.getEquiJoinColumnPairs(input), offset));
            offset += input.getRowType().getFieldCount();
        }
        return List.copyOf(pairs);
    }

    public List<EquiJoinColumnPair> getEquiJoinColumnPairs(Calc rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        List<RexNode> projects =
                rel.getProgram().getProjectList().stream()
                        .map(localRef -> rel.getProgram().expandLocalRef(localRef))
                        .collect(Collectors.toList());
        return projectPairs(fmq.getEquiJoinColumnPairs(rel.getInput()), projects);
    }

    public List<EquiJoinColumnPair> getEquiJoinColumnPairs(Project rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return projectPairs(fmq.getEquiJoinColumnPairs(rel.getInput()), rel.getProjects());
    }

    public List<EquiJoinColumnPair> getEquiJoinColumnPairs(Exchange rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return fmq.getEquiJoinColumnPairs(rel.getInput());
    }

    public List<EquiJoinColumnPair> getEquiJoinColumnPairs(HepRelVertex rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return fmq.getEquiJoinColumnPairs(rel.getCurrentRel());
    }

    public List<EquiJoinColumnPair> getEquiJoinColumnPairs(RelSubset rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return fmq.getEquiJoinColumnPairs(Util.first(rel.getBest(), rel.getOriginal()));
    }

    public List<EquiJoinColumnPair> getEquiJoinColumnPairs(RelNode rel, RelMetadataQuery mq) {
        // Catch-all rule when none of the others apply.
        // More nodes can be supported later, such as Filter, Aggregate or Rank.
        return List.of();
    }

    private static List<EquiJoinColumnPair> shift(List<EquiJoinColumnPair> pairs, int offset) {
        return pairs.stream()
                .map(pair -> pair.withColumns(pair.getSource() + offset, pair.getTarget() + offset))
                .collect(Collectors.toUnmodifiableList());
    }

    /** Maps the pairs to every output position where the projection passes both columns through. */
    private static List<EquiJoinColumnPair> projectPairs(
            List<EquiJoinColumnPair> pairs, List<RexNode> projects) {
        Map<Integer, List<Integer>> mapInToOutPos = new HashMap<>();
        for (int i = 0; i < projects.size(); i++) {
            RexNode projExpr = projects.get(i);
            if (projExpr instanceof RexInputRef) {
                mapInToOutPos
                        .computeIfAbsent(
                                ((RexInputRef) projExpr).getIndex(), k -> new ArrayList<>())
                        .add(i);
            }
        }
        List<EquiJoinColumnPair> result = new ArrayList<>();
        for (EquiJoinColumnPair pair : pairs) {
            for (int source : mapInToOutPos.getOrDefault(pair.getSource(), List.of())) {
                for (int target : mapInToOutPos.getOrDefault(pair.getTarget(), List.of())) {
                    result.add(pair.withColumns(source, target));
                }
            }
        }
        return List.copyOf(result);
    }
}
