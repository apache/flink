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

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMultiJoin;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

/**
 * A metadata handler for {@link BuiltInMetadata.Predicates}, which adds {@link StreamPhysicalCalc}
 * and {@link StreamPhysicalMultiJoin} to {@link RelMdPredicates}. Other Calcs are left out so that
 * the predicates seen by logical rules, e.g. ReduceExpressionsRule, don't change.
 */
public class FlinkRelMdPredicates extends RelMdPredicates {

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    new FlinkRelMdPredicates(), BuiltInMetadata.Predicates.Handler.class);

    private FlinkRelMdPredicates() {}

    /**
     * Infers predicates for a Calc: the predicates of its input and its condition, restricted to
     * the columns it passes through, like {@link #getPredicates(Project, RelMetadataQuery)}.
     */
    public RelOptPredicateList getPredicates(StreamPhysicalCalc calc, RelMetadataQuery mq) {
        final RelNode input = calc.getInput();
        final RexProgram program = calc.getProgram();
        final List<RexNode> inputPredicates =
                new ArrayList<>(mq.getPulledUpPredicates(input).pulledUpPredicates);
        if (program.getCondition() != null) {
            inputPredicates.addAll(
                    RexUtil.retainDeterministic(
                            RelOptUtil.conjunctions(
                                    program.expandLocalRef(program.getCondition()))));
        }

        final Mapping mapping =
                Mappings.create(
                        MappingType.PARTIAL_FUNCTION,
                        input.getRowType().getFieldCount(),
                        calc.getRowType().getFieldCount());
        final ImmutableBitSet.Builder passedThrough = ImmutableBitSet.builder();
        final List<RexLocalRef> projects = program.getProjectList();
        for (int i = 0; i < projects.size(); i++) {
            final RexNode project = program.expandLocalRef(projects.get(i));
            if (project instanceof RexInputRef) {
                final int index = ((RexInputRef) project).getIndex();
                if (mapping.getTargetOpt(index) < 0) {
                    mapping.set(index, i);
                    passedThrough.set(index);
                }
            }
        }

        final ImmutableBitSet passedThroughColumns = passedThrough.build();
        final RexPermuteInputsShuttle shuttle = new RexPermuteInputsShuttle(mapping, input);
        final List<RexNode> predicates = new ArrayList<>();
        for (RexNode predicate : inputPredicates) {
            if (passedThroughColumns.contains(RelOptUtil.InputFinder.bits(predicate))) {
                predicates.add(predicate.accept(shuttle));
            }
        }
        return RelOptPredicateList.of(calc.getCluster().getRexBuilder(), predicates);
    }

    /**
     * Infers predicates for a multi-join of inner joins only: its condition and the predicates of
     * its inputs, like {@link #getPredicates(Join, RelMetadataQuery)} for an inner join.
     */
    public RelOptPredicateList getPredicates(
            StreamPhysicalMultiJoin multiJoin, RelMetadataQuery mq) {
        if (!multiJoin.getJoinTypes().stream().allMatch(JoinRelType.INNER::equals)) {
            // the condition of an outer join doesn't hold on the rows it pads with nulls
            return RelOptPredicateList.EMPTY;
        }
        final List<RexNode> predicates =
                new ArrayList<>(
                        RexUtil.retainDeterministic(
                                RelOptUtil.conjunctions(multiJoin.getMultiJoinCondition())));
        int offset = 0;
        for (RelNode input : multiJoin.getInputs()) {
            for (RexNode predicate : mq.getPulledUpPredicates(input).pulledUpPredicates) {
                predicates.add(RexUtil.shift(predicate, offset));
            }
            offset += input.getRowType().getFieldCount();
        }
        return RelOptPredicateList.of(multiJoin.getCluster().getRexBuilder(), predicates);
    }
}
