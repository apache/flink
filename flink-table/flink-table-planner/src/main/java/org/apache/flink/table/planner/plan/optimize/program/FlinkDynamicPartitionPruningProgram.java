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

package org.apache.flink.table.planner.plan.optimize.program;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.utils.DefaultRelShuttle;
import org.apache.flink.table.planner.utils.DynamicPartitionPruningUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Planner program that tries to do partition prune in the execution phase, which can translate a
 * {@link BatchPhysicalTableSourceScan} to a {@link BatchPhysicalDynamicFilteringTableSourceScan}
 * whose source is a partition source. The {@link
 * OptimizerConfigOptions#TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED} need to be true.
 *
 * <p>Suppose we have the original physical plan:
 *
 * <pre>{@code
 * LogicalProject(...)
 * HashJoin(joinType=[InnerJoin], where=[=(fact_partition_key, dim_key)], select=[xxx])
 *  * :- TableSourceScan(table=[[fact]], fields=[xxx, fact_partition_key],) # Is a partition table.
 *  * +- Exchange(distribution=[broadcast])
 *  *    +- Calc(select=[xxx], where=[<(xxx, xxx)]) # Need have an arbitrary filter condition.
 *  *       +- TableSourceScan(table=[[dim, filter=[]]], fields=[xxx, dim_key])
 * }</pre>
 *
 * <p>This physical plan will be rewritten to:
 *
 * <pre>{@code
 * HashJoin(joinType=[InnerJoin], where=[=(fact_partition_key, dim_key)], select=[xxx])
 * :- DynamicFilteringTableSourceScan(table=[[fact]], fields=[xxx, fact_partition_key]) # Is a partition table.
 * :  +- DynamicFilteringDataCollector(fields=[dim_key])
 * :     +- Calc(select=[xxx], where=[<(xxx, xxx)])
 * :        +- TableSourceScan(table=[[dim, filter=[]]], fields=[xxx, dim_key])
 * +- Exchange(distribution=[broadcast])
 *    +- Calc(select=[xxx], where=[<(xxx, xxx)]) # Need have an arbitrary filter condition.
 *       +- TableSourceScan(table=[[dim, filter=[]]], fields=[xxx, dim_key])
 * }</pre>
 *
 * <p>Note: We use a {@link FlinkOptimizeProgram} instead of a {@link
 * org.apache.calcite.plan.RelRule} here because the {@link org.apache.calcite.plan.hep.HepPlanner}
 * doesn't support matching a partially determined pattern or dynamically replacing the inputs of
 * matched nodes. Once we improve the {@link org.apache.calcite.plan.hep.HepPlanner}, then class can
 * be converted to {@link org.apache.calcite.plan.RelRule}.
 */
public class FlinkDynamicPartitionPruningProgram
        implements FlinkOptimizeProgram<BatchOptimizeContext> {

    @Override
    public RelNode optimize(RelNode root, BatchOptimizeContext context) {
        if (!ShortcutUtils.unwrapContext(root)
                .getTableConfig()
                .get(OptimizerConfigOptions.TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED)) {
            return root;
        }
        DefaultRelShuttle shuttle =
                new DefaultRelShuttle() {
                    @Override
                    public RelNode visit(RelNode rel) {
                        if (!(rel instanceof Join)
                                || !DynamicPartitionPruningUtils.isSuitableJoin((Join) rel)) {
                            List<RelNode> newInputs = new ArrayList<>();
                            for (RelNode input : rel.getInputs()) {
                                RelNode newInput = input.accept(this);
                                newInputs.add(newInput);
                            }
                            return rel.copy(rel.getTraitSet(), newInputs);
                        }
                        Join join = (Join) rel;
                        JoinInfo joinInfo = join.analyzeCondition();
                        RelNode leftSide = join.getLeft();
                        RelNode rightSide = join.getRight();
                        Join newJoin = join;
                        boolean changed = false;
                        if (DynamicPartitionPruningUtils.isDppDimSide(leftSide)) {
                            if (join.getJoinType() != JoinRelType.RIGHT) {
                                Tuple2<Boolean, RelNode> relTuple =
                                        DynamicPartitionPruningUtils
                                                .canConvertAndConvertDppFactSide(
                                                        rightSide,
                                                        joinInfo.rightKeys,
                                                        leftSide,
                                                        joinInfo.leftKeys);
                                changed = relTuple.f0;
                                newJoin =
                                        join.copy(
                                                join.getTraitSet(),
                                                Arrays.asList(leftSide, relTuple.f1.accept(this)));
                            }
                        } else if (DynamicPartitionPruningUtils.isDppDimSide(rightSide)) {
                            if (join.getJoinType() != JoinRelType.LEFT) {
                                Tuple2<Boolean, RelNode> relTuple =
                                        DynamicPartitionPruningUtils
                                                .canConvertAndConvertDppFactSide(
                                                        leftSide,
                                                        joinInfo.leftKeys,
                                                        rightSide,
                                                        joinInfo.rightKeys);
                                changed = relTuple.f0;
                                newJoin =
                                        join.copy(
                                                join.getTraitSet(),
                                                Arrays.asList(relTuple.f1.accept(this), rightSide));
                            }
                        }

                        if (changed) {
                            return newJoin;
                        } else {
                            return newJoin.copy(
                                    newJoin.getTraitSet(),
                                    Arrays.asList(
                                            newJoin.getLeft().accept(this),
                                            newJoin.getRight().accept(this)));
                        }
                    }
                };
        return shuttle.visit(root);
    }
}
