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

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeltaJoin;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.RelExplainUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/** Stream physical RelNode for delta join. */
public class StreamPhysicalDeltaJoin extends BiRel implements StreamPhysicalRel, Hintable {

    private final FlinkJoinType joinType;

    private final RexNode originalJoinCondition;

    private final com.google.common.collect.ImmutableList<RelHint> hints;

    private final RelDataType rowType;

    // treat right side as lookup table
    private final DeltaJoinSpec lookupRightTableJoinSpec;

    // treat left side as lookup table
    private final DeltaJoinSpec lookupLeftTableJoinSpec;

    public StreamPhysicalDeltaJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode left,
            RelNode right,
            FlinkJoinType joinType,
            RexNode originalJoinCondition,
            DeltaJoinSpec lookupRightTableJoinSpec,
            DeltaJoinSpec lookupLeftTableJoinSpec,
            RelDataType rowType) {
        super(cluster, traitSet, left, right);
        this.hints = com.google.common.collect.ImmutableList.copyOf(hints);
        this.joinType = joinType;
        this.originalJoinCondition = originalJoinCondition;
        this.lookupRightTableJoinSpec = lookupRightTableJoinSpec;
        this.lookupLeftTableJoinSpec = lookupLeftTableJoinSpec;
        this.rowType = rowType;
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        TableConfig config = unwrapTableConfig(this);
        FunctionCallUtil.AsyncOptions asyncLookupOptions =
                new FunctionCallUtil.AsyncOptions(
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT)
                                .toMillis(),
                        // Currently DeltaJoin only supports ordered processing based on join key.
                        // However, it may be possible to support unordered processing in certain
                        // scenarios to enhance throughput as much as possible.
                        true,
                        AsyncDataStream.OutputMode.ORDERED);

        JoinInfo joinInfo = JoinInfo.of(left, right, originalJoinCondition);

        return new StreamExecDeltaJoin(
                config,
                joinType,
                joinInfo.leftKeys.toIntArray(),
                lookupRightTableJoinSpec,
                joinInfo.rightKeys.toIntArray(),
                lookupLeftTableJoinSpec,
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(rowType),
                getRelDetailedDescription(),
                asyncLookupOptions);
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 2;
        return new StreamPhysicalDeltaJoin(
                getCluster(),
                traitSet,
                hints,
                inputs.get(0),
                inputs.get(1),
                joinType,
                originalJoinCondition,
                lookupRightTableJoinSpec,
                lookupLeftTableJoinSpec,
                rowType);
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public com.google.common.collect.ImmutableList<RelHint> getHints() {
        return hints;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("joinType", joinType.toString())
                .item(
                        "where",
                        getExpressionString(
                                originalJoinCondition,
                                JavaScalaConversionUtil.toScala(this.getRowType().getFieldNames())
                                        .toList(),
                                JavaScalaConversionUtil.toScala(Optional.empty()),
                                RelExplainUtil.preferExpressionFormat(pw),
                                RelExplainUtil.preferExpressionDetail(pw)))
                .item("select", String.join(", ", rowType.getFieldNames()));
    }
}
