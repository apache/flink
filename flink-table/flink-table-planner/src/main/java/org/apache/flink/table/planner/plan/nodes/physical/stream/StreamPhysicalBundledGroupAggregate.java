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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.PartialFinalType;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecBundledGroupAggregate;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.RelDescriptionWriterImpl;
import org.apache.flink.table.planner.plan.utils.RelExplainUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/**
 * Stream physical RelNode for {@link org.apache.flink.table.functions.AggregateBundledFunction}.
 */
public class StreamPhysicalBundledGroupAggregate extends StreamPhysicalGroupAggregateBase {
    private final RelOptCluster cluster;
    private final RelDataType outputRowType;
    private final int[] grouping;
    private final List<AggregateCall> aggCalls;
    private final PartialFinalType partialFinalType;
    private final List<RelHint> hints;

    public StreamPhysicalBundledGroupAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode inputRel,
            RelDataType outputRowType,
            int[] grouping,
            List<AggregateCall> aggCalls,
            PartialFinalType partialFinalType,
            List<RelHint> hints) {
        super(
                cluster,
                traitSet,
                inputRel,
                grouping,
                JavaScalaConversionUtil.toScala(aggCalls),
                hints);
        this.cluster = cluster;
        this.outputRowType = outputRowType;
        this.grouping = grouping;
        this.aggCalls = aggCalls;
        this.partialFinalType = partialFinalType;
        this.hints = hints;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelDataType inputRowType = getInput().getRowType();
        AggregateInfoList aggInfoList =
                AggregateUtil.deriveAggregateInfoList(
                        this, grouping.length, JavaScalaConversionUtil.toScala(aggCalls));
        return super.explainTerms(pw)
                .itemIf(
                        "groupBy",
                        RelExplainUtil.fieldToString(grouping, inputRowType),
                        grouping.length > 0)
                .itemIf(
                        "partialFinalType",
                        partialFinalType,
                        partialFinalType != PartialFinalType.NONE)
                .item(
                        "select",
                        RelExplainUtil.streamGroupAggregationToString(
                                inputRowType,
                                getRowType(),
                                aggInfoList,
                                grouping,
                                JavaScalaConversionUtil.toScala(Optional.empty()),
                                false,
                                false));
    }

    @Override
    public RelDataType deriveRowType() {
        return outputRowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamPhysicalBundledGroupAggregate(
                cluster,
                traitSet,
                inputs.get(0),
                outputRowType,
                grouping,
                aggCalls,
                partialFinalType,
                hints);
    }

    @Override
    public String getRelDetailedDescription() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        RelDescriptionWriterImpl relWriter = new RelDescriptionWriterImpl(pw);
        this.explain(relWriter);
        return sw.toString();
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        boolean[] aggCallNeedRetractions =
                AggregateUtil.deriveAggCallNeedRetractions(
                        this, grouping.length, JavaScalaConversionUtil.toScala(aggCalls));
        boolean generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this);
        boolean needRetraction = !ChangelogPlanUtils.inputInsertOnly(this);
        TableConfig tableConfig = unwrapTableConfig(this);
        setJobMetadata(tableConfig);
        return new StreamExecBundledGroupAggregate(
                tableConfig,
                grouping,
                aggCalls.toArray(new AggregateCall[0]),
                aggCallNeedRetractions,
                generateUpdateBefore,
                needRetraction,
                null,
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }

    private void setJobMetadata(TableConfig tableConfig) {
        RowType inputRowType =
                unwrapTypeFactory(getInput()).toLogicalRowType(getInput().getRowType());
        LogicalType[] inputFieldTypes = InternalTypeInfo.of(inputRowType).toRowFieldTypes();
        LogicalType[] keyFieldTypes = new LogicalType[grouping.length];
        for (int i = 0; i < grouping.length; ++i) {
            keyFieldTypes[i] = inputFieldTypes[grouping[i]];
        }
        RowType keyType = RowType.of(keyFieldTypes);
        tableConfig.addJobParameter("keyType", keyType.asSerializableString());
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }
}
