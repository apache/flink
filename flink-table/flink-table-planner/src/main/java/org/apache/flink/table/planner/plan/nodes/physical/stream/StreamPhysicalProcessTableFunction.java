/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecProcessTableFunction;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.SystemTypeInference;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;
import static org.apache.flink.table.types.inference.SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_ON_TIME_OFFSET;

/**
 * {@link StreamPhysicalRel} node for {@link ProcessTableFunction}.
 *
 * <p>A process table function (PTF) maps zero, one, or multiple tables to zero, one, or multiple
 * rows. PTFs enable implementing user-defined operators that can be as feature-rich as built-in
 * operations. PTFs have access to Flink's managed state, event-time and timer services, underlying
 * table changelogs, and can take multiple partitioned tables to produce a new table.
 */
public class StreamPhysicalProcessTableFunction extends AbstractRelNode
        implements StreamPhysicalRel {

    private final FlinkLogicalTableFunctionScan scan;
    private final @Nullable String uid;

    private List<RelNode> inputs;

    public StreamPhysicalProcessTableFunction(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            FlinkLogicalTableFunctionScan scan,
            RelDataType rowType) {
        super(cluster, traitSet);
        this.inputs = inputs;
        this.rowType = rowType;
        this.scan = scan;
        this.uid = deriveUniqueIdentifier(scan);
    }

    public StreamPhysicalProcessTableFunction(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            FlinkLogicalTableFunctionScan scan,
            RelDataType rowType) {
        this(cluster, traitSet, List.of(input), scan, rowType);
    }

    public RexCall getCall() {
        return (RexCall) scan.getCall();
    }

    @Override
    public boolean requireWatermark() {
        // Even if there is no time attribute in the inputs, PTFs can work with event-time by taking
        // the watermark value as timestamp.
        return true;
    }

    @Override
    public List<RelNode> getInputs() {
        return inputs;
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        final List<RelNode> newInputs = new ArrayList<>(inputs);
        newInputs.set(ordinalInParent, p);
        inputs = List.copyOf(newInputs);
        recomputeDigest();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamPhysicalProcessTableFunction(
                getCluster(), traitSet, inputs, scan, getRowType());
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final double elementRate = 100.0d * getInputs().size();
        return planner.getCostFactory().makeCost(elementRate, elementRate, 0);
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        final List<ChangelogMode> inputChangelogModes =
                getInputs().stream()
                        .map(StreamPhysicalRel.class::cast)
                        .map(ChangelogPlanUtils::getChangelogMode)
                        .map(JavaScalaConversionUtil::toJava)
                        .map(optional -> optional.orElseThrow(IllegalStateException::new))
                        .collect(Collectors.toList());
        final RexCall call = (RexCall) scan.getCall();
        verifyTimeAttributes(getInputs(), call);
        return new StreamExecProcessTableFunction(
                unwrapTableConfig(this),
                getInputs().stream().map(i -> InputProperty.DEFAULT).collect(Collectors.toList()),
                FlinkTypeFactory.toLogicalRowType(rowType),
                getRelDetailedDescription(),
                uid,
                call,
                inputChangelogModes);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e);
        }
        return pw.item("invocation", scan.getCall())
                .item("uid", uid)
                .item("select", String.join(",", getRowType().getFieldNames()))
                .item("rowType", getRowType());
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    /**
     * An important part of {@link ProcessTableFunction} is the mandatory unique identifier for PTFs
     * with set semantics. Even if the PTF has no state entries, state or timers might be added
     * later. So a PTF should serve as an identifiable black box for the optimizer. UIDs ensure
     * that.
     *
     * @see SystemTypeInference
     */
    private static @Nullable String deriveUniqueIdentifier(FlinkLogicalTableFunctionScan scan) {
        final RexCall rexCall = (RexCall) scan.getCall();
        final BridgingSqlFunction.WithTableFunction function =
                (BridgingSqlFunction.WithTableFunction) rexCall.getOperator();
        final List<StaticArgument> staticArgs =
                function.getTypeInference()
                        .getStaticArguments()
                        .orElseThrow(IllegalStateException::new);
        final ContextResolvedFunction resolvedFunction = function.getResolvedFunction();
        final List<RexNode> operands = rexCall.getOperands();
        // Type inference ensures that uid is always added at the end
        final RexNode uidRexNode = operands.get(operands.size() - 1);
        if (uidRexNode.getKind() == SqlKind.DEFAULT) {
            // Optional for constant or row semantics functions
            if (staticArgs.stream().noneMatch(arg -> arg.is(StaticArgumentTrait.TABLE_AS_SET))) {
                return null;
            }
            final String uid =
                    resolvedFunction
                            .getIdentifier()
                            .map(FunctionIdentifier::getFunctionName)
                            .orElse("");
            if (SystemTypeInference.isInvalidUidForProcessTableFunction(uid)) {
                throw new ValidationException(
                        String.format(
                                "Could not derive a unique identifier for process table function '%s'. "
                                        + "The function's name does not qualify for a UID. Please provide "
                                        + "a custom identifier using the implicit `uid` argument. "
                                        + "For example: myFunction(..., uid => 'my-id')",
                                resolvedFunction.asSummaryString()));
            }
            return uid;
        }
        // Otherwise UID should be correct as it has been checked by SystemTypeInference.
        return RexLiteral.stringValue(uidRexNode);
    }

    private static void verifyTimeAttributes(List<RelNode> inputs, RexCall call) {
        final Set<String> onTimeFields = deriveOnTimeFields(call.getOperands());
        inputs.stream()
                .map(RelNode::getRowType)
                .forEach(rowType -> verifyTimeAttribute(rowType, onTimeFields));
    }

    private static void verifyTimeAttribute(RelDataType rowType, Set<String> onTimeFields) {
        onTimeFields.stream()
                .map(onTimeField -> rowType.getField(onTimeField, true, false))
                .filter(Objects::nonNull)
                .forEach(StreamPhysicalProcessTableFunction::verifyTimeAttribute);
    }

    private static void verifyTimeAttribute(RelDataTypeField timeColumn) {
        if (!FlinkTypeFactory.isTimeIndicatorType(timeColumn.getType())) {
            throw new ValidationException(
                    String.format(
                            "Column '%s' is not a valid time attribute. "
                                    + "Only columns with a watermark declaration qualify for the `on_time` argument. "
                                    + "Also, make sure that the watermarked column is forwarded without any modification.",
                            timeColumn.getName()));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Shared utilities
    // --------------------------------------------------------------------------------------------

    public static List<Ord<StaticArgument>> getProvidedInputArgs(RexCall call) {
        final List<RexNode> operands = call.getOperands();
        final BridgingSqlFunction.WithTableFunction function =
                (BridgingSqlFunction.WithTableFunction) call.getOperator();
        final List<StaticArgument> declaredArgs =
                function.getTypeInference()
                        .getStaticArguments()
                        .orElseThrow(IllegalStateException::new);
        // This logic filters out optional tables for which an input is missing. It returns tables
        // in the same order as provided inputs of this RelNode.
        return Ord.zip(declaredArgs).stream()
                .filter(arg -> arg.e.is(StaticArgumentTrait.TABLE))
                .filter(arg -> operands.get(arg.i) instanceof RexTableArgCall)
                .collect(Collectors.toList());
    }

    public static Set<String> deriveOnTimeFields(List<RexNode> operands) {
        final RexCall onTimeOperand =
                (RexCall)
                        operands.get(
                                operands.size() - 1 - PROCESS_TABLE_FUNCTION_ARG_ON_TIME_OFFSET);
        if (onTimeOperand.getKind() == SqlKind.DEFAULT) {
            return Set.of();
        }
        return onTimeOperand.getOperands().stream()
                .map(RexLiteral::stringValue)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }
}
