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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecProcessTableFunction;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.SystemTypeInference;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableSet;

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
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        final RexCall call = (RexCall) scan.getCall();
        validateAllowSystemArgs(call);
        this.uid = deriveUniqueIdentifier(scan);
        verifyInputSize(ShortcutUtils.unwrapTableConfig(cluster), inputs.size());
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
        final ChangelogMode outputChangelogMode =
                JavaScalaConversionUtil.toJava(ChangelogPlanUtils.getChangelogMode(this))
                        .orElseThrow(IllegalStateException::new);
        final RexCall call = (RexCall) scan.getCall();
        verifyTimeAttributes(getInputs(), call, inputChangelogModes, outputChangelogMode);
        final List<Ord<StaticArgument>> providedInputArgs = getProvidedInputArgs(call);
        verifyPassThroughColumnsForUpdates(providedInputArgs, outputChangelogMode);
        return new StreamExecProcessTableFunction(
                unwrapTableConfig(this),
                getInputs().stream().map(i -> InputProperty.DEFAULT).collect(Collectors.toList()),
                FlinkTypeFactory.toLogicalRowType(rowType),
                getRelDetailedDescription(),
                uid,
                call,
                inputChangelogModes,
                outputChangelogMode);
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
            if (staticArgs.stream()
                    .noneMatch(arg -> arg.is(StaticArgumentTrait.SET_SEMANTIC_TABLE))) {
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

    public static void validateAllowSystemArgs(RexCall rexCall) {
        final BridgingSqlFunction.WithTableFunction function =
                (BridgingSqlFunction.WithTableFunction) rexCall.getOperator();

        if (function.getTypeInference().disableSystemArguments()) {
            // Disabling uid and time attributes for process table functions with implementation
            // is not supported for now. It can only be disabled for syntax purpose: for example
            // it's disabled for ML_PREDICT which is not processed by this rule.
            throw new ValidationException(
                    "Disabling system arguments is not supported for user-defined PTF.");
        }
    }

    private static void verifyTimeAttributes(
            List<RelNode> inputs,
            RexCall call,
            List<ChangelogMode> inputChangelogModes,
            ChangelogMode outputChangelogMode) {
        final Set<String> onTimeFields = deriveOnTimeFields(call);
        verifyOnTimeForUpdates(onTimeFields, inputChangelogModes, outputChangelogMode);
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

    private static void verifyOnTimeForUpdates(
            Set<String> onTimeFields,
            List<ChangelogMode> inputChangelogModes,
            ChangelogMode outputChangelogMode) {
        if (onTimeFields.isEmpty()) {
            return;
        }
        final boolean isUpdating =
                inputChangelogModes.stream().anyMatch(c -> !c.containsOnly(RowKind.INSERT))
                        || !outputChangelogMode.containsOnly(RowKind.INSERT);
        if (isUpdating) {
            throw new ValidationException(
                    "Time operations using the `on_time` argument are currently not supported "
                            + "for PTFs that consume or produce updates.");
        }
    }

    private static void verifyPassThroughColumnsForUpdates(
            List<Ord<StaticArgument>> providedInputArgs, ChangelogMode requiredChangelogMode) {
        if (!requiredChangelogMode.containsOnly(RowKind.INSERT)
                && providedInputArgs.stream()
                        .anyMatch(arg -> arg.e.is(StaticArgumentTrait.PASS_COLUMNS_THROUGH))) {
            throw new ValidationException(
                    "Pass-through columns are not supported for PTFs that produce updates.");
        }
    }

    private static void verifyInputSize(TableConfig tableConfig, int providedInputArgs) {
        final int maxCount = tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_PTF_MAX_TABLES);
        if (providedInputArgs > maxCount) {
            throw new ValidationException(
                    String.format(
                            "Unsupported table argument count. Currently, the number of input tables is limited to %s.",
                            maxCount));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Shared utilities
    // --------------------------------------------------------------------------------------------

    /**
     * Returns a list of table arguments (and their position) that have been provided in the call
     * and thus correspond to the {@link StreamPhysicalRel}'s input.
     */
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

    public static Set<String> deriveOnTimeFields(RexCall call) {
        final List<RexNode> operands = call.getOperands();
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

    /**
     * Removes all columns added by the {@link SystemTypeInference} and returns a call that
     * corresponds to the signature of the UDF.
     */
    public static RexCall toUdfCall(RexCall call) {
        final BridgingSqlFunction function = ShortcutUtils.unwrapBridgingSqlFunction(call);
        assert function != null;
        final List<StaticArgument> staticArgs =
                function.getTypeInference()
                        .getStaticArguments()
                        .orElseThrow(IllegalStateException::new);
        final List<RexNode> operands = call.getOperands();

        // Remove system arguments
        final List<RexNode> newOperands =
                operands.subList(
                        0,
                        operands.size()
                                - SystemTypeInference.PROCESS_TABLE_FUNCTION_SYSTEM_ARGS.size());

        // Remove system output columns
        final int prefixOutputSystemFields =
                Ord.zip(newOperands).stream()
                        .mapToInt(
                                operand -> {
                                    if (!(operand.e instanceof RexTableArgCall)) {
                                        return 0;
                                    }
                                    final RexTableArgCall tableArg = (RexTableArgCall) operand.e;
                                    final StaticArgument staticArg = staticArgs.get(0);
                                    if (staticArg.is(StaticArgumentTrait.PASS_COLUMNS_THROUGH)) {
                                        return tableArg.getType().getFieldCount();
                                    } else {
                                        return tableArg.getPartitionKeys().length;
                                    }
                                })
                        .sum();
        final RexNode onTimeArg =
                operands.get(operands.size() - 1 - PROCESS_TABLE_FUNCTION_ARG_ON_TIME_OFFSET);
        final int suffixOutputSystemFields =
                (onTimeArg.getKind() == SqlKind.DEFAULT || RexUtil.isNullLiteral(onTimeArg, true))
                        ? 0
                        : 1;
        final List<RelDataTypeField> projectedFields =
                call.getType()
                        .getFieldList()
                        .subList(
                                prefixOutputSystemFields,
                                call.getType().getFieldCount() - suffixOutputSystemFields);
        final RelDataType newReturnType =
                function.getTypeFactory().createStructType(projectedFields);

        return call.clone(newReturnType, newOperands);
    }

    /**
     * Returns the time column corresponding to each table argument's {@link StreamPhysicalRel}
     * input. Contains -1 if a time attribute was not defined for the input.
     */
    public static List<Integer> toInputTimeColumns(RexCall call) {
        final List<RexNode> operands = call.getOperands();
        final Set<String> onTimeFields = deriveOnTimeFields(call);
        final List<Ord<StaticArgument>> providedInputArgs =
                StreamPhysicalProcessTableFunction.getProvidedInputArgs(call);
        return providedInputArgs.stream()
                .map(
                        providedInputArg -> {
                            final RexTableArgCall tableArgCall =
                                    (RexTableArgCall) operands.get(providedInputArg.i);
                            return onTimeFields.stream()
                                    .map(
                                            onTimeField ->
                                                    tableArgCall
                                                            .getType()
                                                            .getField(onTimeField, true, false))
                                    .filter(Objects::nonNull)
                                    .map(RelDataTypeField::getIndex)
                                    .findFirst()
                                    .orElse(-1);
                        })
                .collect(Collectors.toList());
    }

    public static Set<ImmutableBitSet> toPartitionColumns(RexCall call) {
        final List<RexNode> operands = call.getOperands();
        final List<Ord<StaticArgument>> providedInputArgs =
                StreamPhysicalProcessTableFunction.getProvidedInputArgs(call);
        final Set<ImmutableBitSet> partitionColumnsPerArg = new HashSet<>();
        int pos = 0;
        for (Ord<StaticArgument> providedInputArg : providedInputArgs) {
            final RexTableArgCall tableArgCall = (RexTableArgCall) operands.get(providedInputArg.i);
            if (providedInputArg.e.is(StaticArgumentTrait.PASS_COLUMNS_THROUGH)) {
                // System type inference ensures that at most one table
                // argument can pass columns through. In that case, the
                // output preserves the position of partition columns.
                // f(t(c1, c2, k1, k2, c3) PARTITION BY (k1, k2))
                // -> [c1, c2, k1, k2, c3, function out...]
                assert providedInputArgs.size() == 1;
                final List<Integer> partitionColumns =
                        Arrays.stream(tableArgCall.getPartitionKeys())
                                .boxed()
                                .collect(Collectors.toList());
                partitionColumnsPerArg.add(ImmutableBitSet.of(partitionColumns));
            } else {
                final int partitionKeyCount = tableArgCall.getPartitionKeys().length;
                // Output is prefixed with partition keys:
                // f(t1 PARTITION BY (k1, k2), t2 PARTITION BY (k3, k4))
                // -> [k1, k2, k3, k4, function out...]
                final List<Integer> partitionColumns =
                        IntStream.range(pos, partitionKeyCount)
                                .boxed()
                                .collect(Collectors.toList());
                pos += partitionKeyCount;
                partitionColumnsPerArg.add(ImmutableBitSet.of(partitionColumns));
            }
        }
        return ImmutableSet.copyOf(partitionColumnsPerArg);
    }

    public static CallContext toCallContext(
            RexCall udfCall,
            List<Integer> inputTimeColumns,
            List<ChangelogMode> inputChangelogModes,
            @Nullable ChangelogMode outputChangelogMode) {
        final BridgingSqlFunction function = ShortcutUtils.unwrapBridgingSqlFunction(udfCall);
        assert function != null;
        final FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(udfCall);
        return new OperatorBindingCallContext(
                function.getDataTypeFactory(),
                definition,
                RexCallBinding.create(function.getTypeFactory(), udfCall, Collections.emptyList()),
                udfCall.getType(),
                inputTimeColumns,
                inputChangelogModes,
                outputChangelogMode);
    }
}
