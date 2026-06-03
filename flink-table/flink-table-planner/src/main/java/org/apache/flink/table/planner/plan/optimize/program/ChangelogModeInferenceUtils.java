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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.ChangelogFunction.ChangelogContext;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalcBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalProcessTableFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.trait.DeleteKind;
import org.apache.flink.table.planner.plan.trait.DeleteKindTrait;
import org.apache.flink.table.planner.plan.trait.DeleteKindTraitDef;
import org.apache.flink.table.planner.plan.trait.ModifyKindSet;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTrait;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTraitDef;
import org.apache.flink.table.planner.plan.trait.UpdateKindTrait;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.types.RowKind;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.Option;

/**
 * Shared helper methods for changelog mode inference, used by {@link
 * FlinkChangelogModeInferenceProgram} and its trait visitors.
 */
final class ChangelogModeInferenceUtils {

    private ChangelogModeInferenceUtils() {}

    /**
     * Whether the condition of the given calc only references non-upsert-key columns. If so, the
     * calc can forward whatever changelog mode is required, because records are filtered based on
     * columns that don't take part in the upsert key.
     */
    static boolean isNonUpsertKeyCondition(StreamPhysicalCalcBase calc) {
        RexProgram program = calc.getProgram();
        if (program.getCondition() == null) {
            return false;
        }

        RexNode condition = program.expandLocalRef(program.getCondition());
        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(calc.getCluster().getMetadataQuery());
        Set<ImmutableBitSet> upsertKeys = fmq.getUpsertKeys(calc.getInput());
        if (upsertKeys == null || upsertKeys.isEmpty()) {
            // there are no upsert keys, so all columns are non-primary key columns
            return true;
        }

        int[] inputRefIndices =
                RexNodeExtractor.extractRefInputFields(Collections.singletonList(condition));
        ImmutableBitSet inputRefSet = ImmutableBitSet.of(inputRefIndices);
        return upsertKeys.stream().noneMatch(uk -> uk.contains(inputRefSet));
    }

    static ModifyKindSet getModifyKindSet(RelNode node) {
        ModifyKindSetTrait modifyKindSetTrait =
                node.getTraitSet().getTrait(ModifyKindSetTraitDef.INSTANCE());
        return modifyKindSetTrait.modifyKindSet();
    }

    static DeleteKind getDeleteKind(RelNode node) {
        DeleteKindTrait deleteKindTrait =
                node.getTraitSet().getTrait(DeleteKindTraitDef.INSTANCE());
        return deleteKindTrait.deleteKind();
    }

    // ----------------------------------------------------------------------------------------------
    // PTF helper methods
    // ----------------------------------------------------------------------------------------------

    static ChangelogMode toChangelogMode(
            StreamPhysicalRel node,
            @Nullable UpdateKindTrait updateKindTrait,
            @Nullable DeleteKindTrait deleteKindTrait) {
        ChangelogMode.Builder modeBuilder = ChangelogMode.newBuilder();
        Option<ChangelogMode> givenMode = ChangelogPlanUtils.getChangelogMode(node);
        if (givenMode.isEmpty()) {
            throw new IllegalStateException(
                    "Unable to derive changelog mode from node " + node + ". This is a bug.");
        }
        for (RowKind kind : givenMode.get().getContainedKinds()) {
            modeBuilder.addContainedKind(kind);
        }
        if (updateKindTrait != null && UpdateKindTrait.BEFORE_AND_AFTER().equals(updateKindTrait)) {
            modeBuilder.addContainedKind(RowKind.UPDATE_BEFORE);
        }
        if (deleteKindTrait != null && DeleteKindTrait.DELETE_BY_KEY().equals(deleteKindTrait)) {
            modeBuilder.keyOnlyDeletes(true);
        }
        return modeBuilder.build();
    }

    /**
     * Whether the PTF requires UPDATE_BEFORE from its input. Returns true unless partition keys
     * cover the upsert keys (co-located) and the argument doesn't explicitly require UPDATE_BEFORE.
     */
    static boolean ptfRequiresUpdateBefore(
            StaticArgument tableArg, RexTableArgCall tableArgCall, StreamPhysicalRel input) {
        ImmutableBitSet partitionKeys = ImmutableBitSet.of(tableArgCall.getPartitionKeys());
        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(input.getCluster().getMetadataQuery());
        Set<ImmutableBitSet> upsertKeys = fmq.getUpsertKeys(input);
        return upsertKeys == null
                || partitionKeys.isEmpty()
                || !upsertKeys.contains(partitionKeys)
                || tableArg.is(StaticArgumentTrait.REQUIRE_UPDATE_BEFORE);
    }

    static PtfTableArgComponents extractPtfTableArgComponents(
            StreamPhysicalProcessTableFunction process,
            StreamPhysicalRel child,
            Ord<StaticArgument> inputArg) {
        StaticArgument tableArg = inputArg.e;
        RexCall call = process.getCall();
        RexTableArgCall tableArgCall = (RexTableArgCall) call.getOperands().get(inputArg.i);
        ModifyKindSet modifyKindSet = getModifyKindSet(child);
        return new PtfTableArgComponents(tableArg, tableArgCall, modifyKindSet);
    }

    private static ChangelogContext toPtfChangelogContext(
            StreamPhysicalProcessTableFunction process,
            List<ChangelogMode> inputChangelogModes,
            ChangelogMode outputChangelogMode) {
        RexCall udfCall = StreamPhysicalProcessTableFunction.toUdfCall(process.getCall());
        List<Integer> inputTimeColumns =
                StreamPhysicalProcessTableFunction.toInputTimeColumns(process.getCall());
        BridgingSqlFunction function = (BridgingSqlFunction) udfCall.getOperator();
        CallContext callContext =
                function.toCallContext(
                        udfCall, inputTimeColumns, inputChangelogModes, outputChangelogMode);

        // Expose a simplified context focused on changelog-relevant inputs: changelog modes,
        // resolved literal arguments, and table semantics (e.g., partition-by columns).
        return new ChangelogContext() {
            @Override
            public ChangelogMode getTableChangelogMode(int pos) {
                TableSemantics tableSemantics = callContext.getTableSemantics(pos).orElse(null);
                if (tableSemantics == null) {
                    return null;
                }
                return tableSemantics.changelogMode().orElse(null);
            }

            @Override
            public ChangelogMode getRequiredChangelogMode() {
                return callContext.getOutputChangelogMode().orElse(null);
            }

            @Override
            public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
                return callContext.getArgumentValue(pos, clazz);
            }

            @Override
            public Optional<TableSemantics> getTableSemantics(int pos) {
                return callContext.getTableSemantics(pos);
            }
        };
    }

    static <T> T queryPtfChangelogMode(
            StreamPhysicalProcessTableFunction process,
            List<StreamPhysicalRel> children,
            ChangelogMode requiredChangelogMode,
            Function<ChangelogMode, T> toTraitSet,
            T defaultTraitSet) {
        RexCall call = process.getCall();
        FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(call);
        if (definition instanceof ChangelogFunction) {
            ChangelogFunction changelogFunction = (ChangelogFunction) definition;
            ChangelogContext changelogContext =
                    toPtfChangelogContext(process, toInputChangelogModes(children), requiredChangelogMode);
            ChangelogMode changelogMode = changelogFunction.getChangelogMode(changelogContext);
            verifyPtfTableArgsForUpdates(call, changelogMode);
            return toTraitSet.apply(changelogMode);
        } else if (definition instanceof BuiltInFunctionDefinition
                && ((BuiltInFunctionDefinition) definition).getChangelogModeStrategy().isPresent()) {
            BuiltInFunctionDefinition builtIn = (BuiltInFunctionDefinition) definition;
            ChangelogContext changelogContext =
                    toPtfChangelogContext(process, toInputChangelogModes(children), requiredChangelogMode);
            ChangelogMode changelogMode =
                    builtIn.getChangelogModeStrategy().get().inferChangelogMode(changelogContext);
            verifyPtfTableArgsForUpdates(call, changelogMode);
            return toTraitSet.apply(changelogMode);
        } else {
            return defaultTraitSet;
        }
    }

    private static List<ChangelogMode> toInputChangelogModes(List<StreamPhysicalRel> children) {
        return children.stream()
                .map(child -> toChangelogMode(child, null, null))
                .collect(Collectors.toList());
    }

    /**
     * Verifies that PTFs with upsert output (without UPDATE_BEFORE) use set semantics.
     *
     * <p>Retract mode (with UPDATE_BEFORE) is self-describing — each update carries either the old
     * and new value, so downstream can process it without a key. Row semantics is safe.
     *
     * <p>Upsert mode (without UPDATE_BEFORE) requires a key to look up previous values, so set
     * semantics with PARTITION BY is required.
     */
    private static void verifyPtfTableArgsForUpdates(RexCall call, ChangelogMode changelogMode) {
        if (changelogMode.containsOnly(RowKind.INSERT)
                || changelogMode.contains(RowKind.UPDATE_BEFORE)) {
            return;
        }
        for (Ord<StaticArgument> inputArg : StreamPhysicalProcessTableFunction.getProvidedInputArgs(call)) {
            StaticArgument tableArg = inputArg.e;
            if (tableArg.is(StaticArgumentTrait.ROW_SEMANTIC_TABLE)) {
                throw new ValidationException(
                        "PTFs that take table arguments with row semantics don't support upsert "
                                + "output. Table argument '"
                                + tableArg.getName()
                                + "' of function '"
                                + call.getOperator().toString()
                                + "' must use set semantics.");
            }
        }
    }

    /** Components of a PTF table argument, derived from a {@link RexTableArgCall} operand. */
    static final class PtfTableArgComponents {
        final StaticArgument tableArg;
        final RexTableArgCall tableArgCall;
        final ModifyKindSet modifyKindSet;

        PtfTableArgComponents(
                StaticArgument tableArg, RexTableArgCall tableArgCall, ModifyKindSet modifyKindSet) {
            this.tableArg = tableArg;
            this.tableArgCall = tableArgCall;
            this.modifyKindSet = modifyKindSet;
        }
    }
}
