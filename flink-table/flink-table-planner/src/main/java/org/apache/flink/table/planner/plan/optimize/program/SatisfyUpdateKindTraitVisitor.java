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

import org.apache.flink.legacy.table.sinks.AppendStreamTableSink;
import org.apache.flink.legacy.table.sinks.RetractStreamTableSink;
import org.apache.flink.legacy.table.sinks.StreamTableSink;
import org.apache.flink.legacy.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.api.InsertConflictStrategy.ConflictBehavior;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.legacy.sinks.TableSink;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalcBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupTableAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupWindowAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntermediateTableScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMLPredictTableFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMatch;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMultiJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalProcessTableFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalPythonGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalPythonGroupTableAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalPythonOverAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSortLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalValues;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalVectorSearchTableFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;
import org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.PtfTableArgComponents;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.trait.ModifyKind;
import org.apache.flink.table.planner.plan.trait.ModifyKindSet;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTrait;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTraitDef;
import org.apache.flink.table.planner.plan.trait.UpdateKind;
import org.apache.flink.table.planner.plan.trait.UpdateKindTrait;
import org.apache.flink.table.planner.plan.trait.UpdateKindTraitDef;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.planner.sinks.DataStreamTableSink;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.types.RowKind;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.extractPtfTableArgComponents;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.getModifyKindSet;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.isNonUpsertKeyCondition;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.ptfRequiresUpdateBefore;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.queryPtfChangelogMode;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.toChangelogMode;

/**
 * A visitor which will try to satisfy the required {@link UpdateKindTrait} from root.
 *
 * <p>After traversed by this visitor, every node should have a correct {@link UpdateKindTrait} or
 * returns {@link Optional#empty()} if the planner doesn't support to satisfy the required {@link
 * UpdateKindTrait}.
 */
class SatisfyUpdateKindTraitVisitor {

    private final StreamOptimizeContext context;

    SatisfyUpdateKindTraitVisitor(StreamOptimizeContext context) {
        this.context = context;
    }

    /**
     * Try to satisfy the required {@link UpdateKindTrait} from root.
     *
     * <p>Each node will first require a UpdateKindTrait to its children. The required
     * UpdateKindTrait may come from the node's parent, or come from the node itself, depending on
     * whether the node will destroy the trait provided by children or pass the trait from children.
     *
     * <p>If the node will pass the children's UpdateKindTrait without destroying it, then return a
     * new node with new inputs and forwarded UpdateKindTrait.
     *
     * <p>If the node will destroy the children's UpdateKindTrait, then the node itself needs to be
     * converted, or a new node should be generated to satisfy the required trait, such as marking
     * itself not to generate UPDATE_BEFORE, or generating a new node to filter UPDATE_BEFORE.
     *
     * @param rel the node who should satisfy the requiredTrait
     * @param requiredUpdateTrait the required UpdateKindTrait
     * @return A converted node which satisfies required traits by input nodes of current node. Or
     *     {@link Optional#empty()} if required traits cannot be satisfied.
     */
    Optional<StreamPhysicalRel> visit(StreamPhysicalRel rel, UpdateKindTrait requiredUpdateTrait) {
        if (rel instanceof StreamPhysicalSink) {
            StreamPhysicalSink sink = (StreamPhysicalSink) rel;
            List<UpdateKindTrait> sinkRequiredTraits = inferSinkRequiredTraits(sink);
            boolean upsertMaterialize = analyzeUpsertMaterializeStrategy(sink);
            return visitSink(sink.copy(upsertMaterialize), sinkRequiredTraits);
        } else if (rel instanceof StreamPhysicalLegacySink) {
            StreamPhysicalLegacySink<?> legacySink = (StreamPhysicalLegacySink<?>) rel;
            ModifyKindSet childModifyKindSet = getModifyKindSet(legacySink.getInput());
            UpdateKindTrait onlyAfter = UpdateKindTrait.onlyAfterOrNone(childModifyKindSet);
            UpdateKindTrait beforeAndAfter = UpdateKindTrait.beforeAfterOrNone(childModifyKindSet);
            TableSink<?> tableSink = legacySink.sink();
            final List<UpdateKindTrait> sinkRequiredTraits;
            if (tableSink instanceof UpsertStreamTableSink) {
                // support both ONLY_AFTER and BEFORE_AFTER, but prefer ONLY_AFTER
                sinkRequiredTraits = Arrays.asList(onlyAfter, beforeAndAfter);
            } else if (tableSink instanceof RetractStreamTableSink) {
                sinkRequiredTraits = Collections.singletonList(beforeAndAfter);
            } else if (tableSink instanceof AppendStreamTableSink
                    || tableSink instanceof StreamTableSink) {
                sinkRequiredTraits = Collections.singletonList(UpdateKindTrait.NONE());
            } else if (tableSink instanceof DataStreamTableSink) {
                DataStreamTableSink<?> ds = (DataStreamTableSink<?>) tableSink;
                if (ds.withChangeFlag()) {
                    if (ds.needUpdateBefore()) {
                        sinkRequiredTraits = Collections.singletonList(beforeAndAfter);
                    } else {
                        // support both ONLY_AFTER and BEFORE_AFTER, but prefer ONLY_AFTER
                        sinkRequiredTraits = Arrays.asList(onlyAfter, beforeAndAfter);
                    }
                } else {
                    sinkRequiredTraits = Collections.singletonList(UpdateKindTrait.NONE());
                }
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported sink '" + tableSink.getClass().getSimpleName() + "'");
            }
            return visitSink(legacySink, sinkRequiredTraits);
        } else if (requiresUpdateBeforeFromChildren(rel)) {
            // Aggregate, TableAggregate, OverAggregate, Limit, GroupWindowAggregate,
            // WindowAggregate,
            // and WindowTableAggregate requires update_before if there are updates
            UpdateKindTrait requiredChildUpdateTrait =
                    UpdateKindTrait.beforeAfterOrNone(getModifyKindSet(rel.getInput(0)));
            Optional<List<StreamPhysicalRel>> children =
                    visitChildren(rel, requiredChildUpdateTrait);
            // use requiredTrait as providedTrait, because they should support all kinds of
            // UpdateKind
            return createNewNode(rel, children, requiredUpdateTrait);
        } else if (requiresNoUpdateFromChildren(rel)) {
            // WindowRank, WindowDeduplicate, Deduplicate, TemporalSort, CEP,
            // and IntervalJoin, WindowJoin require nothing about UpdateKind.
            Optional<List<StreamPhysicalRel>> children = visitChildren(rel, UpdateKindTrait.NONE());
            return createNewNode(rel, children, requiredUpdateTrait);
        } else if (rel instanceof StreamPhysicalRank) {
            StreamPhysicalRank rank = (StreamPhysicalRank) rel;
            List<RankProcessStrategy> rankStrategies =
                    RankProcessStrategy.analyzeRankProcessStrategies(
                            rank, rank.partitionKey(), rank.orderKey());
            return visitRankStrategies(rankStrategies, requiredUpdateTrait, rank::copy);
        } else if (rel instanceof StreamPhysicalSortLimit) {
            StreamPhysicalSortLimit sortLimit = (StreamPhysicalSortLimit) rel;
            List<RankProcessStrategy> rankStrategies =
                    RankProcessStrategy.analyzeRankProcessStrategies(
                            sortLimit, ImmutableBitSet.of(), sortLimit.getCollation());
            return visitRankStrategies(rankStrategies, requiredUpdateTrait, sortLimit::copy);
        } else if (rel instanceof StreamPhysicalSort) {
            StreamPhysicalSort sort = (StreamPhysicalSort) rel;
            UpdateKindTrait requiredChildTrait =
                    UpdateKindTrait.beforeAfterOrNone(getModifyKindSet(sort.getInput()));
            Optional<List<StreamPhysicalRel>> children = visitChildren(sort, requiredChildTrait);
            return createNewNode(sort, children, requiredUpdateTrait);
        } else if (rel instanceof StreamPhysicalJoin) {
            StreamPhysicalJoin join = (StreamPhysicalJoin) rel;
            boolean onlyAfterByParent =
                    requiredUpdateTrait.updateKind() == UpdateKind.ONLY_UPDATE_AFTER;
            List<Optional<StreamPhysicalRel>> children = new ArrayList<>();
            for (int childOrdinal = 0; childOrdinal < join.getInputs().size(); childOrdinal++) {
                StreamPhysicalRel physicalChild = (StreamPhysicalRel) join.getInput(childOrdinal);
                boolean supportOnlyAfter = join.inputUniqueKeyContainsJoinKey(childOrdinal);
                ModifyKindSet inputModifyKindSet = getModifyKindSet(physicalChild);
                if (onlyAfterByParent) {
                    if (inputModifyKindSet.contains(ModifyKind.UPDATE) && !supportOnlyAfter) {
                        // the parent requires only-after, however, the join doesn't support this
                        children.add(Optional.empty());
                    } else {
                        children.add(
                                visit(
                                        physicalChild,
                                        UpdateKindTrait.onlyAfterOrNone(inputModifyKindSet)));
                    }
                } else {
                    children.add(
                            visit(
                                    physicalChild,
                                    UpdateKindTrait.beforeAfterOrNone(inputModifyKindSet)));
                }
            }
            if (children.stream().anyMatch(c -> !c.isPresent())) {
                return Optional.empty();
            }
            return createNewNode(join, Optional.of(present(children)), requiredUpdateTrait);
        } else if (rel instanceof StreamPhysicalTemporalJoin) {
            StreamPhysicalTemporalJoin temporalJoin = (StreamPhysicalTemporalJoin) rel;
            StreamPhysicalRel left = (StreamPhysicalRel) temporalJoin.getLeft();
            StreamPhysicalRel right = (StreamPhysicalRel) temporalJoin.getRight();

            // the left input required trait depends on it's parent in temporal join
            // the left input will send message to parent
            boolean requiredUpdateBeforeByParent =
                    requiredUpdateTrait.updateKind() == UpdateKind.BEFORE_AND_AFTER;
            ModifyKindSet leftInputModifyKindSet = getModifyKindSet(left);
            UpdateKindTrait leftRequiredTrait =
                    requiredUpdateBeforeByParent
                            ? UpdateKindTrait.beforeAfterOrNone(leftInputModifyKindSet)
                            : UpdateKindTrait.onlyAfterOrNone(leftInputModifyKindSet);
            Optional<StreamPhysicalRel> newLeftOption = visit(left, leftRequiredTrait);

            ModifyKindSet rightInputModifyKindSet = getModifyKindSet(right);
            // currently temporal join support changelog stream as the right side
            // so it supports both ONLY_AFTER and BEFORE_AFTER, but prefer ONLY_AFTER
            Optional<StreamPhysicalRel> newRightOption =
                    visit(right, UpdateKindTrait.onlyAfterOrNone(rightInputModifyKindSet));
            if (!newRightOption.isPresent()) {
                newRightOption =
                        visit(right, UpdateKindTrait.beforeAfterOrNone(rightInputModifyKindSet));
            }

            if (newLeftOption.isPresent() && newRightOption.isPresent()) {
                StreamPhysicalRel newLeft = newLeftOption.get();
                StreamPhysicalRel newRight = newRightOption.get();
                UpdateKindTrait leftTrait =
                        newLeft.getTraitSet().getTrait(UpdateKindTraitDef.INSTANCE());
                return createNewNode(
                        temporalJoin, Optional.of(Arrays.asList(newLeft, newRight)), leftTrait);
            } else {
                return Optional.empty();
            }
        } else if (rel instanceof StreamPhysicalCalcBase) {
            // if the condition is applied on the upsert key, we can emit whatever the requiredTrait
            // is, because we will filter all records based on the condition that applies to that
            // key
            StreamPhysicalCalcBase calc = (StreamPhysicalCalcBase) rel;
            if (requiredUpdateTrait.equals(UpdateKindTrait.ONLY_UPDATE_AFTER())
                    && isNonUpsertKeyCondition(calc)) {
                // we don't expect filter to satisfy ONLY_UPDATE_AFTER update kind,
                // to solve the bad case like a single 'cnt < 10' condition after aggregation.
                // See FLINK-9528.
                return Optional.empty();
            }
            // otherwise, forward UpdateKind requirement
            Optional<List<StreamPhysicalRel>> children = visitChildren(rel, requiredUpdateTrait);
            if (!children.isPresent()) {
                return Optional.empty();
            }
            UpdateKindTrait childTrait =
                    children.get().get(0).getTraitSet().getTrait(UpdateKindTraitDef.INSTANCE());
            return createNewNode(rel, children, childTrait);
        } else if (isTransparentForwardOperator(rel)) {
            // transparent forward requiredTrait to children
            Optional<List<StreamPhysicalRel>> children = visitChildren(rel, requiredUpdateTrait);
            if (!children.isPresent()) {
                return Optional.empty();
            }
            UpdateKindTrait childTrait =
                    children.get().get(0).getTraitSet().getTrait(UpdateKindTraitDef.INSTANCE());
            return createNewNode(rel, children, childTrait);
        } else if (rel instanceof StreamPhysicalUnion) {
            StreamPhysicalUnion union = (StreamPhysicalUnion) rel;
            List<Optional<StreamPhysicalRel>> children = new ArrayList<>();
            for (RelNode childNode : union.getInputs()) {
                StreamPhysicalRel child = (StreamPhysicalRel) childNode;
                ModifyKindSet childModifyKindSet = getModifyKindSet(child);
                UpdateKindTrait requiredChildTrait =
                        childModifyKindSet.isInsertOnly()
                                ? UpdateKindTrait.NONE()
                                : requiredUpdateTrait;
                children.add(visit(child, requiredChildTrait));
            }
            if (children.stream().anyMatch(c -> !c.isPresent())) {
                return Optional.empty();
            }
            List<StreamPhysicalRel> childRels = present(children);
            List<UpdateKindTrait> updateKinds = new ArrayList<>();
            for (StreamPhysicalRel child : childRels) {
                updateKinds.add(child.getTraitSet().getTrait(UpdateKindTraitDef.INSTANCE()));
            }
            // union can just forward changes, can't actively satisfy to another changelog mode
            final UpdateKindTrait providedTrait;
            if (updateKinds.stream().allMatch(k -> UpdateKindTrait.NONE().equals(k))) {
                // if all the children is NO_UPDATE, union is NO_UPDATE
                providedTrait = UpdateKindTrait.NONE();
            } else {
                // otherwise, merge update kinds.
                UpdateKind merged = null;
                for (UpdateKindTrait updateKindTrait : updateKinds) {
                    UpdateKind updateKind = updateKindTrait.updateKind();
                    if (merged == null) {
                        merged = updateKind;
                    } else if (merged == UpdateKind.NONE) {
                        merged = updateKind;
                    } else if (updateKind == UpdateKind.NONE) {
                        // merged stays unchanged
                    } else if (merged == updateKind) {
                        // merged stays unchanged
                    } else {
                        // UNION doesn't support to union ONLY_UPDATE_AFTER and BEFORE_AND_AFTER
                        // inputs
                        return Optional.empty();
                    }
                }
                providedTrait = new UpdateKindTrait(merged);
            }
            return createNewNode(union, Optional.of(childRels), providedTrait);
        } else if (rel instanceof StreamPhysicalChangelogNormalize) {
            StreamPhysicalChangelogNormalize normalize = (StreamPhysicalChangelogNormalize) rel;
            // changelog normalize currently only supports input only sending UPDATE_AFTER
            Optional<List<StreamPhysicalRel>> children =
                    visitChildren(normalize, UpdateKindTrait.ONLY_UPDATE_AFTER());
            // use requiredTrait as providedTrait,
            // because changelog normalize supports all kinds of UpdateKind
            return createNewNode(rel, children, requiredUpdateTrait);
        } else if (rel instanceof StreamPhysicalTableSourceScan) {
            // currently only support BEFORE_AND_AFTER if source produces updates
            StreamPhysicalTableSourceScan ts = (StreamPhysicalTableSourceScan) rel;
            UpdateKindTrait providedTrait =
                    UpdateKindTrait.fromChangelogMode(ts.tableSource().getChangelogMode());
            Optional<StreamPhysicalRel> newSource =
                    createNewNode(rel, Optional.of(Collections.emptyList()), providedTrait);
            if (providedTrait.equals(UpdateKindTrait.BEFORE_AND_AFTER())
                    && requiredUpdateTrait.equals(UpdateKindTrait.ONLY_UPDATE_AFTER())) {
                // requiring only-after, but the source is CDC source, then drop update_before
                // manually
                StreamPhysicalDropUpdateBefore dropUB =
                        new StreamPhysicalDropUpdateBefore(
                                rel.getCluster(), rel.getTraitSet(), rel);
                return createNewNode(
                        dropUB, newSource.map(Collections::singletonList), requiredUpdateTrait);
            } else {
                return newSource;
            }
        } else if (rel instanceof StreamPhysicalDataStreamScan
                || rel instanceof StreamPhysicalLegacyTableSourceScan
                || rel instanceof StreamPhysicalValues) {
            return createNewNode(rel, Optional.of(Collections.emptyList()), UpdateKindTrait.NONE());
        } else if (rel instanceof StreamPhysicalIntermediateTableScan) {
            StreamPhysicalIntermediateTableScan scan = (StreamPhysicalIntermediateTableScan) rel;
            final UpdateKindTrait providedTrait;
            if (scan.intermediateTable().isUpdateBeforeRequired()) {
                // we can't drop UPDATE_BEFORE if it is required by other parent blocks
                providedTrait = UpdateKindTrait.BEFORE_AND_AFTER();
            } else {
                providedTrait = requiredUpdateTrait;
            }
            if (!providedTrait.satisfies(requiredUpdateTrait)) {
                // require ONLY_AFTER but can only provide BEFORE_AND_AFTER
                return Optional.empty();
            }
            return createNewNode(rel, Optional.of(Collections.emptyList()), providedTrait);
        } else if (rel instanceof StreamPhysicalProcessTableFunction) {
            // Required update traits depend on the table argument declaration,
            // input traits, partition keys, and upsert keys
            StreamPhysicalProcessTableFunction process = (StreamPhysicalProcessTableFunction) rel;
            List<Ord<StaticArgument>> inputArgs =
                    StreamPhysicalProcessTableFunction.getProvidedInputArgs(process.getCall());
            List<StreamPhysicalRel> children = new ArrayList<>();
            List<RelNode> inputs = process.getInputs();
            for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
                StreamPhysicalRel child = (StreamPhysicalRel) inputs.get(inputIndex);
                final Optional<StreamPhysicalRel> visited;
                // For PTF without table arguments (i.e. values child)
                if (inputArgs.isEmpty()) {
                    visited = visit(child, UpdateKindTrait.NONE());
                } else {
                    // Derive the required update trait for table arguments
                    Ord<StaticArgument> inputArg = inputArgs.get(inputIndex);
                    PtfTableArgComponents components =
                            extractPtfTableArgComponents(process, child, inputArg);
                    StaticArgument tableArg = components.tableArg;
                    final UpdateKindTrait childRequiredTrait;
                    if (!components.modifyKindSet.isInsertOnly()
                            && tableArg.is(StaticArgumentTrait.SUPPORT_UPDATES)) {
                        childRequiredTrait =
                                ptfRequiresUpdateBefore(tableArg, components.tableArgCall, child)
                                        ? UpdateKindTrait.BEFORE_AND_AFTER()
                                        : UpdateKindTrait.ONLY_UPDATE_AFTER();
                    } else {
                        childRequiredTrait = UpdateKindTrait.NONE();
                    }
                    visited = visit(child, childRequiredTrait);
                }
                visited.ifPresent(children::add);
            }
            // Query PTF for upsert vs. retract
            UpdateKindTrait providedUpdateTrait =
                    queryPtfChangelogMode(
                            process,
                            children,
                            toChangelogMode(process, requiredUpdateTrait, null),
                            UpdateKindTrait::fromChangelogMode,
                            UpdateKindTrait.NONE());
            return createNewNode(rel, Optional.of(children), providedUpdateTrait);
        } else if (rel instanceof StreamPhysicalMultiJoin) {
            StreamPhysicalMultiJoin multiJoin = (StreamPhysicalMultiJoin) rel;
            boolean onlyAfterByParent =
                    requiredUpdateTrait.updateKind() == UpdateKind.ONLY_UPDATE_AFTER;
            List<Optional<StreamPhysicalRel>> children = new ArrayList<>();
            for (int childOrdinal = 0;
                    childOrdinal < multiJoin.getInputs().size();
                    childOrdinal++) {
                StreamPhysicalRel physicalChild =
                        (StreamPhysicalRel) multiJoin.getInput(childOrdinal);
                boolean supportOnlyAfter =
                        multiJoin.inputUniqueKeyContainsCommonJoinKey(childOrdinal);
                ModifyKindSet inputModifyKindSet = getModifyKindSet(physicalChild);
                if (onlyAfterByParent) {
                    if (inputModifyKindSet.contains(ModifyKind.UPDATE) && !supportOnlyAfter) {
                        // the parent requires only-after, however, the multi-join doesn't support
                        // this for this input
                        children.add(Optional.empty());
                    } else {
                        children.add(
                                visit(
                                        physicalChild,
                                        UpdateKindTrait.onlyAfterOrNone(inputModifyKindSet)));
                    }
                } else {
                    children.add(
                            visit(
                                    physicalChild,
                                    UpdateKindTrait.beforeAfterOrNone(inputModifyKindSet)));
                }
            }
            if (children.stream().anyMatch(c -> !c.isPresent())) {
                return Optional.empty();
            }
            return createNewNode(multiJoin, Optional.of(present(children)), requiredUpdateTrait);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported visit for " + rel.getClass().getSimpleName());
        }
    }

    /**
     * Aggregate, TableAggregate, OverAggregate, Limit, GroupWindowAggregate, WindowAggregate, and
     * WindowTableAggregate require update_before if there are updates.
     */
    private static boolean requiresUpdateBeforeFromChildren(StreamPhysicalRel rel) {
        return rel instanceof StreamPhysicalGroupAggregate
                || rel instanceof StreamPhysicalGroupTableAggregate
                || rel instanceof StreamPhysicalLimit
                || rel instanceof StreamPhysicalPythonGroupAggregate
                || rel instanceof StreamPhysicalPythonGroupTableAggregate
                || rel instanceof StreamPhysicalGroupWindowAggregateBase
                || rel instanceof StreamPhysicalWindowAggregate
                || rel instanceof StreamPhysicalOverAggregate;
    }

    /**
     * WindowRank, WindowDeduplicate, TemporalSort, CEP, IntervalJoin and WindowJoin require nothing
     * about UpdateKind.
     */
    private static boolean requiresNoUpdateFromChildren(StreamPhysicalRel rel) {
        return rel instanceof StreamPhysicalWindowRank
                || rel instanceof StreamPhysicalWindowDeduplicate
                || rel instanceof StreamPhysicalTemporalSort
                || rel instanceof StreamPhysicalMatch
                || rel instanceof StreamPhysicalIntervalJoin
                || rel instanceof StreamPhysicalPythonOverAggregate
                || rel instanceof StreamPhysicalWindowJoin;
    }

    /** Operators that transparently forward the required UpdateKindTrait to their children. */
    private static boolean isTransparentForwardOperator(StreamPhysicalRel rel) {
        return rel instanceof StreamPhysicalCorrelateBase
                || rel instanceof StreamPhysicalLookupJoin
                || rel instanceof StreamPhysicalExchange
                || rel instanceof StreamPhysicalExpand
                || rel instanceof StreamPhysicalMiniBatchAssigner
                || rel instanceof StreamPhysicalWatermarkAssigner
                || rel instanceof StreamPhysicalWindowTableFunction
                || rel instanceof StreamPhysicalMLPredictTableFunction
                || rel instanceof StreamPhysicalVectorSearchTableFunction;
    }

    private Optional<List<StreamPhysicalRel>> visitChildren(
            StreamPhysicalRel parent, UpdateKindTrait requiredChildrenUpdateTrait) {
        List<StreamPhysicalRel> newChildren = new ArrayList<>();
        for (RelNode childNode : parent.getInputs()) {
            Optional<StreamPhysicalRel> newChild =
                    visit((StreamPhysicalRel) childNode, requiredChildrenUpdateTrait);
            if (!newChild.isPresent()) {
                // return empty if one of the children can't satisfy
                return Optional.empty();
            }
            UpdateKindTrait providedUpdateTrait =
                    newChild.get().getTraitSet().getTrait(UpdateKindTraitDef.INSTANCE());
            if (!providedUpdateTrait.satisfies(requiredChildrenUpdateTrait)) {
                // the provided trait can't satisfy required trait, thus we should return empty.
                return Optional.empty();
            }
            newChildren.add(newChild.get());
        }
        return Optional.of(newChildren);
    }

    private Optional<StreamPhysicalRel> createNewNode(
            StreamPhysicalRel node,
            Optional<List<StreamPhysicalRel>> childrenOption,
            UpdateKindTrait providedUpdateTrait) {
        if (!childrenOption.isPresent()) {
            return Optional.empty();
        }
        List<StreamPhysicalRel> children = childrenOption.get();
        ModifyKindSetTrait modifyKindSetTrait =
                node.getTraitSet().getTrait(ModifyKindSetTraitDef.INSTANCE());
        String nodeDescription = node.getRelDetailedDescription();
        final boolean isUpdateKindValid;
        switch (providedUpdateTrait.updateKind()) {
            case NONE:
                isUpdateKindValid = !modifyKindSetTrait.modifyKindSet().contains(ModifyKind.UPDATE);
                break;
            case BEFORE_AND_AFTER:
            case ONLY_UPDATE_AFTER:
                isUpdateKindValid = modifyKindSetTrait.modifyKindSet().contains(ModifyKind.UPDATE);
                break;
            default:
                isUpdateKindValid = false;
        }
        if (!isUpdateKindValid) {
            throw new TableException(
                    "UpdateKindTrait "
                            + providedUpdateTrait
                            + " conflicts with ModifyKindSetTrait "
                            + modifyKindSetTrait
                            + ". This is a bug in planner, please file an issue. \n"
                            + "Current node is "
                            + nodeDescription
                            + ".");
        }
        RelTraitSet newTraitSet = node.getTraitSet().plus(providedUpdateTrait);
        return Optional.of(
                (StreamPhysicalRel) node.copy(newTraitSet, new ArrayList<RelNode>(children)));
    }

    /**
     * Try all possible rank strategies and return the first viable new node.
     *
     * @param rankStrategies all possible supported rank strategy by current node
     * @param requiredUpdateKindTrait the required UpdateKindTrait by parent of rank node
     * @param applyRankStrategy a function to apply rank strategy to get a new copied rank node
     */
    private Optional<StreamPhysicalRel> visitRankStrategies(
            List<RankProcessStrategy> rankStrategies,
            UpdateKindTrait requiredUpdateKindTrait,
            Function<RankProcessStrategy, StreamPhysicalRel> applyRankStrategy) {
        // go pass every RankProcessStrategy, apply the rank strategy to get a new copied rank node,
        // return the first satisfied converted node
        for (RankProcessStrategy strategy : rankStrategies) {
            final UpdateKindTrait requiredChildrenTrait;
            if (strategy instanceof RankProcessStrategy.UpdateFastStrategy) {
                requiredChildrenTrait = UpdateKindTrait.ONLY_UPDATE_AFTER();
            } else if (strategy instanceof RankProcessStrategy.RetractStrategy) {
                requiredChildrenTrait = UpdateKindTrait.BEFORE_AND_AFTER();
            } else if (strategy instanceof RankProcessStrategy.AppendFastStrategy) {
                requiredChildrenTrait = UpdateKindTrait.NONE();
            } else {
                throw new IllegalStateException(
                        "Unsupported rank strategy: " + strategy.getClass().getSimpleName());
            }
            StreamPhysicalRel node = applyRankStrategy.apply(strategy);
            Optional<List<StreamPhysicalRel>> children = visitChildren(node, requiredChildrenTrait);
            Optional<StreamPhysicalRel> newNode =
                    createNewNode(node, children, requiredUpdateKindTrait);
            if (newNode.isPresent()) {
                return newNode;
            }
        }
        return Optional.empty();
    }

    private Optional<StreamPhysicalRel> visitSink(
            StreamPhysicalRel sink, List<UpdateKindTrait> sinkRequiredTraits) {
        List<List<StreamPhysicalRel>> satisfiedChildren = new ArrayList<>();
        for (UpdateKindTrait requiredTrait : sinkRequiredTraits) {
            visitChildren(sink, requiredTrait).ifPresent(satisfiedChildren::add);
        }
        if (satisfiedChildren.isEmpty()) {
            return Optional.empty();
        }
        RelTraitSet sinkTrait = sink.getTraitSet().plus(UpdateKindTrait.NONE());
        return Optional.of(
                (StreamPhysicalRel)
                        sink.copy(sinkTrait, new ArrayList<RelNode>(satisfiedChildren.get(0))));
    }

    /**
     * Infer sink required traits by the sink node and its input. Sink required traits is based on
     * the sink node's changelog mode, the only exception is when sink's pk(s) are not satisfied by
     * the input's upsert keys (considering immutable columns) and sink's changelog mode is
     * ONLY_UPDATE_AFTER.
     */
    private List<UpdateKindTrait> inferSinkRequiredTraits(StreamPhysicalSink sink) {
        ModifyKindSet childModifyKindSet = getModifyKindSet(sink.getInput());
        UpdateKindTrait onlyAfter = UpdateKindTrait.onlyAfterOrNone(childModifyKindSet);
        UpdateKindTrait beforeAndAfter = UpdateKindTrait.beforeAfterOrNone(childModifyKindSet);
        UpdateKindTrait sinkTrait =
                UpdateKindTrait.fromChangelogMode(
                        sink.tableSink()
                                .getChangelogMode(childModifyKindSet.toDefaultChangelogMode()));

        if (sinkTrait.equals(UpdateKindTrait.ONLY_UPDATE_AFTER())) {
            // if sink's pk(s) are not satisfied by input upsert keys (considering immutable
            // columns),
            // fallback to beforeAndAfter mode for correctness
            boolean requireBeforeAndAfter = !canUpsertKeysWithImmutableColsSatisfyPk(sink);
            if (requireBeforeAndAfter) {
                return Collections.singletonList(beforeAndAfter);
            } else {
                return Arrays.asList(onlyAfter, beforeAndAfter);
            }
        } else if (sinkTrait.equals(UpdateKindTrait.BEFORE_AND_AFTER())) {
            return Collections.singletonList(beforeAndAfter);
        } else {
            return Collections.singletonList(UpdateKindTrait.NONE());
        }
    }

    /**
     * Check whether input's upsert keys (together with immutable columns) can satisfy sink's
     * primary keys.
     *
     * <p>A sink pk is considered "satisfied" when there exists an upsert key {@code uk} such that:
     *
     * <ul>
     *   <li>{@code uk} is a subset of sink pk (no extra columns that could cause key collision)
     *   <li>the remaining sink pk columns not in {@code uk} are all immutable (immutable columns
     *       never change, so they effectively act as part of the key for upsert semantics)
     * </ul>
     *
     * <p>Example: sink pk = {a, b, c}, uk = {a, b}, immutable columns = {a, b, c, d}.
     *
     * <ul>
     *   <li>Step 1: uk {a, b} ⊆ sink pk {a, b, c} → true
     *   <li>Step 2: sink pk \ uk = {c}, immutable columns contain {c} → true
     *   <li>Result: satisfied
     * </ul>
     *
     * <p>Notice: even if sink pk is a subset of the upsert key, the pk is NOT considered satisfied
     * when the upsert key has columns outside sink pk. This differs from batch job's unique key
     * inference.
     */
    private boolean canUpsertKeysWithImmutableColsSatisfyPk(StreamPhysicalSink sink) {
        int[] sinkDefinedPks =
                sink.contextResolvedTable().getResolvedSchema().getPrimaryKeyIndexes();
        if (sinkDefinedPks.length == 0) {
            return true;
        }
        ImmutableBitSet sinkPks = ImmutableBitSet.of(sinkDefinedPks);
        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(sink.getCluster().getMetadataQuery());
        Set<ImmutableBitSet> changeLogUpsertKeys = fmq.getUpsertKeys(sink.getInput());
        // if upsert key is null, pk cannot be satisfied, should fall back to beforeAndAfter
        if (changeLogUpsertKeys == null) {
            return false;
        }
        ImmutableBitSet immutableCols = fmq.getImmutableColumns(sink.getInput());
        if (immutableCols == null) {
            immutableCols = ImmutableBitSet.of();
        }
        final ImmutableBitSet immutableColumns = immutableCols;

        // when input immutableCols is empty, this degrades to uk.equals(sinkPks)
        return changeLogUpsertKeys.stream()
                .anyMatch(
                        uk -> {
                            // 1. uk ⊆ sinkPks
                            boolean isSinkPkContainsUk = sinkPks.contains(uk);
                            // 2. (sinkPks \ uk) ⊆ immutableCols
                            ImmutableBitSet extraSinkPkCols = sinkPks.except(uk);
                            boolean areExtraSinkPkColsImmutable =
                                    immutableColumns.contains(extraSinkPkCols);
                            return isSinkPkContainsUk && areExtraSinkPkColsImmutable;
                        });
    }

    /**
     * Analyze whether to enable upsertMaterialize or not. In these case will return true:
     *
     * <ol>
     *   <li>when {@code TABLE_EXEC_SINK_UPSERT_MATERIALIZE} set to FORCE and sink's primary key
     *       nonempty.
     *   <li>when {@code TABLE_EXEC_SINK_UPSERT_MATERIALIZE} set to AUTO and sink's primary key
     *       doesn't contain upsertKeys of the input update stream.
     * </ol>
     *
     * <p>Also validates that ON CONFLICT clause is specified when upsert key differs from primary
     * key.
     */
    private boolean analyzeUpsertMaterializeStrategy(StreamPhysicalSink sink) {
        TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(sink);
        ChangelogMode inputChangelogMode =
                ChangelogPlanUtils.getChangelogMode((StreamPhysicalRel) sink.getInput()).get();
        int[] primaryKeys = sink.contextResolvedTable().getResolvedSchema().getPrimaryKeyIndexes();
        ChangelogMode sinkChangelogMode = sink.tableSink().getChangelogMode(inputChangelogMode);
        boolean inputIsAppend = inputChangelogMode.containsOnly(RowKind.INSERT);
        boolean sinkIsAppend = sinkChangelogMode.containsOnly(RowKind.INSERT);
        boolean sinkIsRetract = sinkChangelogMode.contains(RowKind.UPDATE_BEFORE);

        // Validate ON CONFLICT is only allowed for upsert sinks
        if (sink.conflictStrategy() != null) {
            boolean isUpsertSink = !sinkIsAppend && !sinkIsRetract;
            if (!isUpsertSink) {
                String reason =
                        sinkIsAppend
                                ? "it only accepts INSERT (append-only) changes"
                                : "it requires UPDATE_BEFORE (retract mode)";
                throw new ValidationException(
                        "ON CONFLICT clause is only allowed for upsert sinks. The sink '"
                                + sink.contextResolvedTable().getIdentifier().asSummaryString()
                                + "' is not an upsert sink because "
                                + reason
                                + ".");
            }
        }

        // Validate that sources have watermarks when using ERROR or NOTHING strategy
        if (sink.conflictStrategy() != null
                && (sink.conflictStrategy().getBehavior() == ConflictBehavior.ERROR
                        || sink.conflictStrategy().getBehavior() == ConflictBehavior.NOTHING)) {
            validateSourcesHaveWatermarks(sink);
        }

        switch (tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE)) {
            case FORCE:
                return primaryKeys.length > 0 && !sinkIsRetract;
            case NONE:
                return false;
            case AUTO:
                // if the sink is not an UPSERT sink (has no PK, or is an APPEND or RETRACT sink)
                // we don't need to materialize results
                if (primaryKeys.length == 0 || sinkIsAppend || sinkIsRetract) {
                    return false;
                }

                // For a DEDUPLICATE strategy and INSERT only input, we simply let the inserts be
                // handled as UPSERT_AFTER and overwrite previous value
                if (inputIsAppend && sink.isDeduplicateConflictStrategy()) {
                    return false;
                }

                // if input has updates and primary key != upsert key  we should enable
                // upsertMaterialize.
                //
                // An optimize is: do not enable upsertMaterialize when sink pk(s) contains input
                // changeLogUpsertKeys
                boolean upsertKeyDiffersFromPk = !sink.primaryKeysContainsUpsertKey();

                // Validate that ON CONFLICT is specified when upsert key differs from primary key
                boolean requireOnConflict =
                        tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT);
                if (requireOnConflict
                        && upsertKeyDiffersFromPk
                        && sink.conflictStrategy() == null) {
                    String pkNames = sink.getPrimaryKeyNames();
                    String upsertKeyNames = sink.getUpsertKeyNames();
                    throw new ValidationException(
                            "The query has an upsert key that differs from the primary key of the "
                                    + "sink table '"
                                    + sink.contextResolvedTable().getIdentifier().asSummaryString()
                                    + "'. Primary key: "
                                    + pkNames
                                    + ", upsert key: "
                                    + upsertKeyNames
                                    + ". This can lead to non-deterministic results when multiple "
                                    + "records with different upsert keys map to the same primary "
                                    + "key. Please specify an ON CONFLICT clause to define how "
                                    + "conflicts should be handled: ON CONFLICT DO DEDUPLICATE "
                                    + "(update to the latest record, state intensive, since we need "
                                    + "to keep the entire history), or ON CONFLICT DO ERROR (fail on "
                                    + "conflict), or ON CONFLICT DO NOTHING (keep first record).");
                }

                return upsertKeyDiffersFromPk;
            default:
                throw new IllegalStateException(
                        "Unsupported upsert materialize strategy: "
                                + tableConfig.get(
                                        ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE));
        }
    }

    private void validateSourcesHaveWatermarks(StreamPhysicalSink sink) {
        List<String> sourcesWithoutWatermarks = new ArrayList<>();
        collectSourcesWithoutWatermarks(sink.getInput(), sourcesWithoutWatermarks);
        if (!sourcesWithoutWatermarks.isEmpty()) {
            throw new ValidationException(
                    "ON CONFLICT DO "
                            + sink.conflictStrategy().getBehavior()
                            + " requires all source tables to define watermarks, but the following "
                            + "source(s) do not: "
                            + String.join(", ", sourcesWithoutWatermarks)
                            + ". Please add a WATERMARK declaration to these tables.");
        }
    }

    private void collectSourcesWithoutWatermarks(RelNode rel, List<String> result) {
        if (rel instanceof StreamPhysicalTableSourceScan) {
            StreamPhysicalTableSourceScan ts = (StreamPhysicalTableSourceScan) rel;
            TableSourceTable table = ts.getTable().unwrap(TableSourceTable.class);
            if (table != null
                    && table.contextResolvedTable()
                            .getResolvedSchema()
                            .getWatermarkSpecs()
                            .isEmpty()) {
                result.add(table.contextResolvedTable().getIdentifier().asSummaryString());
            }
        } else {
            rel.getInputs().forEach(input -> collectSourcesWithoutWatermarks(input, result));
        }
    }

    private static List<StreamPhysicalRel> present(List<Optional<StreamPhysicalRel>> options) {
        List<StreamPhysicalRel> result = new ArrayList<>(options.size());
        for (Optional<StreamPhysicalRel> option : options) {
            result.add(option.get());
        }
        return result;
    }
}
