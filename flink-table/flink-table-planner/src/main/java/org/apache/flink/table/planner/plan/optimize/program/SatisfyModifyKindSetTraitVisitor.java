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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.legacy.sinks.TableSink;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalcBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupTableAggregateBase;
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
import org.apache.flink.table.planner.plan.trait.ModifyKind;
import org.apache.flink.table.planner.plan.trait.ModifyKindSet;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTrait;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTraitDef;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.RankUtil;
import org.apache.flink.table.planner.sinks.DataStreamTableSink;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.getModifyKindSet;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.queryPtfChangelogMode;

/**
 * A visitor which will try to satisfy the required {@link ModifyKindSetTrait} from root.
 *
 * <p>After traversed by this visitor, every node should have a correct {@link ModifyKindSetTrait}
 * or an exception should be thrown if the planner doesn't support to satisfy the required {@link
 * ModifyKindSetTrait}.
 */
class SatisfyModifyKindSetTraitVisitor {

    /**
     * Try to satisfy the required {@link ModifyKindSetTrait} from root.
     *
     * <p>Each node should first require a {@link ModifyKindSetTrait} to its children. If the trait
     * provided by children does not satisfy the required one, it should throw an exception and
     * prompt the user that plan is not supported. The required {@link ModifyKindSetTrait} may come
     * from the node's parent, or come from the node itself, depending on whether the node will
     * destroy the trait provided by children or pass the trait from children.
     *
     * <p>Each node should provide {@link ModifyKindSetTrait} according to current node's behavior
     * and the ModifyKindSetTrait provided by children.
     *
     * @param rel the node who should satisfy the requiredTrait
     * @param requiredTrait the required ModifyKindSetTrait
     * @param requester the requester who starts the requirement, used for better exception message
     * @return A converted node which satisfy required traits by inputs node of current node. Or
     *     throws exception if required trait can't be satisfied.
     */
    StreamPhysicalRel visit(
            StreamPhysicalRel rel, ModifyKindSetTrait requiredTrait, String requester) {
        if (rel instanceof StreamPhysicalSink) {
            StreamPhysicalSink sink = (StreamPhysicalSink) rel;
            String name =
                    "Table sink '"
                            + sink.contextResolvedTable().getIdentifier().asSummaryString()
                            + "'";
            ChangelogMode queryModifyKindSet = deriveQueryDefaultChangelogMode(sink.getInput(), name);
            ModifyKindSetTrait sinkRequiredTrait =
                    ModifyKindSetTrait.fromChangelogMode(
                            sink.tableSink().getChangelogMode(queryModifyKindSet));
            List<StreamPhysicalRel> children = visitChildren(sink, sinkRequiredTrait, name);
            RelTraitSet sinkTrait = sink.getTraitSet().plus(ModifyKindSetTrait.EMPTY());
            // ignore required trait from context, because sink is the true root
            return (StreamPhysicalRel) sink.copy(sinkTrait, new ArrayList<RelNode>(children));
        } else if (rel instanceof StreamPhysicalLegacySink) {
            StreamPhysicalLegacySink<?> legacySink = (StreamPhysicalLegacySink<?>) rel;
            TableSink<?> tableSink = legacySink.sink();
            final ModifyKindSetTrait sinkRequiredTrait;
            final String name;
            if (tableSink instanceof UpsertStreamTableSink) {
                sinkRequiredTrait = ModifyKindSetTrait.ALL_CHANGES();
                name = "UpsertStreamTableSink";
            } else if (tableSink instanceof RetractStreamTableSink) {
                sinkRequiredTrait = ModifyKindSetTrait.ALL_CHANGES();
                name = "RetractStreamTableSink";
            } else if (tableSink instanceof AppendStreamTableSink) {
                sinkRequiredTrait = ModifyKindSetTrait.INSERT_ONLY();
                name = "AppendStreamTableSink";
            } else if (tableSink instanceof StreamTableSink) {
                sinkRequiredTrait = ModifyKindSetTrait.INSERT_ONLY();
                name = "StreamTableSink";
            } else if (tableSink instanceof DataStreamTableSink) {
                DataStreamTableSink<?> ds = (DataStreamTableSink<?>) tableSink;
                if (ds.withChangeFlag()) {
                    sinkRequiredTrait = ModifyKindSetTrait.ALL_CHANGES();
                    name = "toRetractStream";
                } else {
                    sinkRequiredTrait = ModifyKindSetTrait.INSERT_ONLY();
                    name = "toAppendStream";
                }
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported sink '" + tableSink.getClass().getSimpleName() + "'");
            }
            List<StreamPhysicalRel> children = visitChildren(legacySink, sinkRequiredTrait, name);
            RelTraitSet sinkTrait = legacySink.getTraitSet().plus(ModifyKindSetTrait.EMPTY());
            // ignore required trait from context, because sink is the true root
            return (StreamPhysicalRel) legacySink.copy(sinkTrait, new ArrayList<RelNode>(children));
        } else if (rel instanceof StreamPhysicalGroupAggregate) {
            // agg support all changes in input
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES());
            ModifyKindSet inputModifyKindSet = getModifyKindSet(children.get(0));
            ModifyKindSet.Builder builder =
                    ModifyKindSet.newBuilder()
                            .addContainedKind(ModifyKind.INSERT)
                            .addContainedKind(ModifyKind.UPDATE);
            if (inputModifyKindSet.contains(ModifyKind.UPDATE)
                    || inputModifyKindSet.contains(ModifyKind.DELETE)) {
                builder.addContainedKind(ModifyKind.DELETE);
            }
            ModifyKindSetTrait providedTrait = new ModifyKindSetTrait(builder.build());
            return createNewNode(rel, children, providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalGroupTableAggregateBase) {
            // table agg support all changes in input
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES());
            // table aggregate will produce all changes, including deletions
            return createNewNode(
                    rel, children, ModifyKindSetTrait.ALL_CHANGES(), requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalPythonGroupAggregate) {
            // agg support all changes in input
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES());
            ModifyKindSet inputModifyKindSet = getModifyKindSet(children.get(0));
            ModifyKindSet.Builder builder =
                    ModifyKindSet.newBuilder()
                            .addContainedKind(ModifyKind.INSERT)
                            .addContainedKind(ModifyKind.UPDATE);
            if (inputModifyKindSet.contains(ModifyKind.UPDATE)
                    || inputModifyKindSet.contains(ModifyKind.DELETE)) {
                builder.addContainedKind(ModifyKind.DELETE);
            }
            ModifyKindSetTrait providedTrait = new ModifyKindSetTrait(builder.build());
            return createNewNode(rel, children, providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalGroupWindowAggregateBase) {
            // WindowAggregate and WindowTableAggregate support all changes in input
            StreamPhysicalGroupWindowAggregateBase window =
                    (StreamPhysicalGroupWindowAggregateBase) rel;
            List<StreamPhysicalRel> children = visitChildren(window, ModifyKindSetTrait.ALL_CHANGES());
            ModifyKindSet.Builder builder =
                    ModifyKindSet.newBuilder().addContainedKind(ModifyKind.INSERT);
            if (window.emitStrategy().produceUpdates()) {
                builder.addContainedKind(ModifyKind.UPDATE);
            }
            ModifyKindSetTrait providedTrait = new ModifyKindSetTrait(builder.build());
            return createNewNode(window, children, providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalWindowAggregate) {
            // WindowAggregate and WindowTableAggregate support all changes in input
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES());
            // TODO support early / late fire and then this node may produce update records
            ModifyKindSetTrait providedTrait = new ModifyKindSetTrait(ModifyKindSet.INSERT_ONLY);
            return createNewNode(rel, children, providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalWindowRank
                || rel instanceof StreamPhysicalWindowDeduplicate) {
            // WindowAggregate, WindowRank, WindowDeduplicate support insert-only in input
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY());
            ModifyKindSetTrait providedTrait = ModifyKindSetTrait.INSERT_ONLY();
            return createNewNode(rel, children, providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalRank) {
            StreamPhysicalRank rank = (StreamPhysicalRank) rel;
            if (RankUtil.isDeduplication(rank)) {
                List<StreamPhysicalRel> children =
                        visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES());
                TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(rank);

                // if the rank is deduplication and can be executed as insert-only, forward that
                // information
                boolean insertOnly = children.stream().allMatch(ChangelogPlanUtils::isInsertOnly);

                final ModifyKindSetTrait providedTrait;
                if (insertOnly
                        && RankUtil.outputInsertOnlyInDeduplicate(
                                tableConfig,
                                RankUtil.keepLastDeduplicateRow(rank.orderKey()))) {
                    // Deduplicate outputs append only if first row is kept and mini batching is
                    // disabled
                    providedTrait = ModifyKindSetTrait.INSERT_ONLY();
                } else {
                    providedTrait = ModifyKindSetTrait.ALL_CHANGES();
                }
                return createNewNode(rel, children, providedTrait, requiredTrait, requester);
            } else {
                // Rank supports consuming all changes
                List<StreamPhysicalRel> children =
                        visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES());
                return createNewNode(
                        rel, children, ModifyKindSetTrait.ALL_CHANGES(), requiredTrait, requester);
            }
        } else if (rel instanceof StreamPhysicalLimit) {
            // limit support all changes in input
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES());
            ModifyKindSetTrait providedTrait =
                    getModifyKindSet(children.get(0)).isInsertOnly()
                            ? ModifyKindSetTrait.INSERT_ONLY()
                            : ModifyKindSetTrait.ALL_CHANGES();
            return createNewNode(rel, children, providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalSortLimit) {
            // SortLimit supports consuming all changes
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES());
            return createNewNode(
                    rel, children, ModifyKindSetTrait.ALL_CHANGES(), requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalSort) {
            // Sort supports consuming all changes
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES());
            // Sort will buffer all inputs, and produce insert-only messages when input is finished
            return createNewNode(
                    rel, children, ModifyKindSetTrait.INSERT_ONLY(), requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalMatch) {
            // CEP only supports consuming insert-only and producing insert-only changes
            // give a better requester name for exception message
            List<StreamPhysicalRel> children =
                    visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY(), "Match Recognize");
            return createNewNode(
                    rel, children, ModifyKindSetTrait.INSERT_ONLY(), requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalOverAggregate) {
            // OverAggregate can only support insert for row-time/proc-time sort keys
            StreamPhysicalOverAggregate over = (StreamPhysicalOverAggregate) rel;
            ModifyKindSetTrait overRequiredTrait = ModifyKindSetTrait.INSERT_ONLY();
            ModifyKindSet.Builder builder =
                    ModifyKindSet.newBuilder().addContainedKind(ModifyKind.INSERT);
            List<? extends org.apache.calcite.rel.core.Window.Group> groups =
                    over.logicWindow().groups;

            if (!groups.isEmpty() && !groups.get(0).orderKeys.getFieldCollations().isEmpty()) {
                // All aggregates are computed over the same window and order by is supported for
                // only 1 field
                int orderKeyIndex = groups.get(0).orderKeys.getFieldCollations().get(0).getFieldIndex();
                RelDataType orderKeyType =
                        over.logicWindow().getRowType().getFieldList().get(orderKeyIndex).getType();
                if (!FlinkTypeFactory.isRowtimeIndicatorType(orderKeyType)
                        && !FlinkTypeFactory.isProctimeIndicatorType(orderKeyType)) {
                    // Only non row-time/proc-time sort can support UPDATES
                    builder.addContainedKind(ModifyKind.UPDATE);
                    builder.addContainedKind(ModifyKind.DELETE);
                    overRequiredTrait = ModifyKindSetTrait.ALL_CHANGES();
                }
            }
            List<StreamPhysicalRel> children = visitChildren(over, overRequiredTrait);
            ModifyKindSetTrait providedTrait = new ModifyKindSetTrait(builder.build());
            return createNewNode(over, children, providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalTemporalSort
                || rel instanceof StreamPhysicalIntervalJoin
                || rel instanceof StreamPhysicalPythonOverAggregate) {
            // TemporalSort, IntervalJoin only support consuming insert-only
            // and producing insert-only changes
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY());
            return createNewNode(
                    rel, children, ModifyKindSetTrait.INSERT_ONLY(), requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalMLPredictTableFunction
                || rel instanceof StreamPhysicalVectorSearchTableFunction) {
            // MLPredict, VectorSearch supports only support consuming insert-only
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY());
            return createNewNode(
                    rel, children, ModifyKindSetTrait.INSERT_ONLY(), requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalJoin) {
            // join support all changes in input
            StreamPhysicalJoin join = (StreamPhysicalJoin) rel;
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES());
            ModifyKindSet leftKindSet = getModifyKindSet(children.get(0));
            ModifyKindSet rightKindSet = getModifyKindSet(children.get(children.size() - 1));
            boolean innerOrSemi =
                    join.joinSpec().getJoinType() == FlinkJoinType.INNER
                            || join.joinSpec().getJoinType() == FlinkJoinType.SEMI;
            final ModifyKindSetTrait providedTrait;
            if (innerOrSemi) {
                // forward left and right modify operations
                providedTrait = new ModifyKindSetTrait(leftKindSet.union(rightKindSet));
            } else {
                // otherwise, it may produce any kinds of changes
                providedTrait = ModifyKindSetTrait.ALL_CHANGES();
            }
            return createNewNode(join, children, providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalWindowJoin) {
            // Currently, window join only supports INSERT_ONLY in input
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY());
            return createNewNode(
                    rel, children, ModifyKindSetTrait.INSERT_ONLY(), requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalTemporalJoin) {
            // currently, temporal join supports all kings of changes, including right side
            StreamPhysicalTemporalJoin temporalJoin = (StreamPhysicalTemporalJoin) rel;
            List<StreamPhysicalRel> children =
                    visitChildren(temporalJoin, ModifyKindSetTrait.ALL_CHANGES());
            // forward left input changes
            ModifyKindSetTrait leftTrait =
                    children.get(0).getTraitSet().getTrait(ModifyKindSetTraitDef.INSTANCE());
            return createNewNode(temporalJoin, children, leftTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalMultiJoin) {
            // multi-join supports all changes in input
            StreamPhysicalMultiJoin multiJoin = (StreamPhysicalMultiJoin) rel;
            List<StreamPhysicalRel> children =
                    visitChildren(multiJoin, ModifyKindSetTrait.ALL_CHANGES());
            boolean allInnerJoins =
                    multiJoin.getJoinTypes().stream().allMatch(t -> t == JoinRelType.INNER);
            final ModifyKindSetTrait providedTrait;
            if (allInnerJoins) {
                // if all are inner joins, forward all modify operations from children
                ModifyKindSet[] kindSets =
                        children.stream()
                                .map(ChangelogModeInferenceUtils::getModifyKindSet)
                                .toArray(ModifyKindSet[]::new);
                providedTrait = new ModifyKindSetTrait(ModifyKindSet.union(kindSets));
            } else {
                // if there is any outer join, it may produce any kinds of changes
                providedTrait = ModifyKindSetTrait.ALL_CHANGES();
            }
            return createNewNode(multiJoin, children, providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalCalcBase
                || rel instanceof StreamPhysicalCorrelateBase
                || rel instanceof StreamPhysicalLookupJoin
                || rel instanceof StreamPhysicalExchange
                || rel instanceof StreamPhysicalExpand
                || rel instanceof StreamPhysicalMiniBatchAssigner
                || rel instanceof StreamPhysicalWatermarkAssigner
                || rel instanceof StreamPhysicalWindowTableFunction) {
            // transparent forward requiredTrait to children
            List<StreamPhysicalRel> children = visitChildren(rel, requiredTrait, requester);
            ModifyKindSetTrait childrenTrait =
                    children.get(0).getTraitSet().getTrait(ModifyKindSetTraitDef.INSTANCE());
            // forward children mode
            return createNewNode(rel, children, childrenTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalUnion) {
            // transparent forward requiredTrait to children
            StreamPhysicalUnion union = (StreamPhysicalUnion) rel;
            List<StreamPhysicalRel> children = visitChildren(rel, requiredTrait, requester);
            // union provides all possible kinds of children have
            ModifyKindSet[] kindSets =
                    children.stream()
                            .map(ChangelogModeInferenceUtils::getModifyKindSet)
                            .toArray(ModifyKindSet[]::new);
            ModifyKindSet providedKindSet = ModifyKindSet.union(kindSets);
            return createNewNode(
                    union,
                    children,
                    new ModifyKindSetTrait(providedKindSet),
                    requiredTrait,
                    requester);
        } else if (rel instanceof StreamPhysicalChangelogNormalize) {
            // changelog normalize support update&delete input
            List<StreamPhysicalRel> children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES());
            // changelog normalize will output all changes
            ModifyKindSetTrait providedTrait = ModifyKindSetTrait.ALL_CHANGES();
            return createNewNode(rel, children, providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalTableSourceScan) {
            // ScanTableSource supports produces updates and deletions
            StreamPhysicalTableSourceScan ts = (StreamPhysicalTableSourceScan) rel;
            ModifyKindSetTrait providedTrait =
                    ModifyKindSetTrait.fromChangelogMode(ts.tableSource().getChangelogMode());
            return createNewNode(
                    ts, Collections.emptyList(), providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalDataStreamScan
                || rel instanceof StreamPhysicalLegacyTableSourceScan
                || rel instanceof StreamPhysicalValues) {
            // DataStream, TableSource and Values only support producing insert-only messages
            return createNewNode(
                    rel,
                    Collections.emptyList(),
                    ModifyKindSetTrait.INSERT_ONLY(),
                    requiredTrait,
                    requester);
        } else if (rel instanceof StreamPhysicalIntermediateTableScan) {
            StreamPhysicalIntermediateTableScan scan = (StreamPhysicalIntermediateTableScan) rel;
            ModifyKindSetTrait providedTrait =
                    new ModifyKindSetTrait(scan.intermediateTable().modifyKindSet());
            return createNewNode(
                    scan, Collections.emptyList(), providedTrait, requiredTrait, requester);
        } else if (rel instanceof StreamPhysicalProcessTableFunction) {
            // Accepted changes depend on table argument declaration
            StreamPhysicalProcessTableFunction process = (StreamPhysicalProcessTableFunction) rel;
            List<Ord<StaticArgument>> inputArgs =
                    StreamPhysicalProcessTableFunction.getProvidedInputArgs(process.getCall());
            List<ModifyKindSetTrait> requiredChildrenTraits = new ArrayList<>();
            for (Ord<StaticArgument> inputArg : inputArgs) {
                StaticArgument tableArg = inputArg.e;
                requiredChildrenTraits.add(
                        tableArg.is(StaticArgumentTrait.SUPPORT_UPDATES)
                                ? ModifyKindSetTrait.ALL_CHANGES()
                                : ModifyKindSetTrait.INSERT_ONLY());
            }
            final List<StreamPhysicalRel> children;
            if (requiredChildrenTraits.isEmpty()) {
                // Constant function has a single StreamPhysicalValues input
                children = visitChildren(process, ModifyKindSetTrait.INSERT_ONLY());
            } else {
                children = visitChildren(process, requiredChildrenTraits);
            }
            // Query PTF for updating vs. non-updating
            ModifyKindSetTrait providedModifyTrait =
                    queryPtfChangelogMode(
                            process,
                            children,
                            requiredTrait.modifyKindSet().toChangelogModeBuilder().build(),
                            ModifyKindSetTrait::fromChangelogMode,
                            ModifyKindSetTrait.INSERT_ONLY());
            return createNewNode(process, children, providedModifyTrait, requiredTrait, requester);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported visit for " + rel.getClass().getSimpleName());
        }
    }

    private List<StreamPhysicalRel> visitChildren(
            StreamPhysicalRel parent, ModifyKindSetTrait requiredChildrenTrait) {
        return visitChildren(parent, requiredChildrenTrait, getNodeName(parent));
    }

    private List<StreamPhysicalRel> visitChildren(
            StreamPhysicalRel parent, ModifyKindSetTrait requiredChildrenTrait, String requester) {
        List<StreamPhysicalRel> newChildren = new ArrayList<>();
        for (int i = 0; i < parent.getInputs().size(); i++) {
            newChildren.add(visitChild(parent, i, requiredChildrenTrait, requester));
        }
        return newChildren;
    }

    private List<StreamPhysicalRel> visitChildren(
            StreamPhysicalRel parent, List<ModifyKindSetTrait> requiredChildrenTraits) {
        String requester = getNodeName(parent);
        List<StreamPhysicalRel> newChildren = new ArrayList<>();
        for (int i = 0; i < parent.getInputs().size(); i++) {
            newChildren.add(visitChild(parent, i, requiredChildrenTraits.get(i), requester));
        }
        return newChildren;
    }

    private StreamPhysicalRel visitChild(
            StreamPhysicalRel parent,
            int childOrdinal,
            ModifyKindSetTrait requiredChildTrait,
            String requester) {
        StreamPhysicalRel child = (StreamPhysicalRel) parent.getInput(childOrdinal);
        return visit(child, requiredChildTrait, requester);
    }

    private static String getNodeName(StreamPhysicalRel rel) {
        String prefix = "StreamExec";
        String typeName = rel.getRelTypeName();
        if (typeName.startsWith(prefix)) {
            return typeName.substring(prefix.length());
        } else {
            return typeName;
        }
    }

    /** Derives the {@link ModifyKindSetTrait} of query plan without required ModifyKindSet validation. */
    private ChangelogMode deriveQueryDefaultChangelogMode(RelNode queryNode, String name) {
        StreamPhysicalRel newNode =
                visit((StreamPhysicalRel) queryNode, ModifyKindSetTrait.ALL_CHANGES(), name);
        return getModifyKindSet(newNode).toDefaultChangelogMode();
    }

    private StreamPhysicalRel createNewNode(
            StreamPhysicalRel node,
            List<StreamPhysicalRel> children,
            ModifyKindSetTrait providedTrait,
            ModifyKindSetTrait requiredTrait,
            String requestedOwner) {
        if (!providedTrait.satisfies(requiredTrait)) {
            ModifyKindSet diff = providedTrait.modifyKindSet().minus(requiredTrait.modifyKindSet());
            // for deterministic error message
            List<ModifyKind> sortedKinds = new ArrayList<>(diff.getContainedKinds());
            Collections.sort(sortedKinds);
            String diffString =
                    sortedKinds.stream()
                            .map(kind -> kind.toString().toLowerCase())
                            .collect(Collectors.joining(" and "));
            // creates a new node based on the new children, to have a more correct node description
            // e.g. description of GroupAggregate is based on the ModifyKindSetTrait of children
            StreamPhysicalRel tempNode =
                    (StreamPhysicalRel) node.copy(node.getTraitSet(), new ArrayList<RelNode>(children));
            String nodeString = tempNode.getRelDetailedDescription();
            throw new TableException(
                    requestedOwner
                            + " doesn't support consuming "
                            + diffString
                            + " changes which is produced by node "
                            + nodeString);
        }
        RelTraitSet newTraitSet = node.getTraitSet().plus(providedTrait);
        return (StreamPhysicalRel) node.copy(newTraitSet, new ArrayList<RelNode>(children));
    }
}
