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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
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
import org.apache.flink.table.planner.plan.optimize.ChangelogNormalizeRequirementResolver;
import org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.PtfTableArgComponents;
import org.apache.flink.table.planner.plan.trait.DeleteKind;
import org.apache.flink.table.planner.plan.trait.DeleteKindTrait;
import org.apache.flink.table.planner.plan.trait.DeleteKindTraitDef;
import org.apache.flink.table.planner.plan.trait.ModifyKind;
import org.apache.flink.table.planner.plan.trait.ModifyKindSet;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTrait;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTraitDef;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.extractPtfTableArgComponents;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.getDeleteKind;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.getModifyKindSet;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.isNonUpsertKeyCondition;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.ptfRequiresUpdateBefore;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.queryPtfChangelogMode;
import static org.apache.flink.table.planner.plan.optimize.program.ChangelogModeInferenceUtils.toChangelogMode;

/**
 * A visitor which will try to satisfy the required {@link DeleteKindTrait} from root.
 *
 * <p>After traversed by this visitor, every node should have a correct {@link DeleteKindTrait} or
 * returns {@link Optional#empty()} if the planner doesn't support to satisfy the required {@link
 * DeleteKindTrait}.
 */
class SatisfyDeleteKindTraitVisitor {

    private final StreamOptimizeContext context;

    SatisfyDeleteKindTraitVisitor(StreamOptimizeContext context) {
        this.context = context;
    }

    /**
     * Try to satisfy the required {@link DeleteKindTrait} from root.
     *
     * <p>Each node will first require a DeleteKindTrait to its children. The required
     * DeleteKindTrait may come from the node's parent, or come from the node itself, depending on
     * whether the node will destroy the trait provided by children or pass the trait from children.
     *
     * <p>If the node will pass the children's DeleteKindTrait without destroying it, then return a
     * new node with new inputs and forwarded DeleteKindTrait.
     *
     * <p>If the node will destroy the children's UpdateKindTrait, then the node itself needs to be
     * converted, or a new node should be generated to satisfy the required trait, such as marking
     * itself not to generate UPDATE_BEFORE, or generating a new node to filter UPDATE_BEFORE.
     *
     * @param rel the node who should satisfy the requiredTrait
     * @param requiredTrait the required DeleteKindTrait
     * @return A converted node which satisfies required traits by input nodes of current node. Or
     *     {@link Optional#empty()} if required traits cannot be satisfied.
     */
    Optional<StreamPhysicalRel> visit(StreamPhysicalRel rel, DeleteKindTrait requiredTrait) {
        if (rel instanceof StreamPhysicalSink) {
            StreamPhysicalSink sink = (StreamPhysicalSink) rel;
            List<DeleteKindTrait> sinkRequiredTraits = inferSinkRequiredTraits(sink);
            return visitSink(sink, sinkRequiredTraits);
        } else if (rel instanceof StreamPhysicalLegacySink) {
            ModifyKindSet childModifyKindSet = getModifyKindSet(rel.getInput(0));
            DeleteKindTrait fullDelete = DeleteKindTrait.fullDeleteOrNone(childModifyKindSet);
            return visitSink(rel, Collections.singletonList(fullDelete));
        } else if (requiresFullDeleteIfUpdates(rel)) {
            // if not explicitly supported, all operators require full deletes if there are updates
            List<StreamPhysicalRel> children = new ArrayList<>();
            for (RelNode child : rel.getInputs()) {
                visit((StreamPhysicalRel) child, DeleteKindTrait.fullDeleteOrNone(getModifyKindSet(child)))
                        .ifPresent(children::add);
            }
            return createNewNode(
                    rel, Optional.of(children), DeleteKindTrait.fullDeleteOrNone(getModifyKindSet(rel)));
        } else if (rel instanceof StreamPhysicalProcessTableFunction) {
            // Required delete traits depend on the table argument declaration,
            // input traits, partition keys, and upsert keys
            StreamPhysicalProcessTableFunction process = (StreamPhysicalProcessTableFunction) rel;
            RexCall call = process.getCall();
            List<Ord<StaticArgument>> inputArgs =
                    StreamPhysicalProcessTableFunction.getProvidedInputArgs(call);
            List<StreamPhysicalRel> children = new ArrayList<>();
            List<RelNode> inputs = process.getInputs();
            for (int inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
                final StreamPhysicalRel child = (StreamPhysicalRel) inputs.get(inputIndex);
                final Optional<StreamPhysicalRel> visited;
                // For PTF without table arguments (i.e. values child)
                if (inputArgs.isEmpty()) {
                    visited = visit(child, DeleteKindTrait.NONE());
                } else {
                    // Derive the required delete trait for table arguments
                    Ord<StaticArgument> inputArg = inputArgs.get(inputIndex);
                    PtfTableArgComponents components =
                            extractPtfTableArgComponents(process, child, inputArg);
                    StaticArgument tableArg = components.tableArg;
                    final ModifyKindSet modifyKindSet = components.modifyKindSet;
                    if (tableArg.is(StaticArgumentTrait.SUPPORT_UPDATES)
                            && !ptfRequiresUpdateBefore(tableArg, components.tableArgCall, child)
                            && !tableArg.is(StaticArgumentTrait.REQUIRE_FULL_DELETE)) {
                        visited =
                                visit(child, DeleteKindTrait.deleteOnKeyOrNone(modifyKindSet))
                                        .or(
                                                () ->
                                                        visit(
                                                                child,
                                                                DeleteKindTrait.fullDeleteOrNone(
                                                                        modifyKindSet)));
                    } else {
                        visited = visit(child, DeleteKindTrait.fullDeleteOrNone(modifyKindSet));
                    }
                }
                visited.ifPresent(children::add);
            }
            final ModifyKindSet modifyTrait = getModifyKindSet(rel);
            // Query the PTF for full vs. partial deletes
            DeleteKindTrait providedDeleteTrait =
                    queryPtfChangelogMode(
                            process,
                            children,
                            toChangelogMode(process, null, requiredTrait),
                            mode ->
                                    mode.keyOnlyDeletes()
                                            ? DeleteKindTrait.deleteOnKeyOrNone(modifyTrait)
                                            : DeleteKindTrait.fullDeleteOrNone(modifyTrait),
                            DeleteKindTrait.fullDeleteOrNone(modifyTrait));
            return createNewNode(process, Optional.of(children), providedDeleteTrait);
        } else if (rel instanceof StreamPhysicalJoin) {
            StreamPhysicalJoin join = (StreamPhysicalJoin) rel;
            List<Optional<StreamPhysicalRel>> children = new ArrayList<>();
            for (int childOrdinal = 0; childOrdinal < join.getInputs().size(); childOrdinal++) {
                final StreamPhysicalRel physicalChild =
                        (StreamPhysicalRel) join.getInput(childOrdinal);
                boolean supportsDeleteByKey = join.inputUniqueKeyContainsJoinKey(childOrdinal);
                final ModifyKindSet inputModifyKindSet = getModifyKindSet(physicalChild);
                if (supportsDeleteByKey && DeleteKindTrait.DELETE_BY_KEY().equals(requiredTrait)) {
                    children.add(
                            visit(physicalChild, DeleteKindTrait.deleteOnKeyOrNone(inputModifyKindSet))
                                    .or(
                                            () ->
                                                    visit(
                                                            physicalChild,
                                                            DeleteKindTrait.fullDeleteOrNone(
                                                                    inputModifyKindSet))));
                } else {
                    children.add(
                            visit(physicalChild, DeleteKindTrait.fullDeleteOrNone(inputModifyKindSet)));
                }
            }
            if (children.stream().anyMatch(c -> !c.isPresent())) {
                return Optional.empty();
            }
            List<StreamPhysicalRel> childRels = present(children);
            if (childRels.stream().anyMatch(r -> getDeleteKind(r) == DeleteKind.DELETE_BY_KEY)) {
                return createNewNode(
                        join, Optional.of(childRels), DeleteKindTrait.deleteOnKeyOrNone(getModifyKindSet(rel)));
            } else {
                return createNewNode(
                        join, Optional.of(childRels), DeleteKindTrait.fullDeleteOrNone(getModifyKindSet(rel)));
            }
        } else if (rel instanceof StreamPhysicalCalcBase) {
            // if the condition is applied on the upsert key, we can emit whatever the requiredTrait
            // is, because we will filter all records based on the condition that applies to that key
            StreamPhysicalCalcBase calc = (StreamPhysicalCalcBase) rel;
            if (DeleteKindTrait.DELETE_BY_KEY().equals(requiredTrait)
                    && isNonUpsertKeyCondition(calc)) {
                return Optional.empty();
            }
            // otherwise, forward DeleteKind requirement
            Optional<List<StreamPhysicalRel>> children = visitChildren(rel, requiredTrait);
            if (!children.isPresent()) {
                return Optional.empty();
            }
            DeleteKindTrait childTrait =
                    children.get().get(0).getTraitSet().getTrait(DeleteKindTraitDef.INSTANCE());
            return createNewNode(rel, children, childTrait);
        } else if (rel instanceof StreamPhysicalExchange
                || rel instanceof StreamPhysicalExpand
                || rel instanceof StreamPhysicalMiniBatchAssigner
                || rel instanceof StreamPhysicalDropUpdateBefore) {
            // transparent forward requiredTrait to children
            Optional<List<StreamPhysicalRel>> children = visitChildren(rel, requiredTrait);
            if (!children.isPresent()) {
                return Optional.empty();
            }
            DeleteKindTrait childTrait =
                    children.get().get(0).getTraitSet().getTrait(DeleteKindTraitDef.INSTANCE());
            return createNewNode(rel, children, childTrait);
        } else if (rel instanceof StreamPhysicalUnion) {
            StreamPhysicalUnion union = (StreamPhysicalUnion) rel;
            List<Optional<StreamPhysicalRel>> children = new ArrayList<>();
            for (RelNode childNode : union.getInputs()) {
                StreamPhysicalRel child = (StreamPhysicalRel) childNode;
                ModifyKindSet childModifyKindSet = getModifyKindSet(child);
                DeleteKindTrait requiredChildTrait =
                        !childModifyKindSet.contains(ModifyKind.DELETE)
                                ? DeleteKindTrait.NONE()
                                : requiredTrait;
                children.add(visit(child, requiredChildTrait));
            }
            if (children.stream().anyMatch(c -> !c.isPresent())) {
                return Optional.empty();
            }
            List<StreamPhysicalRel> childRels = present(children);
            List<DeleteKindTrait> deleteKinds = new ArrayList<>();
            for (StreamPhysicalRel child : childRels) {
                deleteKinds.add(child.getTraitSet().getTrait(DeleteKindTraitDef.INSTANCE()));
            }
            // union can just forward changes, can't actively satisfy to another changelog mode
            final DeleteKindTrait providedTrait;
            if (deleteKinds.stream().allMatch(k -> DeleteKindTrait.NONE().equals(k))) {
                // if all the children is NONE, union is NONE
                providedTrait = DeleteKindTrait.NONE();
            } else {
                // otherwise, merge delete kinds.
                DeleteKind merged = null;
                for (DeleteKindTrait deleteKindTrait : deleteKinds) {
                    DeleteKind deleteKind = deleteKindTrait.deleteKind();
                    merged = merged == null ? deleteKind : mergeDeleteKind(merged, deleteKind);
                }
                providedTrait = new DeleteKindTrait(merged);
            }
            return createNewNode(union, Optional.of(childRels), providedTrait);
        } else if (rel instanceof StreamPhysicalChangelogNormalize) {
            StreamPhysicalChangelogNormalize normalize = (StreamPhysicalChangelogNormalize) rel;
            // if
            // 1. we don't need to produce UPDATE_BEFORE,
            // 2. children can satisfy the required delete trait,
            // 3. the normalize doesn't have filter condition which we'd lose,
            // 4. we don't use metadata columns
            // we can skip ChangelogNormalize
            if (!ChangelogNormalizeRequirementResolver.isRequired(normalize)) {
                Optional<List<StreamPhysicalRel>> children = visitChildren(normalize, requiredTrait);
                if (children.isPresent()) {
                    StreamPhysicalRel first = children.get().get(0);
                    RelNode input =
                            first instanceof StreamPhysicalExchange
                                    ? ((StreamPhysicalExchange) first).getInput()
                                    : normalize.getInput();
                    return Optional.of((StreamPhysicalRel) input);
                }
            }
            ModifyKindSet childModifyKindTrait = getModifyKindSet(rel.getInput(0));

            // prefer delete by key, but accept both
            Optional<List<StreamPhysicalRel>> children =
                    visitChildren(normalize, DeleteKindTrait.deleteOnKeyOrNone(childModifyKindTrait))
                            .or(
                                    () ->
                                            visitChildren(
                                                    normalize,
                                                    DeleteKindTrait.fullDeleteOrNone(
                                                            childModifyKindTrait)));

            // changelog normalize produces full deletes
            return createNewNode(
                    rel, children, DeleteKindTrait.fullDeleteOrNone(getModifyKindSet(rel)));
        } else if (rel instanceof StreamPhysicalTableSourceScan) {
            // currently only support BEFORE_AND_AFTER if source produces updates
            StreamPhysicalTableSourceScan ts = (StreamPhysicalTableSourceScan) rel;
            DeleteKindTrait providedTrait =
                    DeleteKindTrait.fromChangelogMode(ts.tableSource().getChangelogMode());
            return createNewNode(rel, Optional.of(Collections.emptyList()), providedTrait);
        } else if (rel instanceof StreamPhysicalDataStreamScan
                || rel instanceof StreamPhysicalLegacyTableSourceScan
                || rel instanceof StreamPhysicalValues) {
            return createNewNode(rel, Optional.of(Collections.emptyList()), DeleteKindTrait.NONE());
        } else if (rel instanceof StreamPhysicalIntermediateTableScan) {
            return createNewNode(
                    rel,
                    Optional.of(Collections.emptyList()),
                    DeleteKindTrait.fullDeleteOrNone(getModifyKindSet(rel)));
        } else if (rel instanceof StreamPhysicalMultiJoin) {
            StreamPhysicalMultiJoin multiJoin = (StreamPhysicalMultiJoin) rel;
            List<Optional<StreamPhysicalRel>> children = new ArrayList<>();
            for (int childOrdinal = 0; childOrdinal < multiJoin.getInputs().size(); childOrdinal++) {
                final StreamPhysicalRel physicalChild =
                        (StreamPhysicalRel) multiJoin.getInput(childOrdinal);
                boolean supportsDeleteByKey =
                        multiJoin.inputUniqueKeyContainsCommonJoinKey(childOrdinal);
                final ModifyKindSet inputModifyKindSet = getModifyKindSet(physicalChild);
                if (supportsDeleteByKey && DeleteKindTrait.DELETE_BY_KEY().equals(requiredTrait)) {
                    children.add(
                            visit(physicalChild, DeleteKindTrait.deleteOnKeyOrNone(inputModifyKindSet))
                                    .or(
                                            () ->
                                                    visit(
                                                            physicalChild,
                                                            DeleteKindTrait.fullDeleteOrNone(
                                                                    inputModifyKindSet))));
                } else {
                    children.add(
                            visit(physicalChild, DeleteKindTrait.fullDeleteOrNone(inputModifyKindSet)));
                }
            }
            if (children.stream().anyMatch(c -> !c.isPresent())) {
                return Optional.empty();
            }
            List<StreamPhysicalRel> childRels = present(children);
            if (childRels.stream().anyMatch(r -> getDeleteKind(r) == DeleteKind.DELETE_BY_KEY)) {
                return createNewNode(
                        multiJoin,
                        Optional.of(childRels),
                        DeleteKindTrait.deleteOnKeyOrNone(getModifyKindSet(rel)));
            } else {
                return createNewNode(
                        multiJoin,
                        Optional.of(childRels),
                        DeleteKindTrait.fullDeleteOrNone(getModifyKindSet(rel)));
            }
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported visit for " + rel.getClass().getSimpleName());
        }
    }

    /**
     * Operators that, if not explicitly supported, require full deletes when there are updates. The
     * grouping mirrors the original Scala pattern match and must be checked before the more
     * specific node types below.
     */
    private static boolean requiresFullDeleteIfUpdates(StreamPhysicalRel rel) {
        return rel instanceof StreamPhysicalGroupAggregate
                || rel instanceof StreamPhysicalGroupTableAggregate
                || rel instanceof StreamPhysicalLimit
                || rel instanceof StreamPhysicalPythonGroupAggregate
                || rel instanceof StreamPhysicalPythonGroupTableAggregate
                || rel instanceof StreamPhysicalGroupWindowAggregateBase
                || rel instanceof StreamPhysicalWindowAggregate
                || rel instanceof StreamPhysicalSort
                || rel instanceof StreamPhysicalRank
                || rel instanceof StreamPhysicalSortLimit
                || rel instanceof StreamPhysicalTemporalJoin
                || rel instanceof StreamPhysicalCorrelateBase
                || rel instanceof StreamPhysicalLookupJoin
                || rel instanceof StreamPhysicalWatermarkAssigner
                || rel instanceof StreamPhysicalWindowTableFunction
                || rel instanceof StreamPhysicalWindowRank
                || rel instanceof StreamPhysicalWindowDeduplicate
                || rel instanceof StreamPhysicalTemporalSort
                || rel instanceof StreamPhysicalMatch
                || rel instanceof StreamPhysicalOverAggregate
                || rel instanceof StreamPhysicalIntervalJoin
                || rel instanceof StreamPhysicalPythonOverAggregate
                || rel instanceof StreamPhysicalWindowJoin
                || rel instanceof StreamPhysicalMLPredictTableFunction
                || rel instanceof StreamPhysicalVectorSearchTableFunction;
    }

    private static DeleteKind mergeDeleteKind(DeleteKind left, DeleteKind right) {
        if (left == DeleteKind.NONE) {
            return right;
        }
        if (right == DeleteKind.NONE) {
            return left;
        }
        if (left == right) {
            return left;
        }
        // if any of the union input produces DELETE_BY_KEY, the union produces delete by key
        return DeleteKind.DELETE_BY_KEY;
    }

    private Optional<List<StreamPhysicalRel>> visitChildren(
            StreamPhysicalRel parent, DeleteKindTrait requiredChildrenTrait) {
        List<StreamPhysicalRel> newChildren = new ArrayList<>();
        for (RelNode childNode : parent.getInputs()) {
            Optional<StreamPhysicalRel> newChild =
                    visit((StreamPhysicalRel) childNode, requiredChildrenTrait);
            if (!newChild.isPresent()) {
                // return empty if one of the children can't satisfy
                return Optional.empty();
            }
            DeleteKindTrait providedTrait =
                    newChild.get().getTraitSet().getTrait(DeleteKindTraitDef.INSTANCE());
            if (!providedTrait.satisfies(requiredChildrenTrait)) {
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
            DeleteKindTrait providedDeleteTrait) {
        if (!childrenOption.isPresent()) {
            return Optional.empty();
        }
        List<StreamPhysicalRel> children = childrenOption.get();
        ModifyKindSetTrait modifyKindSetTrait =
                node.getTraitSet().getTrait(ModifyKindSetTraitDef.INSTANCE());
        String nodeDescription = node.getRelDetailedDescription();
        final boolean isDeleteKindValid;
        switch (providedDeleteTrait.deleteKind()) {
            case NONE:
                isDeleteKindValid = !modifyKindSetTrait.modifyKindSet().contains(ModifyKind.DELETE);
                break;
            case DELETE_BY_KEY:
            case FULL_DELETE:
                isDeleteKindValid = modifyKindSetTrait.modifyKindSet().contains(ModifyKind.DELETE);
                break;
            default:
                isDeleteKindValid = false;
        }
        if (!isDeleteKindValid) {
            throw new TableException(
                    "DeleteKindTrait "
                            + providedDeleteTrait
                            + " conflicts with ModifyKindSetTrait "
                            + modifyKindSetTrait
                            + ". This is a bug in planner, please file an issue. \n"
                            + "Current node is "
                            + nodeDescription
                            + ".");
        }
        RelTraitSet newTraitSet = node.getTraitSet().plus(providedDeleteTrait);
        return Optional.of((StreamPhysicalRel) node.copy(newTraitSet, new ArrayList<RelNode>(children)));
    }

    private Optional<StreamPhysicalRel> visitSink(
            StreamPhysicalRel sink, List<DeleteKindTrait> sinkRequiredTraits) {
        List<List<StreamPhysicalRel>> satisfiedChildren = new ArrayList<>();
        for (DeleteKindTrait requiredTrait : sinkRequiredTraits) {
            visitChildren(sink, requiredTrait).ifPresent(satisfiedChildren::add);
        }
        if (satisfiedChildren.isEmpty()) {
            return Optional.empty();
        }
        RelTraitSet sinkTrait = sink.getTraitSet().plus(DeleteKindTrait.NONE());
        return Optional.of(
                (StreamPhysicalRel)
                        sink.copy(sinkTrait, new ArrayList<RelNode>(satisfiedChildren.get(0))));
    }

    /**
     * Infer sink required traits by the sink node and its input. Sink required traits is based on
     * the sink node's changelog mode, the only exception is when sink's pk(s) not exactly the same
     * as the changeLogUpsertKeys and sink's changelog mode is DELETE_BY_KEY.
     */
    private List<DeleteKindTrait> inferSinkRequiredTraits(StreamPhysicalSink sink) {
        ModifyKindSet childModifyKindSet = getModifyKindSet(sink.getInput());
        ChangelogMode sinkChangelogMode =
                sink.tableSink().getChangelogMode(childModifyKindSet.toDefaultChangelogMode());

        DeleteKindTrait sinkDeleteTrait = DeleteKindTrait.fromChangelogMode(sinkChangelogMode);

        DeleteKindTrait fullDelete = DeleteKindTrait.fullDeleteOrNone(childModifyKindSet);
        if (sinkDeleteTrait.equals(DeleteKindTrait.DELETE_BY_KEY())) {
            if (areUpsertKeysDifferentFromPk(sink)) {
                return Collections.singletonList(fullDelete);
            } else {
                return Arrays.asList(sinkDeleteTrait, fullDelete);
            }
        } else {
            return Collections.singletonList(fullDelete);
        }
    }

    private boolean areUpsertKeysDifferentFromPk(StreamPhysicalSink sink) {
        // if sink's pk(s) are not exactly match input changeLogUpsertKeys then it will fallback
        // to beforeAndAfter mode for the correctness
        boolean upsertKeyDifferentFromPk = false;
        int[] sinkDefinedPks = sink.contextResolvedTable().getResolvedSchema().getPrimaryKeyIndexes();

        if (sinkDefinedPks.length > 0) {
            ImmutableBitSet sinkPks = ImmutableBitSet.of(sinkDefinedPks);
            FlinkRelMetadataQuery fmq =
                    FlinkRelMetadataQuery.reuseOrCreate(sink.getCluster().getMetadataQuery());
            Set<ImmutableBitSet> changeLogUpsertKeys = fmq.getUpsertKeys(sink.getInput());
            // if input is UA only, primary key != upsert key (upsert key can be null) we should
            // fallback to beforeAndAfter.
            // Notice: even sink pk(s) contains input upsert key we cannot optimize to UA only,
            // this differs from batch job's unique key inference
            if (changeLogUpsertKeys == null
                    || changeLogUpsertKeys.stream().noneMatch(k -> k.equals(sinkPks))) {
                upsertKeyDifferentFromPk = true;
            }
        }
        return upsertKeyDifferentFromPk;
    }

    private static List<StreamPhysicalRel> present(List<Optional<StreamPhysicalRel>> options) {
        List<StreamPhysicalRel> result = new ArrayList<>(options.size());
        for (Optional<StreamPhysicalRel> option : options) {
            result.add(option.get());
        }
        return result;
    }
}
