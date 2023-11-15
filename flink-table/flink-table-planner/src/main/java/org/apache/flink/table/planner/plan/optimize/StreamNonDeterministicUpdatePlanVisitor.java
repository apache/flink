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

package org.apache.flink.table.planner.plan.optimize;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalcBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMatch;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSortLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalValues;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An inner visitor to validate if there's any NDU problems which may cause wrong result and try to
 * rewrite lookup join node with materialization (to eliminate the non-deterministic result
 * generated by lookup join node only).
 *
 * <p>The visitor will try to satisfy the required determinism(represent by ImmutableBitSet) from
 * root. The transmission rule of required determinism:
 *
 * <p>0. all required determinism is under the precondition: input has updates, that is say no
 * update determinism will be passed to an insert only stream
 *
 * <p>1. the initial required determinism to the root node(e.g., sink node) was none
 *
 * <p>2. for a relNode, it will process on two aspects: - can satisfy non-empty required determinism
 * - actively requires determinism from input by self requirements(e.g., stateful node works on
 * retract by row mode)
 *
 * <pre>{@code
 * Rel3
 *  | require input
 *  v
 * Rel2 {1. satisfy Rel3's requirement 2. append new requirement to input Rel1}
 *  | require input
 *  v
 * Rel1
 * }</pre>
 *
 * <p>the requiredDeterminism passed to input will exclude columns which were in upsertKey, e.g.,
 *
 * <pre>{@code
 * Sink {pk=(c3)} requiredDeterminism=(c3)
 *   | passed requiredDeterminism={}
 *  GroupAgg{group by c3, day} append requiredDeterminism=(c3, day)
 *   | passed requiredDeterminism=(c3, day)
 * Project{select c1,c2,DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') day,...} [x] can not satisfy
 *   |
 * Deduplicate{keep last row, dedup on c1,c2}
 *   |
 *  Scan
 * }</pre>
 *
 * <p>3. for a sink node, it will require key columns' determinism when primary key is defined or
 * require all columns' determinism when no primary key is defined
 *
 * <p>4. for a cdc source node(which will generate updates), the metadata columns are treated as
 * non-deterministic.
 */
public class StreamNonDeterministicUpdatePlanVisitor {
    private static final ImmutableBitSet NO_REQUIRED_DETERMINISM = ImmutableBitSet.of();

    private static final String NON_DETERMINISTIC_CONDITION_ERROR_MSG_TEMPLATE =
            "There exists non deterministic function: '%s' in condition: '%s' which may cause wrong result in update pipeline.";

    public StreamPhysicalRel visit(final StreamPhysicalRel rel) {
        return visit(rel, NO_REQUIRED_DETERMINISM);
    }

    public StreamPhysicalRel visit(
            final StreamPhysicalRel rel, final ImmutableBitSet requireDeterminism) {
        if (rel instanceof StreamPhysicalSink) {
            if (inputInsertOnly(rel)) {
                // for append stream, not care about NDU
                return transmitDeterminismRequirement(rel, NO_REQUIRED_DETERMINISM);
            } else {
                // for update streaming, when
                // 1. sink with pk:
                // upsert sink, update by pk, ideally pk == input.upsertKey,
                // (otherwise upsertMaterialize will handle it)

                // 1.1 input.upsertKey nonEmpty -> not care about NDU
                // 1.2 input.upsertKey isEmpty -> retract by complete row, must not contain NDU

                // once sink's requirement on pk was satisfied, no further request will be transited
                // only when new requirement generated at stateful node which input has update
                // (e.g., grouping keys)

                // 2. sink without pk:
                // retract sink, retract by complete row (all input columns should be deterministic)
                // whether input.upsertKey is empty or not, must not contain NDU
                StreamPhysicalSink sink = (StreamPhysicalSink) rel;
                int[] primaryKey =
                        sink.contextResolvedTable().getResolvedSchema().getPrimaryKeyIndexes();
                ImmutableBitSet requireInputDeterminism;
                if (sink.upsertMaterialize() || null == primaryKey || primaryKey.length == 0) {
                    // SinkUpsertMaterializer only support no upsertKey mode, it says all input
                    // columns should be deterministic (same as no primary key defined on sink)
                    // TODO should optimize it after SinkUpsertMaterializer support upsertKey
                    // FLINK-28569.
                    requireInputDeterminism =
                            ImmutableBitSet.range(sink.getInput().getRowType().getFieldCount());
                } else {
                    requireInputDeterminism = ImmutableBitSet.of(primaryKey);
                }
                return transmitDeterminismRequirement(sink, requireInputDeterminism);
            }
        } else if (rel instanceof StreamPhysicalLegacySink<?>) {
            if (inputInsertOnly(rel)) {
                // for append stream, not care about NDU
                return transmitDeterminismRequirement(rel, NO_REQUIRED_DETERMINISM);
            } else {
                StreamPhysicalLegacySink<?> sink = (StreamPhysicalLegacySink<?>) rel;
                TableSchema tableSchema = sink.sink().getTableSchema();
                Optional<UniqueConstraint> primaryKey = tableSchema.getPrimaryKey();
                List<String> columns = Arrays.asList(tableSchema.getFieldNames());
                // SinkUpsertMaterializer does not support legacy sink
                ImmutableBitSet requireInputDeterminism;
                if (primaryKey.isPresent()) {
                    requireInputDeterminism =
                            ImmutableBitSet.of(
                                    primaryKey.get().getColumns().stream()
                                            .map(columns::indexOf)
                                            .collect(Collectors.toList()));
                } else {
                    requireInputDeterminism = ImmutableBitSet.range(columns.size());
                }
                return transmitDeterminismRequirement(rel, requireInputDeterminism);
            }
        } else if (rel instanceof StreamPhysicalCalcBase) {
            if (inputInsertOnly(rel) || requireDeterminism.isEmpty()) {
                // for append stream, not care about NDU
                return transmitDeterminismRequirement(rel, NO_REQUIRED_DETERMINISM);
            } else {
                // if input has updates, any non-deterministic conditions are not acceptable, also
                // requireDeterminism should be satisfied.
                StreamPhysicalCalcBase calc = (StreamPhysicalCalcBase) rel;
                checkNonDeterministicRexProgram(requireDeterminism, calc.getProgram(), calc);

                // evaluate required determinism from input
                List<RexNode> projects =
                        calc.getProgram().getProjectList().stream()
                                .map(expr -> calc.getProgram().expandLocalRef(expr))
                                .collect(Collectors.toList());
                Map<Integer, List<Integer>> outFromSourcePos = extractSourceMapping(projects);
                List<Integer> conv2Inputs =
                        requireDeterminism.toList().stream()
                                .map(
                                        out ->
                                                Optional.ofNullable(outFromSourcePos.get(out))
                                                        .orElseThrow(
                                                                () ->
                                                                        new TableException(
                                                                                String.format(
                                                                                        "Invalid pos:%d over projection:%s",
                                                                                        out,
                                                                                        calc
                                                                                                .getProgram()))))
                                .flatMap(Collection::stream)
                                .filter(index -> index != -1)
                                .distinct()
                                .collect(Collectors.toList());

                return transmitDeterminismRequirement(calc, ImmutableBitSet.of(conv2Inputs));
            }
        } else if (rel instanceof StreamPhysicalCorrelateBase) {
            if (inputInsertOnly(rel) || requireDeterminism.isEmpty()) {
                return transmitDeterminismRequirement(rel, NO_REQUIRED_DETERMINISM);
            } else {
                // check if non-deterministic condition (may exist after FLINK-7865 was fixed).
                StreamPhysicalCorrelateBase correlate = (StreamPhysicalCorrelateBase) rel;
                if (correlate.condition().isDefined()) {
                    RexNode rexNode = correlate.condition().get();
                    checkNonDeterministicCondition(rexNode, correlate);
                }
                // check if it is a non-deterministic function
                int leftFieldCnt = correlate.inputRel().getRowType().getFieldCount();
                Optional<String> ndCall =
                        FlinkRexUtil.getNonDeterministicCallName(correlate.scan().getCall());
                if (ndCall.isPresent()) {
                    // all columns from table function scan cannot satisfy the required determinism
                    List<Integer> unsatisfiedColumns =
                            requireDeterminism.toList().stream()
                                    .filter(index -> index >= leftFieldCnt)
                                    .collect(Collectors.toList());
                    if (!unsatisfiedColumns.isEmpty()) {
                        throwNonDeterministicColumnsError(
                                unsatisfiedColumns,
                                correlate.getRowType(),
                                correlate,
                                null,
                                ndCall);
                    }
                }
                // evaluate required determinism from input
                List<Integer> fromLeft =
                        requireDeterminism.toList().stream()
                                .filter(index -> index < leftFieldCnt)
                                .collect(Collectors.toList());
                if (fromLeft.isEmpty()) {
                    return transmitDeterminismRequirement(correlate, NO_REQUIRED_DETERMINISM);
                }
                return transmitDeterminismRequirement(correlate, ImmutableBitSet.of(fromLeft));
            }

        } else if (rel instanceof StreamPhysicalLookupJoin) {
            if (inputInsertOnly(rel) || requireDeterminism.isEmpty()) {
                return transmitDeterminismRequirement(rel, NO_REQUIRED_DETERMINISM);
            } else {
                /**
                 * if input has updates, the lookup join may produce non-deterministic result itself
                 * due to backed lookup source which data may change over time, we can try to
                 * eliminate this non-determinism by adding materialization to the join operator,
                 * but still exists non-determinism we cannot solve: 1. join condition 2. the inner
                 * calc in lookJoin.
                 */
                StreamPhysicalLookupJoin lookupJoin = (StreamPhysicalLookupJoin) rel;

                // required determinism cannot be satisfied even upsert materialize was enabled if:
                // 1. remaining join condition contains non-deterministic call
                JavaScalaConversionUtil.toJava(lookupJoin.finalPreFilterCondition())
                        .ifPresent(cond -> checkNonDeterministicCondition(cond, lookupJoin));
                JavaScalaConversionUtil.toJava(lookupJoin.finalRemainingCondition())
                        .ifPresent(cond -> checkNonDeterministicCondition(cond, lookupJoin));

                // 2. inner calc in lookJoin contains either non-deterministic condition or calls
                JavaScalaConversionUtil.toJava(lookupJoin.calcOnTemporalTable())
                        .ifPresent(
                                calc ->
                                        checkNonDeterministicRexProgram(
                                                requireDeterminism, calc, lookupJoin));

                // Try to resolve non-determinism by adding materialization which can eliminate
                // non-determinism produced by lookup join via an evolving source.
                int leftFieldCnt = lookupJoin.getInput().getRowType().getFieldCount();
                List<Integer> requireRight =
                        requireDeterminism.toList().stream()
                                .filter(index -> index >= leftFieldCnt)
                                .collect(Collectors.toList());
                boolean omitUpsertMaterialize = false;
                // two optimizations: 1. no fields from lookup source was required 2. lookup key
                // contains pk and no requirement on other fields we can omit materialization,
                // otherwise upsert materialize can not be omitted.
                if (requireRight.isEmpty()) {
                    omitUpsertMaterialize = true;
                } else {
                    int[] outputPkIdx = lookupJoin.getOutputIndexesOfTemporalTablePrimaryKey();
                    ImmutableBitSet outputPkBitSet = ImmutableBitSet.of(outputPkIdx);
                    // outputPkIdx need to used so not using #lookupKeyContainsPrimaryKey directly.
                    omitUpsertMaterialize =
                            Arrays.stream(outputPkIdx)
                                            .allMatch(
                                                    index ->
                                                            lookupJoin
                                                                    .allLookupKeys()
                                                                    .contains(index))
                                    && requireRight.stream().allMatch(outputPkBitSet::get);
                }
                List<Integer> requireLeft =
                        requireDeterminism.toList().stream()
                                .filter(index -> index < leftFieldCnt)
                                .collect(Collectors.toList());

                if (omitUpsertMaterialize) {
                    return transmitDeterminismRequirement(
                            lookupJoin, ImmutableBitSet.of(requireLeft));
                } else {
                    // enable materialize for lookup join
                    return transmitDeterminismRequirement(
                            lookupJoin.copy(true), ImmutableBitSet.of(requireLeft));
                }
            }
        } else if (rel instanceof StreamPhysicalTableSourceScan) {
            // tableScan has no input, so only check metadata from cdc source
            if (!requireDeterminism.isEmpty()) {
                StreamPhysicalTableSourceScan tableScan = (StreamPhysicalTableSourceScan) rel;
                boolean insertOnly =
                        tableScan.tableSource().getChangelogMode().containsOnly(RowKind.INSERT);
                boolean supportsReadingMetadata =
                        tableScan.tableSource() instanceof SupportsReadingMetadata;
                if (!insertOnly && supportsReadingMetadata) {
                    TableSourceTable sourceTable =
                            tableScan.getTable().unwrap(TableSourceTable.class);
                    // check if requireDeterminism contains metadata column
                    List<Column.MetadataColumn> metadataColumns =
                            DynamicSourceUtils.extractMetadataColumns(
                                    sourceTable.contextResolvedTable().getResolvedSchema());
                    Set<String> metaColumnSet =
                            metadataColumns.stream()
                                    .map(Column::getName)
                                    .collect(Collectors.toSet());
                    List<String> columns = tableScan.getRowType().getFieldNames();
                    List<String> metadataCauseErr = new ArrayList<>();
                    for (int index = 0; index < columns.size(); index++) {
                        String column = columns.get(index);
                        if (metaColumnSet.contains(column) && requireDeterminism.get(index)) {
                            metadataCauseErr.add(column);
                        }
                    }
                    if (!metadataCauseErr.isEmpty()) {
                        StringBuilder errorMsg = new StringBuilder();
                        errorMsg.append("The metadata column(s): '")
                                .append(String.join(", ", metadataCauseErr.toArray(new String[0])))
                                .append("' in cdc source may cause wrong result or error on")
                                .append(" downstream operators, please consider removing these")
                                .append(" columns or use a non-cdc source that only has insert")
                                .append(" messages.\nsource node:\n")
                                .append(
                                        FlinkRelOptUtil.toString(
                                                tableScan,
                                                SqlExplainLevel.DIGEST_ATTRIBUTES,
                                                false,
                                                true,
                                                false,
                                                true,
                                                false));
                        throw new TableException(errorMsg.toString());
                    }
                }
            }
            return rel;
        } else if (rel instanceof StreamPhysicalLegacyTableSourceScan
                || rel instanceof StreamPhysicalDataStreamScan
                || rel instanceof StreamPhysicalValues) {
            // not cdc source, end visit
            return rel;
        } else if (rel instanceof StreamPhysicalGroupAggregateBase) {
            // output row type = grouping keys + aggCalls
            StreamPhysicalGroupAggregateBase groupAgg = (StreamPhysicalGroupAggregateBase) rel;
            if (inputInsertOnly(groupAgg)) {
                // no further requirement to input, only check if it can satisfy the
                // requiredDeterminism
                if (!requireDeterminism.isEmpty()) {
                    checkUnsatisfiedDeterminism(
                            requireDeterminism,
                            groupAgg.grouping().length,
                            // TODO remove this conversion when scala-free was total done.
                            scala.collection.JavaConverters.seqAsJavaList(groupAgg.aggCalls()),
                            groupAgg.getRowType(),
                            groupAgg);
                }
                return transmitDeterminismRequirement(groupAgg, NO_REQUIRED_DETERMINISM);
            } else {
                // agg works under retract mode if input is not insert only, and requires all input
                // columns be deterministic
                return transmitDeterminismRequirement(
                        groupAgg,
                        ImmutableBitSet.range(groupAgg.getInput().getRowType().getFieldCount()));
            }
        } else if (rel instanceof StreamPhysicalWindowAggregateBase) {
            // output row type = grouping keys + aggCalls + windowProperties
            // same logic with 'groupAgg' but they have no common parent
            StreamPhysicalWindowAggregateBase windowAgg = (StreamPhysicalWindowAggregateBase) rel;
            if (inputInsertOnly(windowAgg)) {
                // no further requirement to input, only check if it can satisfy the
                // requiredDeterminism
                if (!requireDeterminism.isEmpty()) {
                    checkUnsatisfiedDeterminism(
                            requireDeterminism,
                            windowAgg.grouping().length,
                            // TODO remove this conversion when scala-free was total done.
                            scala.collection.JavaConverters.seqAsJavaList(windowAgg.aggCalls()),
                            windowAgg.getRowType(),
                            windowAgg);
                }
                return transmitDeterminismRequirement(windowAgg, NO_REQUIRED_DETERMINISM);
            } else {
                // agg works under retract mode if input is not insert only, and requires all input
                // columns be deterministic
                return transmitDeterminismRequirement(
                        windowAgg,
                        ImmutableBitSet.range(windowAgg.getInput().getRowType().getFieldCount()));
            }
        } else if (rel instanceof StreamPhysicalExpand) {
            // Expand is an internal operator only for plan rewriting currently, so only remove the
            // expandIdIndex from requireDeterminism. We also skip checking if input has updates due
            // to this is a non-stateful node which never changes the changelog mode.
            StreamPhysicalExpand expand = (StreamPhysicalExpand) rel;
            return transmitDeterminismRequirement(
                    expand, requireDeterminism.except(ImmutableBitSet.of(expand.expandIdIndex())));
        } else if (rel instanceof CommonPhysicalJoin) {
            // output row type = left row type + right row type
            CommonPhysicalJoin join = (CommonPhysicalJoin) rel;
            StreamPhysicalRel leftRel = (StreamPhysicalRel) join.getLeft();
            StreamPhysicalRel rightRel = (StreamPhysicalRel) join.getRight();
            boolean leftInputHasUpdate = !inputInsertOnly(leftRel);
            boolean rightInputHasUpdate = !inputInsertOnly(rightRel);
            boolean innerOrSemi =
                    join.joinSpec().getJoinType() == FlinkJoinType.INNER
                            || join.joinSpec().getJoinType() == FlinkJoinType.SEMI;
            /**
             * we do not distinguish the time attribute condition in interval/temporal join from
             * regular/window join here because: rowtime field always from source, proctime is not
             * limited (from source), when proctime appended to an update row without upsertKey then
             * result may goes wrong, in such a case proctime( was materialized as
             * PROCTIME_MATERIALIZE(PROCTIME())) is equal to a normal dynamic temporal function and
             * will be validated in calc node.
             */
            Optional<String> ndCall = FlinkRexUtil.getNonDeterministicCallName(join.getCondition());
            if ((leftInputHasUpdate || rightInputHasUpdate || !innerOrSemi) && ndCall.isPresent()) {
                // when output has update, the join condition cannot be non-deterministic:
                // 1. input has update -> output has update
                // 2. input insert only and is not innerOrSemi join -> output has update
                throwNonDeterministicConditionError(
                        ndCall.get(), join.getCondition(), (StreamPhysicalRel) join);
            }
            int leftFieldCnt = leftRel.getRowType().getFieldCount();
            StreamPhysicalRel newLeft =
                    visitJoinChild(
                            requireDeterminism,
                            leftRel,
                            leftInputHasUpdate,
                            leftFieldCnt,
                            true,
                            join.joinSpec().getLeftKeys(),
                            // TODO remove this conversion when scala-free was total done.
                            scala.collection.JavaConverters.seqAsJavaList(
                                    join.getUpsertKeys(leftRel, join.joinSpec().getLeftKeys())));
            StreamPhysicalRel newRight =
                    visitJoinChild(
                            requireDeterminism,
                            rightRel,
                            rightInputHasUpdate,
                            leftFieldCnt,
                            false,
                            join.joinSpec().getRightKeys(),
                            // TODO remove this conversion when scala-free was total done.
                            scala.collection.JavaConverters.seqAsJavaList(
                                    join.getUpsertKeys(rightRel, join.joinSpec().getRightKeys())));

            return (StreamPhysicalRel)
                    join.copy(
                            join.getTraitSet(),
                            join.getCondition(),
                            newLeft,
                            newRight,
                            join.getJoinType(),
                            join.isSemiJoin());

        } else if (rel instanceof StreamPhysicalOverAggregateBase) {
            // output row type = input row type + overAgg outputs
            StreamPhysicalOverAggregateBase overAgg = ((StreamPhysicalOverAggregateBase) rel);
            if (inputInsertOnly(overAgg)) {
                // no further requirement to input, only check if the agg outputs can satisfy the
                // requiredDeterminism
                if (!requireDeterminism.isEmpty()) {
                    int inputFieldCnt = overAgg.getInput().getRowType().getFieldCount();
                    OverSpec overSpec = OverAggregateUtil.createOverSpec(overAgg.logicWindow());
                    // add aggCall's input
                    int aggOutputIndex = inputFieldCnt;
                    for (OverSpec.GroupSpec groupSpec : overSpec.getGroups()) {
                        checkUnsatisfiedDeterminism(
                                requireDeterminism,
                                aggOutputIndex,
                                groupSpec.getAggCalls(),
                                overAgg.getRowType(),
                                overAgg);
                        aggOutputIndex += groupSpec.getAggCalls().size();
                    }
                }
                return transmitDeterminismRequirement(overAgg, NO_REQUIRED_DETERMINISM);
            } else {
                // OverAgg does not support input with updates currently, so this branch will not be
                // reached for now.

                // We should append partition keys and order key to requireDeterminism
                return transmitDeterminismRequirement(
                        overAgg, mappingRequireDeterminismToInput(requireDeterminism, overAgg));
            }
        } else if (rel instanceof StreamPhysicalRank) {
            // if outputRankNumber:  output row type = input row type + rank number type
            // else keeps the same as input
            StreamPhysicalRank rank = (StreamPhysicalRank) rel;
            if (inputInsertOnly(rank)) {
                // rank output is deterministic when input is insert only, so required determinism
                // always be satisfied here.
                return transmitDeterminismRequirement(rank, NO_REQUIRED_DETERMINISM);
            } else {
                int inputFieldCnt = rank.getInput().getRowType().getFieldCount();
                if (rank.rankStrategy() instanceof RankProcessStrategy.UpdateFastStrategy) {
                    // in update fast mode, pass required determinism excludes partition keys and
                    // order key
                    ImmutableBitSet.Builder bitSetBuilder = ImmutableBitSet.builder();
                    rank.partitionKey().toList().forEach(bitSetBuilder::set);
                    rank.orderKey().getKeys().toIntegerList().forEach(bitSetBuilder::set);
                    if (rank.outputRankNumber()) {
                        // exclude last column
                        bitSetBuilder.set(inputFieldCnt);
                    }
                    return transmitDeterminismRequirement(
                            rank, requireDeterminism.except(bitSetBuilder.build()));
                } else if (rank.rankStrategy() instanceof RankProcessStrategy.RetractStrategy) {
                    // in retract mode then require all input columns be deterministic
                    return transmitDeterminismRequirement(
                            rank, ImmutableBitSet.range(inputFieldCnt));
                } else {
                    // AppendFastStrategy only applicable for insert only input, so the undefined
                    // strategy is not as expected here.
                    throw new TableException(
                            String.format(
                                    "Can not infer the determinism for unsupported rank strategy: %s, this is a bug, please file an issue.",
                                    rank.rankStrategy()));
                }
            }
        } else if (rel instanceof StreamPhysicalDeduplicate) {
            // output row type same as input and does not change output columns' order
            StreamPhysicalDeduplicate dedup = (StreamPhysicalDeduplicate) rel;
            if (inputInsertOnly(dedup)) {
                // similar to rank, output is deterministic when input is insert only, so required
                // determinism always be satisfied here.
                return transmitDeterminismRequirement(dedup, NO_REQUIRED_DETERMINISM);
            } else {
                // Deduplicate always has unique key currently(exec node has null check and inner
                // state only support data with keys), so only pass the left columns of required
                // determinism to input.
                return transmitDeterminismRequirement(
                        dedup,
                        requireDeterminism.except(ImmutableBitSet.of(dedup.getUniqueKeys())));
            }
        } else if (rel instanceof StreamPhysicalWindowDeduplicate) {
            // output row type same as input and does not change output columns' order
            StreamPhysicalWindowDeduplicate winDedup = (StreamPhysicalWindowDeduplicate) rel;
            if (inputInsertOnly(winDedup)) {
                // similar to rank, output is deterministic when input is insert only, so required
                // determinism always be satisfied here.
                return transmitDeterminismRequirement(winDedup, NO_REQUIRED_DETERMINISM);
            } else {
                // WindowDeduplicate does not support input with updates currently, so this branch
                // will not be reached for now.

                // only append partition keys, no need to process order key because it always comes
                // from window
                return transmitDeterminismRequirement(
                        winDedup,
                        requireDeterminism
                                .clear(winDedup.orderKey())
                                .union(ImmutableBitSet.of(winDedup.partitionKeys())));
            }
        } else if (rel instanceof StreamPhysicalWindowRank) {
            StreamPhysicalWindowRank winRank = (StreamPhysicalWindowRank) rel;
            if (inputInsertOnly(winRank)) {
                // similar to rank, output is deterministic when input is insert only, so required
                // determinism always be satisfied here.
                return transmitDeterminismRequirement(winRank, NO_REQUIRED_DETERMINISM);
            } else {
                // WindowRank does not support input with updates currently, so this branch will not
                // be reached for now.

                // only append partition keys, no need to process order key because it always comes
                // from window
                int inputFieldCnt = winRank.getInput().getRowType().getFieldCount();
                return transmitDeterminismRequirement(
                        winRank,
                        requireDeterminism
                                .intersect(ImmutableBitSet.range(inputFieldCnt))
                                .union(winRank.partitionKey()));
            }
        } else if (rel instanceof StreamPhysicalWindowTableFunction) {
            // output row type = input row type + window attributes
            StreamPhysicalWindowTableFunction winTVF = (StreamPhysicalWindowTableFunction) rel;
            if (inputInsertOnly(winTVF)) {
                return transmitDeterminismRequirement(winTVF, NO_REQUIRED_DETERMINISM);
            } else {
                // pass the left columns of required determinism to input exclude window attributes
                return transmitDeterminismRequirement(
                        winTVF,
                        requireDeterminism.intersect(
                                ImmutableBitSet.range(
                                        winTVF.getInput().getRowType().getFieldCount())));
            }
        } else if (rel instanceof StreamPhysicalChangelogNormalize
                || rel instanceof StreamPhysicalDropUpdateBefore
                || rel instanceof StreamPhysicalMiniBatchAssigner
                || rel instanceof StreamPhysicalUnion
                || rel instanceof StreamPhysicalSort
                || rel instanceof StreamPhysicalLimit
                || rel instanceof StreamPhysicalSortLimit
                || rel instanceof StreamPhysicalTemporalSort
                || rel instanceof StreamPhysicalWatermarkAssigner
                || rel instanceof StreamPhysicalExchange) {
            // transit requireDeterminism transparently
            return transmitDeterminismRequirement(rel, requireDeterminism);
        } else if (rel instanceof StreamPhysicalMatch) {
            StreamPhysicalMatch match = (StreamPhysicalMatch) rel;
            if (inputInsertOnly(match)) {
                // similar to over aggregate, output is insert only when input is insert only, so
                // required determinism always be satisfied here.
                return transmitDeterminismRequirement(match, NO_REQUIRED_DETERMINISM);
            } else {
                // The DEFINE and MEASURES clauses in match-recognize have similar meanings to the
                // WHERE and SELECT clauses in SQL query, we should analyze and transmit the
                // determinism requirement via the RexNodes in these two clauses.
                throw new UnsupportedOperationException(
                        "Unsupported to resolve non-deterministic issue in match-recognize when input has updates.");
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported to visit node %s, please add the visit implementation if it is a newly added stream physical node.",
                            rel.getClass().getSimpleName()));
        }
    }

    // helper methods
    private boolean inputInsertOnly(final StreamPhysicalRel rel) {
        return ChangelogPlanUtils.inputInsertOnly(rel);
    }

    private StreamPhysicalRel transmitDeterminismRequirement(
            final StreamPhysicalRel parent, final ImmutableBitSet requireDeterminism) {
        List<RelNode> newChildren = visitInputs(parent, requireDeterminism);
        return (StreamPhysicalRel) parent.copy(parent.getTraitSet(), newChildren);
    }

    private List<RelNode> visitInputs(
            final StreamPhysicalRel parent, final ImmutableBitSet requireDeterminism) {
        List<RelNode> newChildren = new ArrayList<>();
        for (int index = 0; index < parent.getInputs().size(); index++) {
            StreamPhysicalRel input = (StreamPhysicalRel) parent.getInput(index);
            // unified processing on input upsertKey
            newChildren.add(
                    visit(input, requireDeterminismExcludeUpsertKey(input, requireDeterminism)));
        }
        return newChildren;
    }

    private StreamPhysicalRel visitJoinChild(
            final ImmutableBitSet requireDeterminism,
            final StreamPhysicalRel rel,
            final boolean inputHasUpdate,
            final int leftFieldCnt,
            final boolean isLeft,
            final int[] joinKeys,
            final List<int[]> inputUniqueKeys) {
        JoinInputSideSpec joinInputSideSpec =
                JoinUtil.analyzeJoinInput(
                        ShortcutUtils.unwrapClassLoader(rel),
                        InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(rel.getRowType())),
                        joinKeys,
                        inputUniqueKeys);
        ImmutableBitSet inputRequireDeterminism;
        if (inputHasUpdate) {
            if (joinInputSideSpec.hasUniqueKey() || joinInputSideSpec.joinKeyContainsUniqueKey()) {
                // join hasUniqueKey or joinKeyContainsUniqueKey, then transmit corresponding
                // requirement to input
                if (isLeft) {
                    inputRequireDeterminism =
                            ImmutableBitSet.of(
                                    requireDeterminism.toList().stream()
                                            .filter(index -> index < leftFieldCnt)
                                            .collect(Collectors.toList()));
                } else {
                    inputRequireDeterminism =
                            ImmutableBitSet.of(
                                    requireDeterminism.toList().stream()
                                            .filter(index -> index >= leftFieldCnt)
                                            .map(index -> index - leftFieldCnt)
                                            .collect(Collectors.toList()));
                }
            } else {
                // join need to retract by whole input row
                inputRequireDeterminism = ImmutableBitSet.range(rel.getRowType().getFieldCount());
            }
        } else {
            inputRequireDeterminism = NO_REQUIRED_DETERMINISM;
        }
        return transmitDeterminismRequirement(rel, inputRequireDeterminism);
    }

    /** Extracts the out from source field index mapping of the given projects. */
    private Map<Integer, List<Integer>> extractSourceMapping(final List<RexNode> projects) {
        Map<Integer, List<Integer>> mapOutFromInPos = new HashMap<>();

        for (int index = 0; index < projects.size(); index++) {
            RexNode expr = projects.get(index);
            mapOutFromInPos.put(
                    index,
                    FlinkRexUtil.findAllInputRefs(expr).stream()
                            .mapToInt(RexSlot::getIndex)
                            .boxed()
                            .collect(Collectors.toList()));
        }
        return mapOutFromInPos;
    }

    private void checkNonDeterministicRexProgram(
            final ImmutableBitSet requireDeterminism,
            final RexProgram program,
            final StreamPhysicalRel relatedRel) {
        if (null != program.getCondition()) {
            // firstly check if exists non-deterministic condition
            RexNode rexNode = program.expandLocalRef(program.getCondition());
            checkNonDeterministicCondition(rexNode, relatedRel);
        }
        // extract all non-deterministic output columns first and check if any of them were
        // required be deterministic.
        List<RexNode> projects =
                program.getProjectList().stream()
                        .map(program::expandLocalRef)
                        .collect(Collectors.toList());
        Map<Integer, String> nonDeterministicCols = new HashMap<>();
        for (int index = 0; index < projects.size(); index++) {
            Optional<String> ndCall = FlinkRexUtil.getNonDeterministicCallName(projects.get(index));
            if (ndCall.isPresent()) {
                nonDeterministicCols.put(index, ndCall.get());
            } // else ignore
        }
        List<Integer> unsatisfiedColumns =
                requireDeterminism.toList().stream()
                        .filter(nonDeterministicCols::containsKey)
                        .collect(Collectors.toList());
        if (!unsatisfiedColumns.isEmpty()) {
            throwNonDeterministicColumnsError(
                    unsatisfiedColumns,
                    relatedRel.getRowType(),
                    relatedRel,
                    nonDeterministicCols,
                    Optional.empty());
        }
    }

    private void checkNonDeterministicCondition(
            final RexNode condition, final StreamPhysicalRel relatedRel) {
        Optional<String> ndCall = FlinkRexUtil.getNonDeterministicCallName(condition);
        ndCall.ifPresent(s -> throwNonDeterministicConditionError(s, condition, relatedRel));
    }

    private void checkUnsatisfiedDeterminism(
            final ImmutableBitSet requireDeterminism,
            final int aggStartIndex,
            final List<AggregateCall> aggCalls,
            final RelDataType rowType,
            final StreamPhysicalRel relatedRel) {
        Map<Integer, String> nonDeterministicOutput = new HashMap<>();
        // skip checking non-deterministic columns in grouping keys or filter args in agg call
        // because they were pushed down to input project which processes input only message
        int aggOutputIndex = aggStartIndex;
        for (AggregateCall aggCall : aggCalls) {
            if (!aggCall.getAggregation().isDeterministic()
                    || aggCall.getAggregation().isDynamicFunction()) {
                nonDeterministicOutput.put(aggOutputIndex, aggCall.getAggregation().getName());
            }
            aggOutputIndex++;
        }
        // check if exist non-deterministic aggCalls which were in requireDeterminism
        List<Integer> unsatisfiedColumns =
                requireDeterminism.toList().stream()
                        .filter(nonDeterministicOutput::containsKey)
                        .collect(Collectors.toList());
        if (!unsatisfiedColumns.isEmpty()) {
            throwNonDeterministicColumnsError(
                    unsatisfiedColumns,
                    rowType,
                    relatedRel,
                    nonDeterministicOutput,
                    Optional.empty());
        }
    }

    private void throwNonDeterministicConditionError(
            final String ndCall, final RexNode condition, final StreamPhysicalRel relatedRel)
            throws TableException {
        StringBuilder errorMsg = new StringBuilder();
        errorMsg.append(
                String.format(NON_DETERMINISTIC_CONDITION_ERROR_MSG_TEMPLATE, ndCall, condition));
        errorMsg.append("\nrelated rel plan:\n")
                .append(
                        FlinkRelOptUtil.toString(
                                relatedRel,
                                SqlExplainLevel.DIGEST_ATTRIBUTES,
                                false,
                                true,
                                false,
                                true,
                                false));

        throw new TableException(errorMsg.toString());
    }

    private void throwNonDeterministicColumnsError(
            final List<Integer> indexes,
            final RelDataType rowType,
            final StreamPhysicalRel relatedRel,
            final Map<Integer, String> ndCallMap,
            final Optional<String> ndCallName)
            throws TableException {
        StringBuilder errorMsg = new StringBuilder();
        errorMsg.append("The column(s): ");
        int index = 0;
        for (String column : rowType.getFieldNames()) {
            if (indexes.contains(index)) {
                errorMsg.append(column).append("(generated by non-deterministic function: ");
                if (ndCallName.isPresent()) {
                    errorMsg.append(ndCallName.get());
                } else {
                    errorMsg.append(ndCallMap.get(index));
                }
                errorMsg.append(" ) ");
            }
            index++;
        }
        errorMsg.append(
                "can not satisfy the determinism requirement for correctly processing update message("
                        + "'UB'/'UA'/'D' in changelogMode, not 'I' only), this usually happens when input node has"
                        + " no upsertKey(upsertKeys=[{}]) or current node outputs non-deterministic update "
                        + "messages. Please consider removing these non-deterministic columns or making them "
                        + "deterministic by using deterministic functions.\n");
        errorMsg.append("\nrelated rel plan:\n")
                .append(
                        FlinkRelOptUtil.toString(
                                relatedRel,
                                SqlExplainLevel.DIGEST_ATTRIBUTES,
                                false,
                                true,
                                false,
                                true,
                                false));

        throw new TableException(errorMsg.toString());
    }

    private ImmutableBitSet mappingRequireDeterminismToInput(
            final ImmutableBitSet requireDeterminism,
            final StreamPhysicalOverAggregateBase overAgg) {
        int inputFieldCnt = overAgg.getInput().getRowType().getFieldCount();
        List<Integer> requireInputIndexes =
                requireDeterminism.toList().stream()
                        .filter(index -> index < inputFieldCnt)
                        .collect(Collectors.toList());
        if (requireInputIndexes.size() == inputFieldCnt) {
            return ImmutableBitSet.range(inputFieldCnt);
        } else {
            Set<Integer> allRequiredInputSet = new HashSet<>(requireInputIndexes);

            OverSpec overSpec = OverAggregateUtil.createOverSpec(overAgg.logicWindow());
            // add partitionKeys
            Arrays.stream(overSpec.getPartition().getFieldIndices())
                    .forEach(allRequiredInputSet::add);
            // add aggCall's input
            int aggOutputIndex = inputFieldCnt;
            for (OverSpec.GroupSpec groupSpec : overSpec.getGroups()) {
                for (AggregateCall aggCall : groupSpec.getAggCalls()) {
                    if (requireDeterminism.get(aggOutputIndex)) {
                        requiredSourceInput(aggCall, allRequiredInputSet);
                    }
                    aggOutputIndex++;
                }
            }
            assert allRequiredInputSet.size() <= inputFieldCnt;
            return ImmutableBitSet.of(new ArrayList<>(allRequiredInputSet));
        }
    }

    private void requiredSourceInput(
            final AggregateCall aggCall, final Set<Integer> requiredInputSet) {
        // add agg args first
        requiredInputSet.addAll(aggCall.getArgList());
        // add agg filter args
        if (aggCall.filterArg > -1) {
            requiredInputSet.add(aggCall.filterArg);
        }
    }

    private ImmutableBitSet requireDeterminismExcludeUpsertKey(
            final StreamPhysicalRel inputRel, final ImmutableBitSet requireDeterminism) {
        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(inputRel.getCluster().getMetadataQuery());
        Set<ImmutableBitSet> inputUpsertKeys = fmq.getUpsertKeys(inputRel);
        ImmutableBitSet finalRequireDeterminism;
        if (inputUpsertKeys == null || inputUpsertKeys.isEmpty()) {
            finalRequireDeterminism = requireDeterminism;
        } else {
            if (inputUpsertKeys.stream().anyMatch(uk -> uk.contains(requireDeterminism))) {
                // upsert keys can satisfy the requireDeterminism because they are always
                // deterministic
                finalRequireDeterminism = NO_REQUIRED_DETERMINISM;
            } else {
                // otherwise we should check the column(s) that not in upsert keys
                List<ImmutableBitSet> leftKeys =
                        inputUpsertKeys.stream()
                                .map(requireDeterminism::except)
                                .collect(Collectors.toList());
                if (leftKeys.isEmpty()) {
                    finalRequireDeterminism = NO_REQUIRED_DETERMINISM;
                } else {
                    leftKeys.sort(Comparator.comparingInt(ImmutableBitSet::cardinality));
                    // use least require determinism
                    finalRequireDeterminism = leftKeys.get(0);
                }
            }
        }
        return finalRequireDeterminism;
    }
}
