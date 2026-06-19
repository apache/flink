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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSnapshot;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.rules.common.CommonTemporalTableJoinRule;
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

/**
 * Planner rule that rewrites temporal join with extracted primary key, Event-time temporal table
 * join requires primary key and row time attribute of versioned table. The versioned table could be
 * a table source or a view only if it contains the unique key and time attribute.
 *
 * <p>Flink supports extract the primary key and row time attribute from the view if the view comes
 * from {@link StreamPhysicalRank} node which can convert to a {@link StreamExecDeduplicate} node
 * finally.
 */
@Value.Enclosing
public class TemporalJoinRewriteWithUniqueKeyRule
        extends RelRule<
                TemporalJoinRewriteWithUniqueKeyRule.TemporalJoinRewriteWithUniqueKeyRuleConfig>
        implements CommonTemporalTableJoinRule {

    public static final TemporalJoinRewriteWithUniqueKeyRule INSTANCE =
            TemporalJoinRewriteWithUniqueKeyRule.TemporalJoinRewriteWithUniqueKeyRuleConfig.DEFAULT
                    .toRule();

    private TemporalJoinRewriteWithUniqueKeyRule(
            TemporalJoinRewriteWithUniqueKeyRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);
        FlinkLogicalSnapshot snapshot = call.rel(2);
        FlinkLogicalRel snapshotInput = call.rel(3);

        boolean isTemporalJoin = matches(snapshot);
        boolean canConvertToLookup = canConvertToLookupJoin(snapshot, snapshotInput);
        List<JoinRelType> supportedJoinTypes = Arrays.asList(JoinRelType.INNER, JoinRelType.LEFT);

        return isTemporalJoin
                && !canConvertToLookup
                && supportedJoinTypes.contains(join.getJoinType());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);
        FlinkLogicalRel leftInput = call.rel(1);
        FlinkLogicalSnapshot snapshot = call.rel(2);
        FlinkLogicalRel snapshotInput = call.rel(3);

        RexNode joinCondition = join.getCondition();
        RexNode newJoinCondition =
                joinCondition.accept(
                        new RexShuttle() {
                            @Override
                            public RexNode visitCall(RexCall call) {
                                if (call.getOperator()
                                        .equals(
                                                TemporalJoinUtil
                                                        .INITIAL_TEMPORAL_JOIN_CONDITION())) {
                                    RexNode snapshotTimeInputRef;
                                    List<RexNode> leftJoinKey;
                                    List<RexNode> rightJoinKey;

                                    if (TemporalJoinUtil.isInitialRowTimeTemporalTableJoin(call)) {
                                        snapshotTimeInputRef = call.operands.get(0);
                                        leftJoinKey =
                                                ((RexCall) call.operands.get(2)).getOperands();
                                        rightJoinKey =
                                                ((RexCall) call.operands.get(3)).getOperands();
                                    } else {
                                        snapshotTimeInputRef = call.operands.get(0);
                                        leftJoinKey =
                                                ((RexCall) call.operands.get(1)).getOperands();
                                        rightJoinKey =
                                                ((RexCall) call.operands.get(2)).getOperands();
                                    }

                                    RexBuilder rexBuilder = join.getCluster().getRexBuilder();
                                    Optional<List<RexNode>> primaryKeyInputRefs =
                                            extractPrimaryKeyInputRefs(
                                                    leftInput, snapshot, snapshotInput, rexBuilder);
                                    validateRightPrimaryKey(
                                            join, rightJoinKey, primaryKeyInputRefs);

                                    if (TemporalJoinUtil.isInitialRowTimeTemporalTableJoin(call)) {
                                        RexNode rightTimeInputRef = call.operands.get(1);
                                        return TemporalJoinUtil.makeRowTimeTemporalTableJoinConCall(
                                                rexBuilder,
                                                snapshotTimeInputRef,
                                                rightTimeInputRef,
                                                JavaConverters.asScalaBufferConverter(
                                                                primaryKeyInputRefs.get())
                                                        .asScala(),
                                                JavaConverters.asScalaBufferConverter(leftJoinKey)
                                                        .asScala(),
                                                JavaConverters.asScalaBufferConverter(rightJoinKey)
                                                        .asScala());
                                    } else {
                                        return TemporalJoinUtil
                                                .makeProcTimeTemporalTableJoinConCall(
                                                        rexBuilder,
                                                        snapshotTimeInputRef,
                                                        JavaConverters.asScalaBufferConverter(
                                                                        primaryKeyInputRefs.get())
                                                                .asScala(),
                                                        JavaConverters.asScalaBufferConverter(
                                                                        leftJoinKey)
                                                                .asScala(),
                                                        JavaConverters.asScalaBufferConverter(
                                                                        rightJoinKey)
                                                                .asScala());
                                    }
                                } else {
                                    return super.visitCall(call);
                                }
                            }
                        });
        RelNode rewriteJoin =
                FlinkLogicalJoin.create(
                        leftInput, snapshot, newJoinCondition, join.getHints(), join.getJoinType());
        call.transformTo(rewriteJoin);
    }

    private void validateRightPrimaryKey(
            FlinkLogicalJoin join,
            List<RexNode> rightJoinKeyExpressions,
            Optional<List<RexNode>> rightPrimaryKeyInputRefs) {

        if (!rightPrimaryKeyInputRefs.isPresent()) {
            throw new ValidationException(
                    "Temporal Table Join requires primary key in versioned table, "
                            + "but no primary key can be found. "
                            + "The physical plan is:\n"
                            + RelOptUtil.toString(join)
                            + "\n");
        }

        List<Integer> rightJoinKeyRefIndices =
                rightJoinKeyExpressions.stream()
                        .map(rex -> ((RexInputRef) rex).getIndex())
                        .collect(Collectors.toList());

        List<Integer> rightPrimaryKeyRefIndices =
                rightPrimaryKeyInputRefs.get().stream()
                        .map(rex -> ((RexInputRef) rex).getIndex())
                        .collect(Collectors.toList());

        boolean primaryKeyContainedInJoinKey =
                rightPrimaryKeyRefIndices.stream()
                        .allMatch(pk -> rightJoinKeyRefIndices.contains(pk));

        if (!primaryKeyContainedInJoinKey) {
            List<String> joinFieldNames = join.getRowType().getFieldNames();
            List<String> joinLeftFieldNames = join.getLeft().getRowType().getFieldNames();
            List<String> joinRightFieldNames = join.getRight().getRowType().getFieldNames();

            String primaryKeyNames =
                    rightPrimaryKeyRefIndices.stream()
                            .map(i -> joinFieldNames.get(i))
                            .collect(Collectors.joining(","));

            String joinEquiInfo =
                    join.analyzeCondition().pairs().stream()
                            .map(
                                    pair ->
                                            joinLeftFieldNames.get(pair.source)
                                                    + "="
                                                    + joinRightFieldNames.get(pair.target))
                            .collect(Collectors.joining(","));

            throw new ValidationException(
                    "Temporal table's primary key ["
                            + primaryKeyNames
                            + "] must be included in the "
                            + "equivalence condition of temporal join, but current temporal join condition is ["
                            + joinEquiInfo
                            + "].");
        }
    }

    private Optional<List<RexNode>> extractPrimaryKeyInputRefs(
            RelNode leftInput,
            FlinkLogicalSnapshot snapshot,
            FlinkLogicalRel snapshotInput,
            RexBuilder rexBuilder) {
        List<RelDataTypeField> rightFields = snapshot.getRowType().getFieldList();
        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(snapshot.getCluster().getMetadataQuery());

        Set<ImmutableBitSet> upsertKeySet = fmq.getUpsertKeys(snapshotInput);
        List<RelDataTypeField> fields = snapshot.getRowType().getFieldList();

        if (upsertKeySet != null && !upsertKeySet.isEmpty()) {
            int leftFieldCnt = leftInput.getRowType().getFieldCount();
            List<List<RexNode>> upsertKeySetInputRefs =
                    upsertKeySet.stream()
                            .filter(bitSet -> !bitSet.isEmpty())
                            .map(
                                    bitSet ->
                                            Arrays.stream(bitSet.toArray())
                                                    .mapToObj(index -> fields.get(index))
                                                    // build InputRef of upsert key in snapshot
                                                    .map(
                                                            f ->
                                                                    (RexNode)
                                                                            rexBuilder.makeInputRef(
                                                                                    f.getType(),
                                                                                    leftFieldCnt
                                                                                            + rightFields
                                                                                                    .indexOf(
                                                                                                            f)))
                                                    .collect(Collectors.toList()))
                            .collect(Collectors.toList());
            // select shortest upsert key as primary key
            return upsertKeySetInputRefs.stream()
                    .sorted(Comparator.comparingInt(List::size))
                    .findFirst();
        } else {
            return Optional.empty();
        }
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface TemporalJoinRewriteWithUniqueKeyRuleConfig extends RelRule.Config {
        TemporalJoinRewriteWithUniqueKeyRule.TemporalJoinRewriteWithUniqueKeyRuleConfig DEFAULT =
                ImmutableTemporalJoinRewriteWithUniqueKeyRule
                        .TemporalJoinRewriteWithUniqueKeyRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalJoin.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        b2 ->
                                                                b2.operand(
                                                                                FlinkLogicalSnapshot
                                                                                        .class)
                                                                        .oneInput(
                                                                                b3 ->
                                                                                        b3.operand(
                                                                                                        FlinkLogicalRel
                                                                                                                .class)
                                                                                                .anyInputs())))
                        .withDescription("TemporalJoinRewriteWithUniqueKeyRule");

        @Override
        default TemporalJoinRewriteWithUniqueKeyRule toRule() {
            return new TemporalJoinRewriteWithUniqueKeyRule(this);
        }
    }
}
