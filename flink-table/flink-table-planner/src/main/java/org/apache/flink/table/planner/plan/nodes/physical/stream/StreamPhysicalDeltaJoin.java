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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinAssociation;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinLookupChain;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeltaJoin;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.ExpressionFormat;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.JoinTypeUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.plan.utils.RelExplainUtil;
import org.apache.flink.table.planner.plan.utils.UpsertKeyUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.combineOutputRowType;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.swapJoinType;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/** Stream physical RelNode for delta join. */
public class StreamPhysicalDeltaJoin extends Join implements StreamPhysicalRel {

    private final RelDataType rowType;

    // base on 0
    private final List<Integer> leftAllBinaryInputOrdinals;
    // base on 0
    private final List<Integer> rightAllBinaryInputOrdinals;

    private final DeltaJoinLookupChain left2RightLookupChain;
    private final DeltaJoinLookupChain right2LeftLookupChain;
    private final DeltaJoinAssociation deltaJoinAssociation;

    public StreamPhysicalDeltaJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            RexNode originalJoinCondition,
            List<Integer> leftAllBinaryInputOrdinals,
            List<Integer> rightAllBinaryInputOrdinals,
            DeltaJoinLookupChain left2RightLookupChain,
            DeltaJoinLookupChain right2LeftLookupChain,
            DeltaJoinAssociation deltaJoinAssociation,
            RelDataType rowType) {
        super(
                cluster,
                traitSet,
                hints,
                left,
                right,
                originalJoinCondition,
                Collections.emptySet(),
                joinType);
        this.leftAllBinaryInputOrdinals = leftAllBinaryInputOrdinals;
        this.rightAllBinaryInputOrdinals = rightAllBinaryInputOrdinals;
        this.left2RightLookupChain = left2RightLookupChain;
        this.right2LeftLookupChain = right2LeftLookupChain;
        this.deltaJoinAssociation = deltaJoinAssociation;
        this.rowType = rowType;

        Preconditions.checkArgument(
                left2RightLookupChain.size() == rightAllBinaryInputOrdinals.size());
        Preconditions.checkArgument(
                right2LeftLookupChain.size() == leftAllBinaryInputOrdinals.size());
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
        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(this.getCluster().getMetadataQuery());

        int[] leftUpsertKey = UpsertKeyUtil.smallestKey(fmq.getUpsertKeys(left)).orElse(null);
        int[] rightUpsertKey = UpsertKeyUtil.smallestKey(fmq.getUpsertKeys(right)).orElse(null);

        return new StreamExecDeltaJoin(
                config,
                JoinTypeUtil.getFlinkJoinType(joinType),
                condition,
                joinInfo.leftKeys.toIntArray(),
                leftUpsertKey,
                joinInfo.rightKeys.toIntArray(),
                rightUpsertKey,
                leftAllBinaryInputOrdinals,
                rightAllBinaryInputOrdinals,
                left2RightLookupChain,
                right2LeftLookupChain,
                deltaJoinAssociation.getAllBinaryInputTableSourceSpecs(),
                deltaJoinAssociation.getJoinTree(),
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(rowType),
                getRelDetailedDescription(),
                asyncLookupOptions);
    }

    public DeltaJoinAssociation getDeltaJoinAssociation() {
        return deltaJoinAssociation;
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new StreamPhysicalDeltaJoin(
                getCluster(),
                traitSet,
                hints,
                left,
                right,
                joinType,
                conditionExpr,
                leftAllBinaryInputOrdinals,
                rightAllBinaryInputOrdinals,
                left2RightLookupChain,
                right2LeftLookupChain,
                deltaJoinAssociation,
                rowType);
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw =
                pw.input("left", left)
                        .input("right", right)
                        .item("joinType", JoinTypeUtil.getFlinkJoinType(joinType).toString())
                        .item(
                                "where",
                                getExpressionString(
                                        condition,
                                        JavaScalaConversionUtil.toScala(
                                                        this.getRowType().getFieldNames())
                                                .toList(),
                                        JavaScalaConversionUtil.toScala(Optional.empty()),
                                        RelExplainUtil.preferExpressionFormat(pw),
                                        RelExplainUtil.preferExpressionDetail(pw)));

        String leftToRight =
                rightAllBinaryInputOrdinals.size() == 1
                        ? "Binary"
                        : lookupChainDetailToString(left2RightLookupChain, pw.getDetailLevel());
        String rightToLeft =
                leftAllBinaryInputOrdinals.size() == 1
                        ? "Binary"
                        : lookupChainDetailToString(right2LeftLookupChain, pw.getDetailLevel());

        return pw.item("leftToRight", leftToRight)
                .item("rightToLeft", rightToLeft)
                .item("select", String.join(", ", rowType.getFieldNames()));
    }

    private String lookupChainDetailToString(
            DeltaJoinLookupChain lookupChain, SqlExplainLevel sqlExplainLevel) {
        List<CascadedLookupDetail> cascadedLookupDetails = new ArrayList<>();

        FlinkContext context = unwrapContext(getCluster());
        FlinkTypeFactory typeFactory = unwrapTypeFactory(getCluster());

        Iterator<DeltaJoinLookupChain.Node> iterator = lookupChain.newIterator();

        int round = 1;
        while (iterator.hasNext()) {
            DeltaJoinLookupChain.Node node = iterator.next();
            RowType streamSideRowType =
                    deltaJoinAssociation
                            .getJoinTree()
                            .getOutputRowTypeOnNode(
                                    node.inputTableBinaryInputOrdinals, typeFactory);
            TableSourceTable lookupTable =
                    (TableSourceTable)
                            node.deltaJoinSpec
                                    .getLookupTable()
                                    .getTemporalTable(context, typeFactory);

            String lookupKeys =
                    lookupKeysToString(
                            node.deltaJoinSpec.getLookupKeyMap(),
                            lookupTable.getRowType().getFieldNames(),
                            streamSideRowType.getFieldNames());

            Optional<RexNode> remainingCondition = node.deltaJoinSpec.getRemainingCondition();
            final String remainingConditionStr;
            if (remainingCondition.isPresent()) {
                RowType lookupSideRowType =
                        deltaJoinAssociation
                                .getJoinTree()
                                .getOutputRowTypeOnNode(
                                        new int[] {node.lookupTableBinaryInputOrdinal},
                                        typeFactory);

                boolean leftLookupRight =
                        isLeftSideLookupRightSide(
                                node.inputTableBinaryInputOrdinals,
                                node.lookupTableBinaryInputOrdinal);

                List<String> outputFieldNames =
                        leftLookupRight
                                ? combineOutputRowType(
                                                streamSideRowType,
                                                lookupSideRowType,
                                                node.joinType,
                                                unwrapTypeFactory(this.getCluster()))
                                        .getFieldNames()
                                : combineOutputRowType(
                                                lookupSideRowType,
                                                streamSideRowType,
                                                swapJoinType(node.joinType),
                                                unwrapTypeFactory(this.getCluster()))
                                        .getFieldNames();

                remainingConditionStr =
                        conditionToString(
                                remainingCondition.get(), outputFieldNames, sqlExplainLevel);
            } else {
                remainingConditionStr = null;
            }

            String sourceTables =
                    Arrays.stream(node.inputTableBinaryInputOrdinals)
                            .mapToObj(
                                    i ->
                                            deltaJoinAssociation
                                                    .getBinaryInputTable(i)
                                                    .contextResolvedTable()
                                                    .getIdentifier()
                                                    .asSummaryString())
                            .collect(Collectors.joining(", "));
            CascadedLookupDetail lookupDetail =
                    CascadedLookupDetail.of(
                            round,
                            sourceTables,
                            lookupTable.contextResolvedTable().getIdentifier().asSummaryString(),
                            lookupKeys,
                            remainingConditionStr);

            cascadedLookupDetails.add(lookupDetail);

            round++;
        }

        return cascadedLookupDetails.toString();
    }

    private boolean isLeftSideLookupRightSide(
            int[] inputTableBinaryInputOrdinals, int lookupTableBinaryInputOrdinal) {
        Preconditions.checkArgument(inputTableBinaryInputOrdinals.length > 0);
        if (inputTableBinaryInputOrdinals.length == 1) {
            Preconditions.checkState(
                    inputTableBinaryInputOrdinals[0] != lookupTableBinaryInputOrdinal);
            return inputTableBinaryInputOrdinals[0] < lookupTableBinaryInputOrdinal;
        }

        Preconditions.checkState(
                Arrays.stream(inputTableBinaryInputOrdinals)
                                .allMatch(i -> i < lookupTableBinaryInputOrdinal)
                        || Arrays.stream(inputTableBinaryInputOrdinals)
                                .allMatch(i -> i > lookupTableBinaryInputOrdinal));

        return Arrays.stream(inputTableBinaryInputOrdinals)
                .allMatch(i -> i < lookupTableBinaryInputOrdinal);
    }

    private String lookupKeysToString(
            Map<Integer, FunctionCallUtil.FunctionParam> lookupKeyMap,
            List<String> lookupTableFields,
            List<String> streamInputFields) {
        return lookupKeyMap.entrySet().stream()
                .map(
                        entry -> {
                            String template = "%s=%s";
                            if (entry.getValue() instanceof FunctionCallUtil.FieldRef) {
                                FunctionCallUtil.FieldRef fieldKey =
                                        (FunctionCallUtil.FieldRef) entry.getValue();
                                return String.format(
                                        template,
                                        lookupTableFields.get(entry.getKey()),
                                        streamInputFields.get(fieldKey.index));
                            } else if (entry.getValue() instanceof LookupJoinUtil.Constant) {
                                LookupJoinUtil.Constant constantKey =
                                        (LookupJoinUtil.Constant) entry.getValue();
                                return String.format(
                                        template,
                                        lookupTableFields.get(entry.getKey()),
                                        RelExplainUtil.literalToString(constantKey.literal));
                            } else {
                                throw new TableException(
                                        "Unsupported lookup key type: "
                                                + entry.getValue().getClass().getSimpleName());
                            }
                        })
                .collect(Collectors.joining(", "));
    }

    private String conditionToString(
            RexNode condition, List<String> outputFieldNames, SqlExplainLevel sqlExplainLevel) {
        return getExpressionString(
                condition,
                JavaConverters.asScalaBufferConverter(outputFieldNames).asScala().toList(),
                JavaScalaConversionUtil.toScala(Optional.empty()),
                ExpressionFormat.Infix(),
                sqlExplainLevel);
    }

    private boolean isBinaryDeltaJoin() {
        return left2RightLookupChain.size() == 1 && right2LeftLookupChain.size() == 1;
    }

    private static class CascadedLookupDetail {

        private final int round;
        private final String sourceTables;
        private final String lookupTable;
        private final String lookupKeys;
        private final String remainingCondition;

        private CascadedLookupDetail(
                int round,
                String sourceTables,
                String lookupTable,
                String lookupKeys,
                String remainingCondition) {
            this.round = round;
            this.sourceTables = sourceTables;
            this.lookupTable = lookupTable;
            this.lookupKeys = lookupKeys;
            this.remainingCondition = remainingCondition;
        }

        public static CascadedLookupDetail of(
                int round,
                String sourceTables,
                String lookupTable,
                String lookupKeys,
                String remainingCondition) {
            return new CascadedLookupDetail(
                    round, sourceTables, lookupTable, lookupKeys, remainingCondition);
        }

        @Override
        public final String toString() {
            return toStringTerms().entrySet().stream()
                    .map(entry -> entry.getKey() + "=[" + entry.getValue() + "]")
                    .collect(Collectors.joining(", ", "{", "}"));
        }

        private LinkedHashMap<String, String> toStringTerms() {
            LinkedHashMap<String, String> map = new LinkedHashMap<>();
            map.put("round", String.valueOf(round));
            map.put("sourceTables", sourceTables);
            map.put("lookupTable", lookupTable);
            map.put("lookupKeys", lookupKeys);
            if (remainingCondition != null) {
                map.put("remaining", remainingCondition);
            }
            return map;
        }
    }
}
