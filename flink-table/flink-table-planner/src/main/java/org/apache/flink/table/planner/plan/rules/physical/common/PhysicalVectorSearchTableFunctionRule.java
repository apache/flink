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

package org.apache.flink.table.planner.plan.rules.physical.common;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.ml.SqlVectorSearchTableFunction;
import org.apache.flink.table.planner.plan.nodes.FlinkConvention;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.exec.spec.VectorSearchSpec;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalVectorSearchTableFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalVectorSearchTableFunction;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.Util;
import org.immutables.value.Value;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Rule to convert a {@link FlinkLogicalCorrelate} with VECTOR_SEARCH call into a {@link
 * StreamPhysicalVectorSearchTableFunction}.
 */
@Value.Enclosing
public class PhysicalVectorSearchTableFunctionRule
        extends RelRule<PhysicalVectorSearchTableFunctionRule.Config> {

    public static final PhysicalVectorSearchTableFunctionRule STREAM_INSTANCE =
            Config.STREAM_INSTANCE.toRule();

    public static final PhysicalVectorSearchTableFunctionRule BATCH_INSTANCE =
            Config.BATCH_INSTANCE.toRule();

    protected PhysicalVectorSearchTableFunctionRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalTableFunctionScan scan = call.rel(2);
        RexNode rexNode = scan.getCall();
        if (!(rexNode instanceof RexCall)) {
            return false;
        }
        RexCall rexCall = (RexCall) rexNode;
        return rexCall.getOperator() instanceof SqlVectorSearchTableFunction;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // QUERY_TABLE
        RelNode input = call.rel(1);
        final RelNode newInput = RelOptRule.convert(input, config.convention());

        // SEARCH_TABLE
        FlinkLogicalCorrelate correlate = call.rel(0);
        FlinkLogicalTableFunctionScan vectorSearchCall = call.rel(2);
        String functionName = ((RexCall) vectorSearchCall.getCall()).getOperator().getName();
        SearchTableExtractor extractor = new SearchTableExtractor(functionName);
        extractor.visit(vectorSearchCall.getInput(0));

        if (config.convention() == FlinkConventions.STREAM_PHYSICAL()) {
            call.transformTo(
                    new StreamPhysicalVectorSearchTableFunction(
                            correlate.getCluster(),
                            correlate.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL()),
                            newInput,
                            extractor.searchTable,
                            extractor.calcProgram == null
                                    ? null
                                    : pullUpRexProgram(
                                            input.getCluster(),
                                            extractor.searchTable.getRowType(),
                                            vectorSearchCall.getRowType(),
                                            correlate
                                                    .getRowType()
                                                    .getFieldList()
                                                    .subList(
                                                            input.getRowType().getFieldCount(),
                                                            correlate.getRowType().getFieldCount()),
                                            extractor.calcProgram),
                            buildVectorSearchSpec(
                                    correlate,
                                    vectorSearchCall,
                                    extractor.searchTable,
                                    functionName),
                            correlate.getRowType()));
        } else if (config.convention() == FlinkConventions.BATCH_PHYSICAL()) {
            call.transformTo(
                    new BatchPhysicalVectorSearchTableFunction(
                            correlate.getCluster(),
                            correlate.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL()),
                            newInput,
                            extractor.searchTable,
                            extractor.calcProgram == null
                                    ? null
                                    : pullUpRexProgram(
                                            input.getCluster(),
                                            extractor.searchTable.getRowType(),
                                            vectorSearchCall.getRowType(),
                                            correlate
                                                    .getRowType()
                                                    .getFieldList()
                                                    .subList(
                                                            input.getRowType().getFieldCount(),
                                                            correlate.getRowType().getFieldCount()),
                                            extractor.calcProgram),
                            buildVectorSearchSpec(
                                    correlate,
                                    vectorSearchCall,
                                    extractor.searchTable,
                                    functionName),
                            correlate.getRowType()));
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported convention: " + config.convention());
        }
    }

    private VectorSearchSpec buildVectorSearchSpec(
            FlinkLogicalCorrelate correlate,
            FlinkLogicalTableFunctionScan scan,
            RelOptTable searchTable,
            String functionName) {
        JoinRelType joinType = correlate.getJoinType();
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
            throw new TableException(
                    String.format(
                            "%s only supports INNER JOIN and LEFT JOIN, but get %s JOIN.",
                            functionName, joinType));
        }

        RexCall functionCall = (RexCall) scan.getCall();

        // COLUMN_TO_SEARCH
        RexCall descriptorCall = (RexCall) functionCall.getOperands().get(1);
        RexNode searchColumn = descriptorCall.getOperands().get(0);
        if (!(searchColumn instanceof RexLiteral)) {
            throw new TableException(
                    String.format(
                            "%s got an unknown parameter column_to_search in descriptor: %s.",
                            functionName, searchColumn));
        }
        int searchIndex =
                searchTable
                        .getRowType()
                        .getFieldNames()
                        .indexOf(RexLiteral.stringValue(searchColumn));
        if (searchIndex == -1) {
            throw new TableException(
                    String.format(
                            "%s can not find column `%s` in the search_table %s physical output type. Currently, Flink doesn't support to use computed column as the search column.",
                            functionName,
                            RexLiteral.stringValue(searchColumn),
                            String.join(".", searchTable.getQualifiedName())));
        }

        // COLUMN_TO_QUERY
        FunctionCallUtil.FunctionParam queryColumn =
                getQueryColumnParam(functionCall.getOperands().get(2), correlate, functionName);

        Map<Integer, FunctionCallUtil.FunctionParam> searchColumns = new LinkedHashMap<>();
        searchColumns.put(searchIndex, queryColumn);

        // TOP_K
        RexLiteral topK = (RexLiteral) functionCall.getOperands().get(3);
        FunctionCallUtil.Constant topKParam =
                new FunctionCallUtil.Constant(FlinkTypeFactory.toLogicalType(topK.getType()), topK);

        // Runtime Config
        Map<String, String> runtimeConfig =
                functionCall.getOperands().size() < 5
                        ? null
                        : FunctionCallUtil.convert((RexCall) functionCall.getOperands().get(4));

        return new VectorSearchSpec(
                joinType,
                searchColumns,
                topKParam,
                runtimeConfig,
                FlinkTypeFactory.toLogicalRowType(scan.getRowType()));
    }

    private FunctionCallUtil.FunctionParam getQueryColumnParam(
            RexNode queryColumn, FlinkLogicalCorrelate correlate, String functionName) {
        if (queryColumn instanceof RexFieldAccess) {
            RexNode refNode = ((RexFieldAccess) queryColumn).getReferenceExpr();
            if (refNode instanceof RexFieldAccess) {
                // nested field unsupported
                throw new TableException(
                        String.format(
                                "%s does not support nested field in parameter column_to_query, but get %s.",
                                functionName, queryColumn));
            } else if (!(correlate.getCorrelationId().equals(((RexCorrelVariable) refNode).id))) {
                throw new TableException(
                        String.format(
                                "This is a bug. Planner can not resolve the correlation in %s. Please file an issue.",
                                functionName));
            }
            return new FunctionCallUtil.FieldRef(
                    ((RexFieldAccess) queryColumn).getField().getIndex());
        } else {
            throw new TableException(
                    String.format(
                            "Expect function %s's parameter column_to_query is literal or field reference, but get expression %s. ",
                            functionName, queryColumn));
        }
    }

    /**
     * Pull up the Calc under the VectorSearchCall.
     *
     * <p>Note: The vector search operator actually fetch the data and then do the calculation. So
     * pull up the calc is to align the behaviour.
     */
    private RexProgram pullUpRexProgram(
            RelOptCluster cluster,
            RelDataType scanOutputType,
            RelDataType originFunctionCallType,
            List<RelDataTypeField> correlateRightOutputTypes,
            RexProgram originProgram) {
        RelDataType searchOutputType =
                cluster.getTypeFactory()
                        .builder()
                        .kind(scanOutputType.getStructKind())
                        .addAll(scanOutputType.getFieldList())
                        .add(Util.last(originFunctionCallType.getFieldList()))
                        .build();
        RelDataType newOutputType =
                cluster.getTypeFactory()
                        .builder()
                        .kind(originProgram.getOutputRowType().getStructKind())
                        .addAll(correlateRightOutputTypes)
                        .build();
        List<RexNode> exprs = new ArrayList<>(originProgram.getExprList());
        exprs.add(
                cluster.getRexBuilder()
                        .makeInputRef(searchOutputType, searchOutputType.getFieldCount() - 1));
        return RexProgramBuilder.create(
                        cluster.getRexBuilder(),
                        searchOutputType,
                        exprs,
                        originProgram.getProjectList(),
                        originProgram.getCondition(),
                        newOutputType,
                        true,
                        null)
                .getProgram();
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {

        Config STREAM_INSTANCE =
                ImmutablePhysicalVectorSearchTableFunctionRule.Config.builder()
                        .convention(FlinkConventions.STREAM_PHYSICAL())
                        .operandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalCorrelate.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        b2 ->
                                                                b2.operand(
                                                                                FlinkLogicalTableFunctionScan
                                                                                        .class)
                                                                        .anyInputs()))
                        .description("StreamPhysicalVectorSearchTableFunctionRule")
                        .build();

        Config BATCH_INSTANCE =
                ImmutablePhysicalVectorSearchTableFunctionRule.Config.builder()
                        .convention(FlinkConventions.BATCH_PHYSICAL())
                        .operandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalCorrelate.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        b2 ->
                                                                b2.operand(
                                                                                FlinkLogicalTableFunctionScan
                                                                                        .class)
                                                                        .anyInputs()))
                        .description("BatchPhysicalVectorSearchTableFunctionRule")
                        .build();

        @Override
        default PhysicalVectorSearchTableFunctionRule toRule() {
            return new PhysicalVectorSearchTableFunctionRule(this);
        }

        FlinkConvention convention();
    }

    /**
     * A utility class to extract table source and calc program.
     *
     * <p>Supported tree structure:
     *
     * <pre>{@code
     * Calc(without filter) —— TableScan
     * TableScan
     * }</pre>
     */
    static class SearchTableExtractor {

        enum NodeType {
            CALC,
            SCAN
        }

        @Nullable RexProgram calcProgram;
        TableSourceTable searchTable;

        private final String functionName;
        private NodeType parentNode;

        SearchTableExtractor(String functionName) {
            this.functionName = functionName;
        }

        private void visit(RelNode rel) {
            if (rel instanceof RelSubset) {
                rel = ((RelSubset) rel).getBestOrOriginal();
            }

            NodeType currentNode = transform(rel);
            switch (currentNode) {
                case CALC:
                    if (parentNode != null) {
                        throw new RelOptPlanner.CannotPlanException(
                                String.format(
                                        "%s assumes calc to be the first node in parameter search_table, but it has a parent %s.",
                                        functionName, parentNode));
                    }
                    calcProgram = ((Calc) rel).getProgram();
                    if (calcProgram.getCondition() != null) {
                        throw new RelOptPlanner.CannotPlanException(
                                String.format(
                                        "%s does not support filter on parameter search_table.",
                                        functionName));
                    }
                    break;
                case SCAN:
                    if (!(((TableScan) rel).getTable() instanceof TableSourceTable)) {
                        throw new RelOptPlanner.CannotPlanException(
                                "%s does not support search_table of type: "
                                        + searchTable.getClass());
                    }
                    searchTable = (TableSourceTable) ((TableScan) rel).getTable();
                    break;
            }

            parentNode = currentNode;
            if (currentNode != NodeType.SCAN) {
                visit(rel.getInput(0));
            }
        }

        private NodeType transform(RelNode node) {
            NodeType transformed;
            if (node instanceof Calc) {
                transformed = NodeType.CALC;
            } else if (node instanceof TableScan) {
                transformed = NodeType.SCAN;
            } else {
                throw new RelOptPlanner.CannotPlanException(
                        String.format(
                                "%s does not support %s node in parameter search_table.",
                                functionName, node.getClass().getSimpleName()));
            }
            return transformed;
        }
    }
}
