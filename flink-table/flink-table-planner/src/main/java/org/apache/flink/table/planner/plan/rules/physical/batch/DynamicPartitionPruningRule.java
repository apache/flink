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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringDataCollector;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalJoinBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED;

/**
 * Planner rule that tries to do partition prune in the execution phase, which can translate a
 * {@link BatchPhysicalTableSourceScan} to a {@link BatchPhysicalDynamicFilteringTableSourceScan}
 * whose source is a partition source. The {@link
 * OptimizerConfigOptions#TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED} need to be true.
 *
 * <p>Suppose we have the original physical plan:
 *
 * <pre>{@Code
 * LogicalProject(...)
 * HashJoin(joinType=[InnerJoin], where=[=(fact_partition_key, dim_key)], select=[xxx])
 *  * :- TableSourceScan(table=[[fact]], fields=[xxx, fact_partition_key],) # Is a partition table.
 *  * +- Exchange(distribution=[broadcast])
 *  *    +- Calc(select=[xxx], where=[<(xxx, xxx)]) # Need have an arbitrary filter condition.
 *  *       +- TableSourceScan(table=[[dim, filter=[]]], fields=[xxx, dim_key])
 * }</pre>
 *
 * <p>This physical plan will be rewritten to:
 *
 * <pre>{@Code
 * HashJoin(joinType=[InnerJoin], where=[=(fact_partition_key, dim_key)], select=[xxx])
 * :- DynamicFilteringTableSourceScan(table=[[fact]], fields=[xxx, fact_partition_key]) # Is a partition table.
 * :  +- DynamicFilteringDataCollector(fields=[dim_key])
 * :     +- Calc(select=[xxx], where=[<(xxx, xxx)])
 * :        +- TableSourceScan(table=[[dim, filter=[]]], fields=[xxx, dim_key])
 * +- Exchange(distribution=[broadcast])
 *    +- Calc(select=[xxx], where=[<(xxx, xxx)]) # Need have an arbitrary filter condition.
 *       +- TableSourceScan(table=[[dim, filter=[]]], fields=[xxx, dim_key])
 * }</pre>
 */
public abstract class DynamicPartitionPruningRule extends RelRule<RelRule.Config> {

    // To support more patterns of dynamic partition pruning, in this rule base, we provide eight
    // different matching rules according to the different situation of the fact side (partition
    // table side) and the different order of left/right join.
    public static final RuleSet DYNAMIC_PARTITION_PRUNING_RULES =
            RuleSets.ofList(
                    DynamicPartitionPruningFactInRightRule.Config.DEFAULT.toRule(),
                    DynamicPartitionPruningFactInLeftRule.Config.DEFAULT.toRule(),
                    DynamicPartitionPruningFactInRightWithExchangeRule.Config.DEFAULT.toRule(),
                    DynamicPartitionPruningFactInLeftWithExchangeRule.Config.DEFAULT.toRule(),
                    DynamicPartitionPruningFactInRightWithCalcRule.Config.DEFAULT.toRule(),
                    DynamicPartitionPruningFactInLeftWithCalcRule.Config.DEFAULT.toRule(),
                    DynamicPartitionPruningFactInRightWithExchangeAndCalcRule.Config.DEFAULT
                            .toRule(),
                    DynamicPartitionPruningFactInLeftWithExchangeAndCalcRule.Config.DEFAULT
                            .toRule());

    protected DynamicPartitionPruningRule(RelRule.Config config) {
        super(config);
    }

    protected boolean doMatches(
            BatchPhysicalJoinBase join,
            BatchPhysicalRel dimSide,
            @Nullable BatchPhysicalCalc factCalc,
            BatchPhysicalTableSourceScan factScan,
            boolean factInLeft) {
        if (!ShortcutUtils.unwrapContext(join)
                .getTableConfig()
                .get(TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED)) {
            return false;
        }
        if (factScan instanceof BatchPhysicalDynamicFilteringTableSourceScan) {
            // rule applied
            return false;
        }

        // Now dynamic filtering support left outer join, right outer join, inner join and semi
        // join.
        if (join.getJoinType() == JoinRelType.LEFT) {
            if (factInLeft) {
                return false;
            }
        } else if (join.getJoinType() == JoinRelType.RIGHT) {
            if (!factInLeft) {
                return false;
            }
        } else if (join.getJoinType() != JoinRelType.INNER
                && join.getJoinType() != JoinRelType.SEMI) {
            return false;
        }

        JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.leftKeys.isEmpty()) {
            return false;
        }

        // Judge whether dimSide meets the conditions of dimSide.
        if (!isDimSide(dimSide)) {
            return false;
        }

        TableSourceTable tableSourceTable = factScan.getTable().unwrap(TableSourceTable.class);
        if (tableSourceTable == null) {
            return false;
        }
        CatalogTable catalogTable = tableSourceTable.contextResolvedTable().getTable();
        List<String> partitionKeys = catalogTable.getPartitionKeys();
        if (partitionKeys.isEmpty()) {
            return false;
        }
        DynamicTableSource tableSource = tableSourceTable.tableSource();
        if (!(tableSource instanceof SupportsDynamicFiltering)) {
            return false;
        }

        List<Integer> factJoinKeys = factInLeft ? joinInfo.leftKeys : joinInfo.rightKeys;

        List<Integer> acceptedFieldIndices =
                getAcceptedFieldIndices(factJoinKeys, factCalc, factScan, tableSource);

        return !acceptedFieldIndices.isEmpty();
    }

    private static List<Integer> getAcceptedFieldIndices(
            List<Integer> factJoinKeys,
            @Nullable BatchPhysicalCalc factCalc,
            BatchPhysicalTableSourceScan factScan,
            DynamicTableSource tableSource) {
        List<String> candidateFields;
        if (factCalc == null) {
            candidateFields =
                    factJoinKeys.stream()
                            .map(i -> factScan.getRowType().getFieldNames().get(i))
                            .collect(Collectors.toList());
        } else {
            // Changing the fact key index in fact table calc output to fact key index in fact
            // table, and filtering these fields that computing in calc node.
            RexProgram origProgram = factCalc.getProgram();
            List<Integer> joinKeysIndexInFactTable = new ArrayList<>();
            List<RexNode> projectInJoinKeyList =
                    factJoinKeys.stream()
                            .map(
                                    i ->
                                            origProgram.expandLocalRef(
                                                    origProgram.getProjectList().get(i)))
                            .collect(Collectors.toList());

            for (RexNode node : projectInJoinKeyList) {
                if (node instanceof RexInputRef) {
                    joinKeysIndexInFactTable.add(((RexInputRef) node).getIndex());
                }
            }

            if (joinKeysIndexInFactTable.isEmpty()) {
                return Collections.emptyList();
            }

            candidateFields =
                    joinKeysIndexInFactTable.stream()
                            .map(i -> factScan.getRowType().getFieldNames().get(i))
                            .collect(Collectors.toList());
        }

        List<String> acceptedFields =
                ((SupportsDynamicFiltering) tableSource).applyDynamicFiltering(candidateFields);

        for (String field : acceptedFields) {
            if (!candidateFields.contains(field)) {
                throw new TableException(
                        String.format(
                                "Field %s not in join key, please verify the applyDynamicFiltering method In %s",
                                field, tableSource.asSummaryString()));
            }
        }

        return acceptedFields.stream()
                .map(f -> factScan.getRowType().getFieldNames().indexOf(f))
                .collect(Collectors.toList());
    }

    private static boolean isDimSide(RelNode rel) {
        DppDimSideFactors dimSideFactors = new DppDimSideFactors();
        visitDimSide(rel, dimSideFactors);
        return dimSideFactors.isDimSide();
    }

    /**
     * Visit dim side to judge whether dim side has filter condition and whether dim side's source
     * table scan is non partitioned scan.
     */
    private static void visitDimSide(RelNode rel, DppDimSideFactors dimSideFactors) {
        // TODO Let visitDimSide more efficient and more accurate. Like a filter on dim table or a
        // filter for the partition field on fact table.
        if (rel instanceof TableScan) {
            TableScan scan = (TableScan) rel;
            TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
            if (table == null) {
                return;
            }
            if (!dimSideFactors.hasFilter
                    && table.abilitySpecs() != null
                    && table.abilitySpecs().length != 0) {
                for (SourceAbilitySpec spec : table.abilitySpecs()) {
                    if (spec instanceof FilterPushDownSpec) {
                        List<RexNode> predicates = ((FilterPushDownSpec) spec).getPredicates();
                        for (RexNode predicate : predicates) {
                            if (isSuitableFilter(predicate)) {
                                dimSideFactors.hasFilter = true;
                            }
                        }
                    }
                }
            }

            CatalogTable catalogTable = table.contextResolvedTable().getTable();
            dimSideFactors.hasNonPartitionedScan = !catalogTable.isPartitioned();
        } else if (rel instanceof HepRelVertex) {
            visitDimSide(((HepRelVertex) rel).getCurrentRel(), dimSideFactors);
        } else if (rel instanceof Exchange || rel instanceof Project) {
            visitDimSide(rel.getInput(0), dimSideFactors);
        } else if (rel instanceof Calc) {
            RexProgram origProgram = ((Calc) rel).getProgram();
            if (origProgram.getCondition() != null
                    && isSuitableFilter(origProgram.expandLocalRef(origProgram.getCondition()))) {
                dimSideFactors.hasFilter = true;
            }
            visitDimSide(rel.getInput(0), dimSideFactors);
        } else if (rel instanceof Filter) {
            if (isSuitableFilter(((Filter) rel).getCondition())) {
                dimSideFactors.hasFilter = true;
            }
            visitDimSide(rel.getInput(0), dimSideFactors);
        }
    }

    /**
     * Not all filter condition suitable for using to filter partitions by dynamic partition pruning
     * rules. For example, NOT NULL can only filter one default partition which have a small impact
     * on filtering data.
     */
    private static boolean isSuitableFilter(RexNode filterCondition) {
        switch (filterCondition.getKind()) {
            case AND:
                List<RexNode> conjunctions = RelOptUtil.conjunctions(filterCondition);
                return isSuitableFilter(conjunctions.get(0))
                        || isSuitableFilter(conjunctions.get(1));
            case OR:
                List<RexNode> disjunctions = RelOptUtil.disjunctions(filterCondition);
                return isSuitableFilter(disjunctions.get(0))
                        && isSuitableFilter(disjunctions.get(1));
            case NOT:
                return isSuitableFilter(((RexCall) filterCondition).operands.get(0));
            case EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case NOT_EQUALS:
            case IN:
            case LIKE:
            case CONTAINS:
            case SEARCH:
            case IS_FALSE:
            case IS_NOT_FALSE:
            case IS_NOT_TRUE:
            case IS_TRUE:
                // TODO adding more suitable filters which can filter enough partitions after using
                // this filter in dynamic partition pruning.
                return true;
            default:
                return false;
        }
    }

    private static class DppDimSideFactors {
        private boolean hasFilter;
        private boolean hasNonPartitionedScan;

        public boolean isDimSide() {
            return hasFilter && hasNonPartitionedScan;
        }
    }

    protected BatchPhysicalDynamicFilteringTableSourceScan createDynamicFilteringTableSourceScan(
            BatchPhysicalTableSourceScan factScan,
            BatchPhysicalRel dimSide,
            BatchPhysicalJoinBase join,
            @Nullable BatchPhysicalCalc factCalc,
            boolean factInLeft) {
        JoinInfo joinInfo = join.analyzeCondition();
        TableSourceTable tableSourceTable = factScan.getTable().unwrap(TableSourceTable.class);
        DynamicTableSource tableSource = tableSourceTable.tableSource();

        List<Integer> factJoinKeys = factInLeft ? joinInfo.leftKeys : joinInfo.rightKeys;
        List<Integer> dimJoinKeys = factInLeft ? joinInfo.rightKeys : joinInfo.leftKeys;

        List<Integer> acceptedFieldIndices =
                getAcceptedFieldIndices(factJoinKeys, factCalc, factScan, tableSource);

        List<Integer> dynamicFilteringFieldIndices = new ArrayList<>();
        for (int i = 0; i < joinInfo.leftKeys.size(); ++i) {
            if (acceptedFieldIndices.contains(factJoinKeys.get(i))) {
                dynamicFilteringFieldIndices.add(dimJoinKeys.get(i));
            }
        }
        final BatchPhysicalDynamicFilteringDataCollector dynamicFilteringDataCollector =
                createDynamicFilteringConnector(dimSide, dynamicFilteringFieldIndices);
        return new BatchPhysicalDynamicFilteringTableSourceScan(
                factScan.getCluster(),
                factScan.getTraitSet(),
                factScan.getHints(),
                factScan.tableSourceTable(),
                dynamicFilteringDataCollector);
    }

    private BatchPhysicalDynamicFilteringDataCollector createDynamicFilteringConnector(
            RelNode dimSide, List<Integer> dynamicFilteringFieldIndices) {
        final RelDataType outputType =
                ((FlinkTypeFactory) dimSide.getCluster().getTypeFactory())
                        .projectStructType(
                                dimSide.getRowType(),
                                dynamicFilteringFieldIndices.stream().mapToInt(i -> i).toArray());

        return new BatchPhysicalDynamicFilteringDataCollector(
                dimSide.getCluster(),
                dimSide.getTraitSet(),
                ignoreExchange(dimSide),
                outputType,
                dynamicFilteringFieldIndices.stream().mapToInt(i -> i).toArray());
    }

    private RelNode ignoreExchange(RelNode dimSide) {
        if (dimSide instanceof Exchange) {
            return dimSide.getInput(0);
        } else {
            return dimSide;
        }
    }

    /** Simple dynamic filtering pattern with fact side in join right. */
    protected static class DynamicPartitionPruningFactInRightRule
            extends DynamicPartitionPruningRule {

        public DynamicPartitionPruningFactInRightRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            Config DEFAULT =
                    EMPTY.withOperandSupplier(
                                    b0 ->
                                            b0.operand(BatchPhysicalJoinBase.class)
                                                    .inputs(
                                                            l ->
                                                                    l.operand(
                                                                                    BatchPhysicalRel
                                                                                            .class)
                                                                            .anyInputs(),
                                                            r ->
                                                                    r.operand(
                                                                                    BatchPhysicalTableSourceScan
                                                                                            .class)
                                                                            .noInputs()))
                            .as(DynamicPartitionPruningFactInRightRule.Config.class);

            @Override
            default DynamicPartitionPruningFactInRightRule toRule() {
                return new DynamicPartitionPruningFactInRightRule(this);
            }
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalRel dimSide = call.rel(1);
            final BatchPhysicalTableSourceScan factScan = call.rel(2);
            return doMatches(join, dimSide, null, factScan, false);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalRel dimSide = call.rel(1);
            final BatchPhysicalTableSourceScan factScan = call.rel(2);

            final BatchPhysicalDynamicFilteringTableSourceScan newFactScan =
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, null, false);
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(dimSide, newFactScan));
            call.transformTo(newJoin);
        }
    }

    /** Simple dynamic filtering pattern with fact side in join left. */
    protected static class DynamicPartitionPruningFactInLeftRule
            extends DynamicPartitionPruningRule {

        public DynamicPartitionPruningFactInLeftRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicPartitionPruningFactInLeftRule toRule() {
                return new DynamicPartitionPruningFactInLeftRule(this);
            }

            Config DEFAULT =
                    EMPTY.withOperandSupplier(
                                    b0 ->
                                            b0.operand(BatchPhysicalJoinBase.class)
                                                    .inputs(
                                                            l ->
                                                                    l.operand(
                                                                                    BatchPhysicalTableSourceScan
                                                                                            .class)
                                                                            .noInputs(),
                                                            r ->
                                                                    r.operand(
                                                                                    BatchPhysicalRel
                                                                                            .class)
                                                                            .anyInputs()))
                            .as(DynamicPartitionPruningFactInLeftRule.Config.class);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalTableSourceScan factScan = call.rel(1);
            final BatchPhysicalRel dimSide = call.rel(2);
            return doMatches(join, dimSide, null, factScan, true);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalTableSourceScan factScan = call.rel(1);
            final BatchPhysicalRel dimSide = call.rel(2);

            final BatchPhysicalDynamicFilteringTableSourceScan newFactScan =
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, null, true);
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(newFactScan, dimSide));
            call.transformTo(newJoin);
        }
    }

    /** Dynamic filtering pattern with exchange node in fact side while fact side in join right. */
    protected static class DynamicPartitionPruningFactInRightWithExchangeRule
            extends DynamicPartitionPruningRule {

        public DynamicPartitionPruningFactInRightWithExchangeRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicPartitionPruningFactInRightWithExchangeRule toRule() {
                return new DynamicPartitionPruningFactInRightWithExchangeRule(this);
            }

            Config DEFAULT =
                    EMPTY.withOperandSupplier(
                                    b0 ->
                                            b0.operand(BatchPhysicalJoinBase.class)
                                                    .inputs(
                                                            l ->
                                                                    l.operand(
                                                                                    BatchPhysicalRel
                                                                                            .class)
                                                                            .anyInputs(),
                                                            r ->
                                                                    r.operand(
                                                                                    BatchPhysicalExchange
                                                                                            .class)
                                                                            .oneInput(
                                                                                    e ->
                                                                                            e.operand(
                                                                                                            BatchPhysicalTableSourceScan
                                                                                                                    .class)
                                                                                                    .noInputs())))
                            .as(DynamicPartitionPruningFactInRightWithExchangeRule.Config.class);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalRel dimSide = call.rel(1);
            final BatchPhysicalTableSourceScan factScan = call.rel(3);
            return doMatches(join, dimSide, null, factScan, false);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalRel dimSide = call.rel(1);
            final BatchPhysicalExchange exchange = call.rel(2);
            final BatchPhysicalTableSourceScan factScan = call.rel(3);

            final BatchPhysicalDynamicFilteringTableSourceScan newFactScan =
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, null, false);
            final BatchPhysicalExchange newExchange =
                    (BatchPhysicalExchange)
                            exchange.copy(
                                    exchange.getTraitSet(), Collections.singletonList(newFactScan));
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(dimSide, newExchange));
            call.transformTo(newJoin);
        }
    }

    /** Dynamic filtering pattern with exchange node in fact side while fact side in join left. */
    protected static class DynamicPartitionPruningFactInLeftWithExchangeRule
            extends DynamicPartitionPruningRule {

        public DynamicPartitionPruningFactInLeftWithExchangeRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicPartitionPruningFactInLeftWithExchangeRule toRule() {
                return new DynamicPartitionPruningFactInLeftWithExchangeRule(this);
            }

            Config DEFAULT =
                    EMPTY.withOperandSupplier(
                                    b0 ->
                                            b0.operand(BatchPhysicalJoinBase.class)
                                                    .inputs(
                                                            l ->
                                                                    l.operand(
                                                                                    BatchPhysicalExchange
                                                                                            .class)
                                                                            .oneInput(
                                                                                    e ->
                                                                                            e.operand(
                                                                                                            BatchPhysicalTableSourceScan
                                                                                                                    .class)
                                                                                                    .noInputs()),
                                                            r ->
                                                                    r.operand(
                                                                                    BatchPhysicalRel
                                                                                            .class)
                                                                            .anyInputs()))
                            .as(DynamicPartitionPruningFactInLeftWithExchangeRule.Config.class);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalTableSourceScan factScan = call.rel(2);
            final BatchPhysicalRel dimSide = call.rel(3);
            return doMatches(join, dimSide, null, factScan, true);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalExchange exchange = call.rel(1);
            final BatchPhysicalTableSourceScan factScan = call.rel(2);
            final BatchPhysicalRel dimSide = call.rel(3);

            final BatchPhysicalDynamicFilteringTableSourceScan newFactScan =
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, null, true);
            final BatchPhysicalExchange newExchange =
                    (BatchPhysicalExchange)
                            exchange.copy(
                                    exchange.getTraitSet(), Collections.singletonList(newFactScan));
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(newExchange, dimSide));
            call.transformTo(newJoin);
        }
    }

    /** Dynamic filtering pattern with calc node in fact side while fact side in join right. */
    protected static class DynamicPartitionPruningFactInRightWithCalcRule
            extends DynamicPartitionPruningRule {

        public DynamicPartitionPruningFactInRightWithCalcRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicPartitionPruningFactInRightWithCalcRule toRule() {
                return new DynamicPartitionPruningFactInRightWithCalcRule(this);
            }

            Config DEFAULT =
                    EMPTY.withOperandSupplier(
                                    b0 ->
                                            b0.operand(BatchPhysicalJoinBase.class)
                                                    .inputs(
                                                            l ->
                                                                    l.operand(
                                                                                    BatchPhysicalRel
                                                                                            .class)
                                                                            .anyInputs(),
                                                            r ->
                                                                    r.operand(
                                                                                    BatchPhysicalCalc
                                                                                            .class)
                                                                            .oneInput(
                                                                                    f ->
                                                                                            f.operand(
                                                                                                            BatchPhysicalTableSourceScan
                                                                                                                    .class)
                                                                                                    .noInputs())))
                            .as(DynamicPartitionPruningFactInRightWithCalcRule.Config.class);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalRel dimSide = call.rel(1);
            final BatchPhysicalCalc factCalc = call.rel(2);
            final BatchPhysicalTableSourceScan factScan = call.rel(3);
            return doMatches(join, dimSide, factCalc, factScan, false);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalRel dimSide = call.rel(1);
            final BatchPhysicalCalc factCalc = call.rel(2);
            final BatchPhysicalTableSourceScan factScan = call.rel(3);

            final BatchPhysicalDynamicFilteringTableSourceScan newFactScan =
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, factCalc, false);
            final BatchPhysicalCalc newCalc =
                    (BatchPhysicalCalc)
                            factCalc.copy(
                                    factCalc.getTraitSet(), newFactScan, factCalc.getProgram());
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(dimSide, newCalc));
            call.transformTo(newJoin);
        }
    }

    /** Dynamic filtering pattern with calc node in fact side while fact side in join left. */
    protected static class DynamicPartitionPruningFactInLeftWithCalcRule
            extends DynamicPartitionPruningRule {

        public DynamicPartitionPruningFactInLeftWithCalcRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicPartitionPruningFactInLeftWithCalcRule toRule() {
                return new DynamicPartitionPruningFactInLeftWithCalcRule(this);
            }

            Config DEFAULT =
                    EMPTY.withOperandSupplier(
                                    b0 ->
                                            b0.operand(BatchPhysicalJoinBase.class)
                                                    .inputs(
                                                            l ->
                                                                    l.operand(
                                                                                    BatchPhysicalCalc
                                                                                            .class)
                                                                            .oneInput(
                                                                                    f ->
                                                                                            f.operand(
                                                                                                            BatchPhysicalTableSourceScan
                                                                                                                    .class)
                                                                                                    .noInputs()),
                                                            r ->
                                                                    r.operand(
                                                                                    BatchPhysicalRel
                                                                                            .class)
                                                                            .anyInputs()))
                            .as(DynamicPartitionPruningFactInLeftWithCalcRule.Config.class);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalCalc factCalc = call.rel(1);
            final BatchPhysicalTableSourceScan factScan = call.rel(2);
            final BatchPhysicalRel dimSide = call.rel(3);
            return doMatches(join, dimSide, factCalc, factScan, true);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalCalc factCalc = call.rel(1);
            final BatchPhysicalTableSourceScan factScan = call.rel(2);
            final BatchPhysicalRel dimSide = call.rel(3);

            final BatchPhysicalDynamicFilteringTableSourceScan newFactScan =
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, factCalc, true);
            final BatchPhysicalCalc newCalc =
                    (BatchPhysicalCalc)
                            factCalc.copy(
                                    factCalc.getTraitSet(), newFactScan, factCalc.getProgram());
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(newCalc, dimSide));
            call.transformTo(newJoin);
        }
    }

    /**
     * Dynamic filtering pattern with exchange node and calc node in fact side while fact side in
     * join right.
     */
    protected static class DynamicPartitionPruningFactInRightWithExchangeAndCalcRule
            extends DynamicPartitionPruningRule {

        public DynamicPartitionPruningFactInRightWithExchangeAndCalcRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicPartitionPruningFactInRightWithExchangeAndCalcRule toRule() {
                return new DynamicPartitionPruningFactInRightWithExchangeAndCalcRule(this);
            }

            Config DEFAULT =
                    EMPTY.withOperandSupplier(
                                    b0 ->
                                            b0.operand(BatchPhysicalJoinBase.class)
                                                    .inputs(
                                                            l ->
                                                                    l.operand(
                                                                                    BatchPhysicalRel
                                                                                            .class)
                                                                            .anyInputs(),
                                                            r ->
                                                                    r.operand(
                                                                                    BatchPhysicalExchange
                                                                                            .class)
                                                                            .oneInput(
                                                                                    e ->
                                                                                            e.operand(
                                                                                                            BatchPhysicalCalc
                                                                                                                    .class)
                                                                                                    .oneInput(
                                                                                                            f ->
                                                                                                                    f.operand(
                                                                                                                                    BatchPhysicalTableSourceScan
                                                                                                                                            .class)
                                                                                                                            .noInputs()))))
                            .as(
                                    DynamicPartitionPruningFactInRightWithExchangeAndCalcRule.Config
                                            .class);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalRel dimSide = call.rel(1);
            final BatchPhysicalCalc factCalc = call.rel(3);
            final BatchPhysicalTableSourceScan factScan = call.rel(4);
            return doMatches(join, dimSide, factCalc, factScan, false);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalRel dimSide = call.rel(1);
            final BatchPhysicalExchange exchange = call.rel(2);
            final BatchPhysicalCalc factCalc = call.rel(3);
            final BatchPhysicalTableSourceScan factScan = call.rel(4);

            final BatchPhysicalDynamicFilteringTableSourceScan newFactScan =
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, factCalc, false);
            final BatchPhysicalExchange newExchange =
                    (BatchPhysicalExchange)
                            exchange.copy(
                                    exchange.getTraitSet(), Collections.singletonList(newFactScan));
            final BatchPhysicalCalc newCalc =
                    (BatchPhysicalCalc)
                            factCalc.copy(
                                    factCalc.getTraitSet(), newExchange, factCalc.getProgram());
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(dimSide, newCalc));
            call.transformTo(newJoin);
        }
    }

    /**
     * Dynamic filtering pattern with exchange node and calc node in fact side while fact side in
     * join left.
     */
    protected static class DynamicPartitionPruningFactInLeftWithExchangeAndCalcRule
            extends DynamicPartitionPruningRule {

        public DynamicPartitionPruningFactInLeftWithExchangeAndCalcRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicPartitionPruningFactInLeftWithExchangeAndCalcRule toRule() {
                return new DynamicPartitionPruningFactInLeftWithExchangeAndCalcRule(this);
            }

            Config DEFAULT =
                    EMPTY.withOperandSupplier(
                                    b0 ->
                                            b0.operand(BatchPhysicalJoinBase.class)
                                                    .inputs(
                                                            l ->
                                                                    l.operand(
                                                                                    BatchPhysicalExchange
                                                                                            .class)
                                                                            .oneInput(
                                                                                    e ->
                                                                                            e.operand(
                                                                                                            BatchPhysicalCalc
                                                                                                                    .class)
                                                                                                    .oneInput(
                                                                                                            f ->
                                                                                                                    f.operand(
                                                                                                                                    BatchPhysicalTableSourceScan
                                                                                                                                            .class)
                                                                                                                            .noInputs())),
                                                            r ->
                                                                    r.operand(
                                                                                    BatchPhysicalRel
                                                                                            .class)
                                                                            .anyInputs()))
                            .as(
                                    DynamicPartitionPruningFactInLeftWithExchangeAndCalcRule.Config
                                            .class);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalCalc factCalc = call.rel(2);
            final BatchPhysicalTableSourceScan factScan = call.rel(3);
            final BatchPhysicalRel dimSide = call.rel(4);
            return doMatches(join, dimSide, factCalc, factScan, true);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final BatchPhysicalJoinBase join = call.rel(0);
            final BatchPhysicalExchange exchange = call.rel(1);
            final BatchPhysicalCalc factCalc = call.rel(2);
            final BatchPhysicalTableSourceScan factScan = call.rel(3);
            final BatchPhysicalRel dimSide = call.rel(4);

            final BatchPhysicalDynamicFilteringTableSourceScan newFactScan =
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, factCalc, true);
            final BatchPhysicalExchange newExchange =
                    (BatchPhysicalExchange)
                            exchange.copy(
                                    exchange.getTraitSet(), Collections.singletonList(newFactScan));
            final BatchPhysicalCalc newCalc =
                    (BatchPhysicalCalc)
                            factCalc.copy(
                                    factCalc.getTraitSet(), newExchange, factCalc.getProgram());
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(newCalc, dimSide));
            call.transformTo(newJoin);
        }
    }
}
