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

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
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
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED;

/** Rules for dynamic filtering. */
public abstract class DynamicFilteringRule extends RelRule<RelRule.Config> {

    public static final RuleSet DYNAMIC_FILTERING_RULES =
            RuleSets.ofList(
                    DynamicFilteringFactInRightRule.DynamicFilteringFactInRightRule.Config.EMPTY
                            .withDescription("DynamicFilteringRule:factInRight")
                            .as(DynamicFilteringFactInRightRule.Config.class)
                            .factInRight()
                            .toRule(),
                    DynamicFilteringFactInLeftRule.DynamicFilteringFactInLeftRule.Config.EMPTY
                            .withDescription("DynamicFilteringRule:factInLeft")
                            .as(DynamicFilteringFactInLeftRule.Config.class)
                            .factInLeft()
                            .toRule(),
                    DynamicFilteringFactInRightWithExchangeRule.Config.EMPTY
                            .withDescription("DynamicFilteringRule:factInRightWithExchange")
                            .as(DynamicFilteringFactInRightWithExchangeRule.Config.class)
                            .factInRight()
                            .toRule(),
                    DynamicFilteringFactInLeftWithExchangeRule.Config.EMPTY
                            .withDescription("DynamicFilteringRule:factInLeftWithExchange")
                            .as(DynamicFilteringFactInLeftWithExchangeRule.Config.class)
                            .factInLeft()
                            .toRule(),
                    DynamicFilteringFactInRightWithCalcRule.Config.EMPTY
                            .withDescription("DynamicFilteringRule:factInRightWithCalc")
                            .as(DynamicFilteringFactInRightWithCalcRule.Config.class)
                            .factInRight()
                            .toRule(),
                    DynamicFilteringFactInLeftWithCalcRule.Config.EMPTY
                            .withDescription("DynamicFilteringRule:factInLeftWithCalc")
                            .as(DynamicFilteringFactInLeftWithCalcRule.Config.class)
                            .factInLeft()
                            .toRule(),
                    DynamicFilteringFactInRightWithExchangeAndCalcRule.Config.EMPTY
                            .withDescription("DynamicFilteringRule:factInRightWithExchangeAndCalc")
                            .as(DynamicFilteringFactInRightWithExchangeAndCalcRule.Config.class)
                            .factInRight()
                            .toRule(),
                    DynamicFilteringFactInLeftWithExchangeAndCalcRule.Config.EMPTY
                            .withDescription("DynamicFilteringRule:factInLeftWithExchangeAndCalc")
                            .as(DynamicFilteringFactInLeftWithExchangeAndCalcRule.Config.class)
                            .factInLeft()
                            .toRule());

    public DynamicFilteringRule(RelRule.Config config) {
        super(config);
    }

    public boolean doMatches(
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

        List<String> candidateFields =
                factJoinKeys.stream()
                        .map(i -> factScan.getRowType().getFieldNames().get(i))
                        .collect(Collectors.toList());
        List<String> acceptedFields =
                ((SupportsDynamicFiltering) tableSource).applyDynamicFiltering(candidateFields);

        if (acceptedFields.isEmpty()) {
            return false;
        }

        // Judge whether there are computes column on accepted fields. If there are, dynamic
        // filtering cannot be generated.
        List<Integer> acceptedFieldIndices =
                acceptedFields.stream()
                        .map(f -> factScan.getRowType().getFieldNames().indexOf(f))
                        .collect(Collectors.toList());
        return factCalc == null || !computeColumnOnAcceptedFields(acceptedFieldIndices, factCalc);
    }

    /**
     * Judge whether there are computes column on accepted fields. If there are, dynamic filtering
     * cannot be generated.
     */
    private static boolean computeColumnOnAcceptedFields(
            List<Integer> indexList, BatchPhysicalCalc factCalc) {
        List<RexLocalRef> projectList = factCalc.getProgram().getProjectList();
        for (int index : indexList) {
            if (projectList.get(index).getIndex() != index) {
                return true;
            }
        }
        return false;
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
        if (rel instanceof TableScan) {
            TableScan scan = (TableScan) rel;
            TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
            if (table == null) {
                return;
            }
            if (Arrays.stream(table.abilitySpecs())
                    .anyMatch(spec -> spec instanceof FilterPushDownSpec)) {
                dimSideFactors.hasFilter = true;
            }

            CatalogTable catalogTable = table.contextResolvedTable().getTable();
            dimSideFactors.hasNonPartitionedScan = !catalogTable.isPartitioned();
        } else if (rel instanceof HepRelVertex) {
            visitDimSide(((HepRelVertex) rel).getCurrentRel(), dimSideFactors);
        } else if (rel instanceof Exchange || rel instanceof Project) {
            visitDimSide(rel.getInput(0), dimSideFactors);
        } else if (rel instanceof Calc) {
            if (((Calc) rel).getProgram().getCondition() != null) {
                dimSideFactors.hasFilter = true;
            }
            visitDimSide(rel.getInput(0), dimSideFactors);
        } else if (rel instanceof Filter && ((Filter) rel).getCondition() != null) {
            dimSideFactors.hasFilter = true;
            visitDimSide(rel.getInput(0), dimSideFactors);
        } else {
            rel.getInputs().forEach(n -> visitDimSide(n, dimSideFactors));
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
            boolean factInLeft) {
        JoinInfo joinInfo = join.analyzeCondition();
        TableSourceTable tableSourceTable = factScan.getTable().unwrap(TableSourceTable.class);
        DynamicTableSource tableSource = tableSourceTable.tableSource();

        List<Integer> factJoinKeys = factInLeft ? joinInfo.leftKeys : joinInfo.rightKeys;
        List<Integer> dimJoinKeys = factInLeft ? joinInfo.rightKeys : joinInfo.leftKeys;
        List<String> candidateFields =
                factJoinKeys.stream()
                        .map(i -> factScan.getRowType().getFieldNames().get(i))
                        .collect(Collectors.toList());
        List<String> acceptedFields =
                ((SupportsDynamicFiltering) tableSource).applyDynamicFiltering(candidateFields);
        if (acceptedFields.isEmpty()) {
            return null;
        }
        List<Integer> acceptedFieldIndices =
                acceptedFields.stream()
                        .map(f -> factScan.getRowType().getFieldNames().indexOf(f))
                        .collect(Collectors.toList());

        List<Integer> dynamicFilteringFieldIndices = new ArrayList<>();
        for (int i = 0; i < joinInfo.leftKeys.size(); ++i) {
            if (acceptedFieldIndices.contains(factJoinKeys.get(i))) {
                dynamicFilteringFieldIndices.add(dimJoinKeys.get(i));
            }
        }
        final BatchPhysicalDynamicFilteringDataCollector dppConnector =
                createDynamicFilteringConnector(dimSide, dynamicFilteringFieldIndices);
        return new BatchPhysicalDynamicFilteringTableSourceScan(
                factScan.getCluster(),
                factScan.getTraitSet(),
                factScan.getHints(),
                factScan.tableSourceTable(),
                dppConnector);
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
    protected static class DynamicFilteringFactInRightRule extends DynamicFilteringRule {

        public DynamicFilteringFactInRightRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicFilteringFactInRightRule toRule() {
                return new DynamicFilteringFactInRightRule(this);
            }

            default Config factInRight() {
                return withOperandSupplier(
                                b0 ->
                                        b0.operand(BatchPhysicalJoinBase.class)
                                                .inputs(
                                                        l ->
                                                                l.operand(BatchPhysicalRel.class)
                                                                        .anyInputs(),
                                                        r ->
                                                                r.operand(
                                                                                BatchPhysicalTableSourceScan
                                                                                        .class)
                                                                        .noInputs()))
                        .as(Config.class);
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
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, false);
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(dimSide, newFactScan));
            call.transformTo(newJoin);
        }
    }

    /** Simple dynamic filtering pattern with fact side in join left. */
    protected static class DynamicFilteringFactInLeftRule extends DynamicFilteringRule {

        public DynamicFilteringFactInLeftRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicFilteringFactInLeftRule toRule() {
                return new DynamicFilteringFactInLeftRule(this);
            }

            default Config factInLeft() {
                return withOperandSupplier(
                                b0 ->
                                        b0.operand(BatchPhysicalJoinBase.class)
                                                .inputs(
                                                        l ->
                                                                l.operand(
                                                                                BatchPhysicalTableSourceScan
                                                                                        .class)
                                                                        .noInputs(),
                                                        r ->
                                                                r.operand(BatchPhysicalRel.class)
                                                                        .anyInputs()))
                        .as(Config.class);
            }
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
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, true);
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(newFactScan, dimSide));
            call.transformTo(newJoin);
        }
    }

    /** Dynamic filtering pattern with exchange node in fact side while fact side in join right. */
    protected static class DynamicFilteringFactInRightWithExchangeRule
            extends DynamicFilteringRule {

        public DynamicFilteringFactInRightWithExchangeRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicFilteringFactInRightWithExchangeRule toRule() {
                return new DynamicFilteringFactInRightWithExchangeRule(this);
            }

            default Config factInRight() {
                return withOperandSupplier(
                                b0 ->
                                        b0.operand(BatchPhysicalJoinBase.class)
                                                .inputs(
                                                        l ->
                                                                l.operand(BatchPhysicalRel.class)
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
                        .as(Config.class);
            }
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
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, false);
            final BatchPhysicalExchange newExchange =
                    (BatchPhysicalExchange)
                            exchange.copy(
                                    exchange.getTraitSet(), Collections.singletonList(newFactScan));
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(dimSide, newExchange));
            call.transformTo(newJoin);
        }
    }

    /** Dynamic filtering pattern with exchange node in fact side while fact side in join left. */
    protected static class DynamicFilteringFactInLeftWithExchangeRule extends DynamicFilteringRule {

        public DynamicFilteringFactInLeftWithExchangeRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicFilteringFactInLeftWithExchangeRule toRule() {
                return new DynamicFilteringFactInLeftWithExchangeRule(this);
            }

            default Config factInLeft() {
                return withOperandSupplier(
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
                                                                r.operand(BatchPhysicalRel.class)
                                                                        .anyInputs()))
                        .as(Config.class);
            }
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
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, true);
            final BatchPhysicalExchange newExchange =
                    (BatchPhysicalExchange)
                            exchange.copy(
                                    exchange.getTraitSet(), Collections.singletonList(newFactScan));
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(newExchange, dimSide));
            call.transformTo(newJoin);
        }
    }

    /** Dynamic filtering pattern with calc node in fact side while fact side in join right. */
    protected static class DynamicFilteringFactInRightWithCalcRule extends DynamicFilteringRule {

        public DynamicFilteringFactInRightWithCalcRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicFilteringFactInRightWithCalcRule toRule() {
                return new DynamicFilteringFactInRightWithCalcRule(this);
            }

            default Config factInRight() {
                return withOperandSupplier(
                                b0 ->
                                        b0.operand(BatchPhysicalJoinBase.class)
                                                .inputs(
                                                        l ->
                                                                l.operand(BatchPhysicalRel.class)
                                                                        .anyInputs(),
                                                        r ->
                                                                r.operand(BatchPhysicalCalc.class)
                                                                        .oneInput(
                                                                                f ->
                                                                                        f.operand(
                                                                                                        BatchPhysicalTableSourceScan
                                                                                                                .class)
                                                                                                .noInputs())))
                        .as(Config.class);
            }
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
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, false);
            final BatchPhysicalCalc newCalc =
                    (BatchPhysicalCalc)
                            factCalc.copy(
                                    factCalc.getTraitSet(), newFactScan, factCalc.getProgram());
            final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(dimSide, newCalc));
            call.transformTo(newJoin);
        }
    }

    /** Dynamic filtering pattern with calc node in fact side while fact side in join left. */
    protected static class DynamicFilteringFactInLeftWithCalcRule extends DynamicFilteringRule {

        public DynamicFilteringFactInLeftWithCalcRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicFilteringFactInLeftWithCalcRule toRule() {
                return new DynamicFilteringFactInLeftWithCalcRule(this);
            }

            default Config factInLeft() {
                return withOperandSupplier(
                                b0 ->
                                        b0.operand(BatchPhysicalJoinBase.class)
                                                .inputs(
                                                        l ->
                                                                l.operand(BatchPhysicalCalc.class)
                                                                        .oneInput(
                                                                                f ->
                                                                                        f.operand(
                                                                                                        BatchPhysicalTableSourceScan
                                                                                                                .class)
                                                                                                .noInputs()),
                                                        r ->
                                                                r.operand(BatchPhysicalRel.class)
                                                                        .anyInputs()))
                        .as(Config.class);
            }
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
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, true);
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
    protected static class DynamicFilteringFactInRightWithExchangeAndCalcRule
            extends DynamicFilteringRule {

        public DynamicFilteringFactInRightWithExchangeAndCalcRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicFilteringFactInRightWithExchangeAndCalcRule toRule() {
                return new DynamicFilteringFactInRightWithExchangeAndCalcRule(this);
            }

            default Config factInRight() {
                return withOperandSupplier(
                                b0 ->
                                        b0.operand(BatchPhysicalJoinBase.class)
                                                .inputs(
                                                        l ->
                                                                l.operand(BatchPhysicalRel.class)
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
                        .as(Config.class);
            }
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
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, false);
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
    protected static class DynamicFilteringFactInLeftWithExchangeAndCalcRule
            extends DynamicFilteringRule {

        public DynamicFilteringFactInLeftWithExchangeAndCalcRule(RelRule.Config config) {
            super(config);
        }

        /** Config. */
        public interface Config extends RelRule.Config {
            @Override
            default DynamicFilteringFactInLeftWithExchangeAndCalcRule toRule() {
                return new DynamicFilteringFactInLeftWithExchangeAndCalcRule(this);
            }

            default Config factInLeft() {
                return withOperandSupplier(
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
                                                                r.operand(BatchPhysicalRel.class)
                                                                        .anyInputs()))
                        .as(Config.class);
            }
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
                    createDynamicFilteringTableSourceScan(factScan, dimSide, join, true);
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
