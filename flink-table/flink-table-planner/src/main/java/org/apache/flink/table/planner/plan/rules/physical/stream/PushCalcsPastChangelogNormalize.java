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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;

import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.AbstractTargetMapping;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelDistribution;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.Stack;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.planner.plan.utils.RexNodeExtractor.extractRefInputFields;

/**
 * We want to optimize state size for ChanglogNormalize by removing unused column
 * references from the plan. Before this optimization, all columns of a TableSource
 * would be used as inputs to ChanglogNormalize with projectctions (Calc) nodes
 * coming after the ChangelogNormalize.
 * 
 * By reducing the columns going into ChangelogNormalize we can accomplish the
 * following:
 *   1. Reduction of state used up by ChanegelogNormalize
 *   2. Supressing emitting events when rows with used columns did not change.
 * 
 * The second optimization is particularly impactful since we have customers where
 * an ORM continuously updates some field (e.g. last_updated_ts), but none of the
 * columns used by the query are changed. In that condition, we want to prevent
 * ChangelogNormalize from emitting an event which would effectively retract and
 * then update the exact same value, causing a lot of extra work in the pipeline.
 * 
 * The following optimization needs to maintain state, so the standard HepOptimizer
 * rule rewrites are not sufficient.
 *
 * The optimization works as follows:
 *   1. Find all of the TableSourceScans that have a ChangelogNormalizer and Calc.
 *   2. For each TableSourceScan, maintain a list of all used columns.
 *   3. In cases where not all columns are used, insert a new Calc node before
 *      the ChangelogNormalizer that projects only the used columns.
 * 
 * Note: It's very important to compute step 2 so that we do not undermine the
 *  reuse subgraph optimization. We need to make sure that the subtrees under
 *  each ChangelogNormalizer are the same (i.e. have the same Calc projection).
 * 
 * See the example execution plan below for the following query:
 *  select o1.order_item_id, o1.product_id, o2.merchant_id
 *    from order_items o1
 *    join order_items o2 on o1.order_item_id = o2.order_item_id
 *   where o1.product_name = '123';
 * 
 * Notice how the right side of the join has a Reused(reference_id=[1]) pointer to
 * the left hand sise ChangelogNormalize. And that part of the tree hgas a Calc
 * node before the Exchange that projects the columns from `order_items` referenced
 * by both sides of the join.
 * 
 * == Optimized Execution Plan ==
 * Calc(select=[order_item_id, product_id, merchant_id])
 * +- Join(joinType=[InnerJoin], where=[(order_item_id = $t0)], select=[order_item_id, product_id, $t0, $t1], leftInputSpec=[JoinKeyContainsUniqueKey], rightInputSpec=[JoinKeyContainsUniqueKey])
 *    :- Exchange(distribution=[hash[$t0]])
 *    :  +- Calc(select=[order_item_id AS $t0, product_id AS $t3], where=[(product_name = '123')])
 *    :     +- ChangelogNormalize(key=[order_item_id])(reuse_id=[1])
 *    :        +- Exchange(distribution=[hash[order_item_id]])
 *    :           +- Calc(select=[order_item_id, merchant_id, product_id, product_name])
 *    :              +- DropUpdateBefore
 *    :                 +- TableSourceScan(table=[[9739e980-6aa7-4817-9d61-8c00fd27c970, source_1, order_items]], fields=[order_item_id, merchant_id, order_id, product_id, category, product_name, quantity, price, order_time])
 *    +- Exchange(distribution=[hash[$t0]])
 *       +- Calc(select=[order_item_id AS $t0, merchant_id AS $t1])
 *          +- Reused(reference_id=[1])
 */

@Internal
public class PushCalcsPastChangelogNormalize {

    private static final Logger LOG = LoggerFactory.getLogger(PushCalcsPastChangelogNormalize.class);

    /**
     * Called by the planner after all of the Physical plan optimizations have been
     * performed but right before the subplan reuse code.
     * 
     * @param builder
     * @param inputs
     * @return new list of inputs
     */
    public static List<RelNode> optimize(RelBuilder builder, List<RelNode> inputs) {
        ArrayList<RelNode> newInputs = new ArrayList<RelNode>(inputs.size());
        for (RelNode input : inputs) {
            PushCalcsVisitor shuttle = new PushCalcsVisitor();
            shuttle.go(input);
            RelNode newInput = shuttle.transform(input, builder);
            newInputs.add(newInput);
        }
        return newInputs;
    }

    static class PushCalcsVisitor extends RelVisitor {
        private Stack<RelNode> stack = new Stack<RelNode>();
        private HashMap<StreamPhysicalTableSourceScan, boolean[]> usedColumnsBySource = new HashMap<>();
        private List<ArrayList<RelNode>> matches = new ArrayList<ArrayList<RelNode>>();

        public PushCalcsVisitor() {
            super();
        }

        /**
         * Walk the entire physical plan looking for TableSourceScan nodes. For every
         * such node, maintain a set of used columns from the source. The set consists
         * of a boolean array indexed by the ordinal position of the column in the source
         * with the boolean value indicating whether the column is used. Note how the code
         * reuses the boolean array for duplicate table sources and maintains a global set
         * of all used columns.
         */
        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            stack.push(node);
            if (node instanceof StreamPhysicalTableSourceScan) {
                StreamPhysicalTableSourceScan source = (StreamPhysicalTableSourceScan) node;
                boolean[] usedColumns = findUsedColumns(source, stack);
                boolean[] cachedUsedColumns;
                if (usedColumnsBySource.containsKey(source)) {
                    cachedUsedColumns = usedColumnsBySource.get(source);
                } else {
                    cachedUsedColumns = new boolean[usedColumns.length];
                    usedColumnsBySource.put(source, cachedUsedColumns);
                }
                for (int i = 0; i < usedColumns.length; i++) {
                    if (usedColumns[i]) {
                        cachedUsedColumns[i] = true;
                    }
                }
            }
            super.visit(node, ordinal, parent);
            stack.pop();
        }

        private ArrayList<RelNode> pathMatches(Object[] matchConfig, Stack<RelNode> path) {
            if (path.size() < matchConfig.length) {
                return null;
            }

            ArrayList<RelNode> match = new ArrayList<RelNode>(matchConfig.length);
            path = (Stack<RelNode>) path.clone();
            RelNode node = null;
            for (int i = 0; i < matchConfig.length; i++) {
                node = path.pop();
                if (node.getClass() != matchConfig[i]) {
                    return null;
                }
                match.add(node);
            }

            return match;
        }

        private void computeUsedColumns(StreamPhysicalCalc calc, StreamPhysicalChangelogNormalize changelogNormalize, boolean[] usedColumns) {
            // Create a union list of fields between the projected columns and unique keys
            final RelDataType inputRowType = changelogNormalize.getRowType();

            // unique key indexes
            for (int pidx : changelogNormalize.uniqueKeys()) {
                usedColumns[pidx] = true;
            }

            final RexProgram program = calc.getProgram();

            // column references in the projection list
            for (RexLocalRef expr : program.getProjectList()) {
                // projections can be simple column identieties but they can also contain
                // expressions, so we need to have a more robust way of extracting all
                // column references from the projection list with the helper below
                for (int ref : extractRefInputFields(Collections.singletonList(program.expandLocalRef(expr)))) {
                    usedColumns[ref] = true;
                }
            }

            // column references in any of the predicates
            RexLocalRef condition = program.getCondition();
            if (condition != null) {
                for (int ref : extractRefInputFields(Collections.singletonList(program.expandLocalRef(condition)))) {
                    usedColumns[ref] = true;
                }
            }
        }

        /**
         * Walk up the physical plan tree to find a calc node right after ChangelogNormalize
         * which projects a subset of the columns used by the source scan. If we find such
         * calc nodes, then we can update our used columns map.
         */
        private boolean[] findUsedColumns(StreamPhysicalTableSourceScan source, final Stack<RelNode> stack) {
            Stack<RelNode> path = (Stack<RelNode>) stack.clone();
            RelDataType sourceRowType = source.getRowType();
            boolean[] usedColumns = new boolean[sourceRowType.getFieldCount()];

            final Object[][] matchConfigs = {
                { // with mini batch enabled plans
                    StreamPhysicalTableSourceScan.class,
                    StreamPhysicalMiniBatchAssigner.class,
                    StreamPhysicalDropUpdateBefore.class,
                    StreamPhysicalExchange.class,
                    StreamPhysicalChangelogNormalize.class,
                    StreamPhysicalCalc.class
                },
                { // without mini batch
                    StreamPhysicalTableSourceScan.class,
                    StreamPhysicalDropUpdateBefore.class,
                    StreamPhysicalExchange.class,
                    StreamPhysicalChangelogNormalize.class,
                    StreamPhysicalCalc.class
                },
            };

            // Find the first matching subtree from above
            ArrayList<RelNode> match = null;
            for (Object[] matchConfig : matchConfigs) {
                match = pathMatches(matchConfig, path);
                if (match != null) {
                    break;
                }
            }

            if (match != null) {
                StreamPhysicalCalc calc = (StreamPhysicalCalc) match.get(match.size() - 1);
                StreamPhysicalChangelogNormalize changelogNormalize = (StreamPhysicalChangelogNormalize) match.get(match.size() - 2);
                computeUsedColumns(calc, changelogNormalize, usedColumns);
                matches.add(match);
            } else {
                // if we don't see a match to the plan, then assume all columns have been used
                // by the subplan, effectively turning off this optization for the source
                for (int i = 0; i < usedColumns.length; i++) {
                    usedColumns[i] = true;
                }
            }

            return usedColumns;
        }

        public RelNode transform(RelNode input, RelBuilder relBuilder) {
            for (ArrayList<RelNode> match : matches) {
                StreamPhysicalTableSourceScan source = (StreamPhysicalTableSourceScan) match.get(0);
                StreamPhysicalCalc calc = (StreamPhysicalCalc) match.get(match.size() - 1);
                boolean[] usedColumns = usedColumnsBySource.get(source);
                int numUsedColumns = 0;
                for (int i = 0; i < usedColumns.length; i++) {
                    if (usedColumns[i]) {
                        numUsedColumns++;
                    }
                }
                if (numUsedColumns != usedColumns.length) {
                    // only apply the transformation when not all columns are used
                    RelNode newNode = transform(relBuilder, calc, usedColumns);
                    input = rewrite(input, calc, newNode);
                }
            }
            return input;
        }

        private RelNode rewrite(RelNode root, RelNode origNode, RelNode newNode) {
            if (origNode == newNode) {
                return root;
            }

            if (root == origNode) {
                return newNode;
            }

            RelVisitor visitor = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                    if (node == origNode) {
                        parent.replaceInput(ordinal, newNode);
                    } else {
                        super.visit(node, ordinal, parent);
                    }
                }
            };

            visitor.go(root);

            return root; 
        }

 
        private RelNode transform(RelBuilder relBuilder, StreamPhysicalCalc calc, boolean[] usedColumns) {
            StreamPhysicalChangelogNormalize changelogNormalize = (StreamPhysicalChangelogNormalize) calc.getInput();
            // Create a union list of fields between the projected columns and unique keys
            final RelDataType inputRowType = changelogNormalize.getRowType();
            final int[] inputRemap = new int[inputRowType.getFieldCount()];

            final RexProgram program = calc.getProgram();

            // we need to know the new column index mappings for the calc node
            // so the array is going to be inputRemap[oldIndex] = new index (or -1 if not needed)
            for (int prev = 0, curr = 0; prev < usedColumns.length; prev++) {
                if (usedColumns[prev]) {
                    inputRemap[prev] = curr;
                    curr++;
                } else {
                    inputRemap[prev] = -1;
                }
            }

            // Construct a new ChangelogNormalize which has the new projection pushed into it
            StreamPhysicalChangelogNormalize newChangelogNormalize =
                    pushNeededColumnsThroughChangelogNormalize(
                        relBuilder, changelogNormalize, usedColumns, inputRemap);

            StreamPhysicalCalc newCalc = projectCopyWithRemap(
                relBuilder, newChangelogNormalize, calc, inputRemap);

            if (newCalc.getProgram().isTrivial()) {
                return newChangelogNormalize;
            } else {
                return newCalc;
            }
        }

        static class TargetRemap extends AbstractTargetMapping {
            private int[] inputRemap;

            static private int targetCount(int[] inputRemap) {
                int targetCount = 0;
                for (int i = 0; i < inputRemap.length; i++) {
                    if (inputRemap[i] >= 0) {
                        targetCount++;
                    }
                }
                return targetCount;
            }

            public TargetRemap(int[] inputRemap) {
                super(inputRemap.length, targetCount(inputRemap));
                this.inputRemap = inputRemap;
            }

            public int getTargetOpt(int source) {
                return inputRemap[source];
            }
        }

        private StreamPhysicalChangelogNormalize pushNeededColumnsThroughChangelogNormalize(
                RelBuilder relBuilder, StreamPhysicalChangelogNormalize changelogNormalize,
                boolean[] usedColumns, int[] inputRemap) {
            final StreamPhysicalExchange exchange = (StreamPhysicalExchange) changelogNormalize.getInput();

            final StreamPhysicalCalc pushedProjectionCalc =
                    projectWithNeededColumns(
                            relBuilder, exchange.getInput(), usedColumns);

            Mappings.TargetMapping remap = new TargetRemap(inputRemap);
            RelDistribution newDistibution = exchange.getDistribution().apply(remap);

            final StreamPhysicalExchange newExchange =
                    (StreamPhysicalExchange)
                            exchange.copy(
                                    exchange.getTraitSet(),
                                    pushedProjectionCalc,
                                    newDistibution);
            newExchange.recomputeDigest();

            int[] keys = changelogNormalize.uniqueKeys();
            int[] newKeys = new int[keys.length];
            for (int i = 0; i < keys.length; i++) {
                newKeys[i] = inputRemap[keys[i]];
            }

            return (StreamPhysicalChangelogNormalize)
                    changelogNormalize.copy(
                            changelogNormalize.getTraitSet(),
                            newExchange,
                            newKeys);
        }

        private StreamPhysicalCalc projectWithNeededColumns(
                RelBuilder relBuilder, RelNode newInput, boolean[] usedColumns) {

            final RexProgramBuilder programBuilder =
                    new RexProgramBuilder(newInput.getRowType(), relBuilder.getRexBuilder());
        
            for (RelDataTypeField field : newInput.getRowType().getFieldList()) {
                if (usedColumns[field.getIndex()]) {
                    programBuilder.addProject(new RexInputRef(field.getIndex(), field.getType()), field.getName());
                }
            }

            final RexProgram newProgram = programBuilder.getProgram();
            return new StreamPhysicalCalc(
                    newInput.getCluster(),
                    newInput.getTraitSet(),
                    newInput,
                    newProgram,
                    newProgram.getOutputRowType());
        }

        private StreamPhysicalCalc projectCopyWithRemap(RelBuilder relBuilder,
                    RelNode newInput, StreamPhysicalCalc origCalc, int[] inputRemap) {

            // We have the original calc which references inputs from the wider row
            // before we projected away the unused columns. In the code below, we need
            // to take all the references in the original Calc and remap that to the
            // new (reduced) input row.

            final RexProgramBuilder programBuilder =
                new RexProgramBuilder(newInput.getRowType(), relBuilder.getRexBuilder());

            RexShuttle remapVisitor = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    return new RexInputRef(inputRemap[inputRef.getIndex()], inputRef.getType());
                }
            };

            List<String> origFieldNames = origCalc.getRowType().getFieldNames();
            int i = 0;
            // rewrite all the simple projections
            for (RexLocalRef ref : origCalc.getProgram().getProjectList()) {
                String origFieldName = origFieldNames.get(i);
                RexNode expandedRef  = origCalc.getProgram().expandLocalRef(ref);
                RexNode newRef = expandedRef.accept(remapVisitor);
                programBuilder.addProject(newRef, origFieldName);
                i++;
            }

            // rewrite any predicates (if exists)
            RexLocalRef conditionLocalRef = origCalc.getProgram().getCondition();
            if (conditionLocalRef != null) {
                RexNode expandedRef  = origCalc.getProgram().expandLocalRef(conditionLocalRef);
                RexNode newRef = expandedRef.accept(remapVisitor);
                programBuilder.addCondition(newRef);
            }

            final RexProgram newProgram = programBuilder.getProgram();
            return new StreamPhysicalCalc(
                    newInput.getCluster(),
                    newInput.getTraitSet(),
                    newInput,
                    newProgram,
                    origCalc.getProgram().getOutputRowType());
        }
    }
}
