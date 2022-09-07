/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.DefaultRelShuttle;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This rule is meant to extract the common projects for the table scan to help reuse the source as
 * much as possible. This should be executed after {@link
 * org.apache.calcite.rel.rules.ProjectMergeRule} and before the {@link
 * PushProjectIntoTableSourceScanRule}.
 */
@Value.Enclosing
public class FlinkExtractCommonProjectOnSourceRule
        extends RelRule<FlinkExtractCommonProjectOnSourceRule.Config> {
    private static final Logger LOG =
            LoggerFactory.getLogger(FlinkExtractCommonProjectOnSourceRule.class);

    public static final FlinkExtractCommonProjectOnSourceRule INSTANCE =
            new FlinkExtractCommonProjectOnSourceRule(Config.DEFAULT);

    private static final RelHint MARKER_HINT =
            RelHint.builder(FlinkHints.HINT_NAME_COMMON_PROJECT_PRESERVED).build();

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    public FlinkExtractCommonProjectOnSourceRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {

        final RelNode root = call.rel(0);
        CommonProjectFinder cpFinder = new CommonProjectFinder();
        cpFinder.go(root);
        if (cpFinder.exists) {
            LOG.info("Common project has been injected.");
            return;
        }
        ReusableTableScanFinder visitor = new ReusableTableScanFinder();
        visitor.go(root);
        Iterator<Map.Entry<String, Node>> it = visitor.sourceNodes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Node> entry = it.next();
            int count = entry.getValue().associatedNodes.values().stream().reduce(0, Integer::sum);
            if (count >= 2) {
                LOG.info(
                        "Find the reusable table scan: {} for {} nodes",
                        entry.getValue().sourceDigest,
                        count);
            } else {
                it.remove();
            }
        }

        TableConfig tableConfig = ShortcutUtils.unwrapContext(root).getTableConfig();
        if (!tableConfig.get((OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED))
                || !tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED)
                || !tableConfig.get(
                        OptimizerConfigOptions
                                .TABLE_OPTIMIZER_PUSH_COMMON_PROJECT_ON_SOURCE_ENABLED)) {
            return;
        }

        if (visitor.sourceNodes.isEmpty()) {
            return;
        }

        RelNode newNode = new ReplaceProject(visitor.sourceNodes).visit(root);
        call.transformTo(newNode);
    }

    /** RelShuttle to pull up the common project on source. */
    static class ReplaceProject extends DefaultRelShuttle {

        private final Map<String, Node> sourceNodes;

        public ReplaceProject(Map<String, Node> sourceNodes) {
            this.sourceNodes = sourceNodes;
        }

        @Override
        public RelNode visit(RelNode rel) {
            return visitInputs(rel);
        }

        private RelNode visitInputs(RelNode rel) {
            rel = stripHep(rel);
            boolean change = false;
            List<RelNode> newInputs = new ArrayList<>();
            for (RelNode input : rel.getInputs()) {
                input = stripHep(input);
                RelNode newInput = input.accept(this);
                change = change || (newInput != input);
                newInputs.add(newInput);
            }
            if (change) {
                return rel.copy(rel.getTraitSet(), newInputs);
            } else {
                return rel;
            }
        }

        @Override
        public RelNode visit(LogicalProject currentProject) {
            String sourceDigest = null;
            LogicalTableScan scan = null;
            for (Map.Entry<String, Node> entry : sourceNodes.entrySet()) {
                for (Tuple2<LogicalTableScan, LogicalProject> associatedNode :
                        entry.getValue().associatedNodes.keySet()) {
                    if (associatedNode.f1.equals(currentProject)) {
                        sourceDigest = entry.getKey();
                        scan = associatedNode.f0;
                        break;
                    }
                }
            }
            if (!StringUtils.isNullOrWhitespaceOnly(sourceDigest)) {
                // check the source table support push down
                final TableSourceTable sourceTable = scan.getTable().unwrap(TableSourceTable.class);
                if (sourceTable == null
                        || !(sourceTable.tableSource() instanceof SupportsProjectionPushDown)) {
                    return visitInputs(currentProject);
                }

                List<RexNode> commonProjects = new ArrayList<>();
                Set<Integer> duplicatedRefs = new HashSet<>();
                // old input index to new input index
                Map<Integer, Integer> indexMapping = new HashMap<>();
                // build the common projects ref from all the parent
                for (Tuple2<LogicalTableScan, LogicalProject> associatedNode :
                        sourceNodes.get(sourceDigest).associatedNodes.keySet()) {
                    LogicalProject project = associatedNode.f1;
                    final scala.Tuple2<Object, RelDataType>[] refFields =
                            RexNodeExtractor.extractRefInputFieldsWithType(project.getProjects());

                    for (scala.Tuple2<Object, RelDataType> tuple2 : refFields) {
                        if (duplicatedRefs.add((Integer) tuple2._1())) {
                            commonProjects.add(new RexInputRef((Integer) tuple2._1(), tuple2._2()));
                        }
                    }
                }
                // sort to get a stable order.
                commonProjects.sort(
                        Comparator.comparingInt(value -> ((RexInputRef) value).getIndex()));
                int idx = 0;
                for (RexNode project : commonProjects) {
                    indexMapping.put(((RexInputRef) project).getIndex(), idx++);
                }
                List<RelHint> newHints = new ArrayList<>(currentProject.getHints());
                newHints.add(MARKER_HINT);

                if (currentProject.getProjects().equals(commonProjects)) {
                    // the current project is same with the common, do not apply change to this one.
                    return currentProject.withHints(newHints);
                }

                // create the new common project
                LogicalProject newBottomProj =
                        LogicalProject.create(
                                scan,
                                newHints,
                                commonProjects,
                                RexUtil.createStructType(
                                        scan.getCluster().getTypeFactory(), commonProjects));
                newBottomProj = (LogicalProject) newBottomProj.withHints(newHints);
                List<RexNode> topExprs = new ArrayList<>(currentProject.getProjects().size());

                // replace the ref in the current project
                for (RexNode project : currentProject.getProjects()) {
                    topExprs.add(
                            project.accept(
                                    new RexShuttle() {
                                        @Override
                                        public RexNode visitInputRef(RexInputRef inputRef) {
                                            return new RexInputRef(
                                                    indexMapping.get(inputRef.getIndex()),
                                                    inputRef.getType());
                                        }
                                    }));
                }

                return LogicalProject.create(
                        newBottomProj,
                        currentProject.getHints(),
                        topExprs,
                        RexUtil.createStructType(scan.getCluster().getTypeFactory(), topExprs));
            }
            return visitInputs(currentProject);
        }
    }

    /** Nodes of the reusable source. */
    static class Node {
        private final String sourceDigest;
        // child, parent.
        private final Map<Tuple2<LogicalTableScan, LogicalProject>, Integer> associatedNodes =
                new HashMap<>();

        public Node(String sourceDigest, LogicalTableScan child, LogicalProject parent) {
            this.sourceDigest = sourceDigest;
            associatedNodes.put(Tuple2.of(child, parent), 1);
        }

        public void associate(LogicalTableScan child, LogicalProject parent) {
            Tuple2<LogicalTableScan, LogicalProject> newAssociated = Tuple2.of(child, parent);
            int count = associatedNodes.getOrDefault(newAssociated, 0);
            associatedNodes.put(newAssociated, count + 1);
        }
    }

    /**
     * Visitor to detect the same table scan. It can have multiple related parents to share the
     * source.
     */
    static class ReusableTableScanFinder extends RelVisitor {

        private final Map<String, Node> sourceNodes = new HashMap<>();

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            node = stripHep(node);

            if (node instanceof LogicalTableScan && parent instanceof LogicalProject) {
                String digest = node.getDigest();
                Node cached = sourceNodes.get(digest);
                if (cached != null) {
                    cached.associate((LogicalTableScan) node, (LogicalProject) parent);
                } else {
                    sourceNodes.put(
                            digest,
                            new Node(digest, (LogicalTableScan) node, (LogicalProject) parent));
                }
            } else {
                super.visit(node, ordinal, parent);
            }
        }
    }

    static class CommonProjectFinder extends RelVisitor {

        boolean exists = false;

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            node = stripHep(node);

            if (node instanceof LogicalProject) {
                LogicalProject project = (LogicalProject) node;
                boolean matched =
                        project.getHints().stream()
                                .anyMatch(
                                        h ->
                                                h.hintName.equals(
                                                        FlinkHints
                                                                .HINT_NAME_COMMON_PROJECT_PRESERVED));
                if (matched) {
                    exists = true;
                } else {
                    super.visit(node, ordinal, parent);
                }
            } else {
                super.visit(node, ordinal, parent);
            }
        }
    }

    private static RelNode stripHep(RelNode rel) {
        if (rel instanceof HepRelVertex) {
            HepRelVertex hepRelVertex = (HepRelVertex) rel;
            rel = hepRelVertex.getCurrentRel();
        }
        return rel;
    }

    /** Configuration for {@link FlinkExtractCommonProjectOnSourceRule}. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT =
                ImmutableFlinkExtractCommonProjectOnSourceRule.Config.builder()
                        .build()
                        .onRoot()
                        .as(Config.class);

        @Override
        default RelOptRule toRule() {
            return new FlinkExtractCommonProjectOnSourceRule(this);
        }

        default Config onRoot() {
            final RelRule.OperandTransform transform =
                    operandBuilder -> operandBuilder.operand(RelNode.class).anyInputs();
            return withOperandSupplier(transform).as(Config.class);
        }
    }
}
