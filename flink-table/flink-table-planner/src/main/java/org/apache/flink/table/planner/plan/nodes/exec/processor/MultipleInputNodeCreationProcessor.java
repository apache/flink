/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.processor.utils.InputOrderCalculator;
import org.apache.flink.table.planner.plan.nodes.exec.processor.utils.InputPriorityConflictResolver;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED;

/**
 * A {@link ExecNodeGraphProcessor} which organize {@link ExecNode}s into multiple input nodes.
 *
 * <p>For a detailed explanation of the algorithm, see appendix of the <a
 * href="https://docs.google.com/document/d/1qKVohV12qn-bM51cBZ8Hcgp31ntwClxjoiNBUOqVHsI">design
 * doc</a>.
 */
public class MultipleInputNodeCreationProcessor implements ExecNodeGraphProcessor {

    private final boolean isStreaming;

    public MultipleInputNodeCreationProcessor(boolean isStreaming) {
        this.isStreaming = isStreaming;
    }

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        if (!isStreaming) {
            // As multiple input nodes use function call to deliver records between sub-operators,
            // we cannot rely on network buffers to buffer records not yet ready to be read,
            // so only BLOCKING dam behavior is safe here.
            // If conflict is detected under this stricter constraint,
            // we add a PIPELINED exchange to mark that its input and output node cannot be merged
            // into the same multiple input node
            InputPriorityConflictResolver resolver =
                    new InputPriorityConflictResolver(
                            execGraph.getRootNodes(),
                            InputProperty.DamBehavior.BLOCKING,
                            StreamExchangeMode.PIPELINED,
                            context.getPlanner().getTableConfig());
            resolver.detectAndResolve();
        }

        List<ExecNodeWrapper> rootWrappers = wrapExecNodes(execGraph.getRootNodes());
        // sort all nodes in topological order, sinks come first and sources come last
        List<ExecNodeWrapper> orderedWrappers = topologicalSort(rootWrappers);
        // group nodes into multiple input groups
        createMultipleInputGroups(context.getPlanner().getTableConfig(), orderedWrappers);
        // apply optimizations to remove unnecessary nodes out of multiple input groups
        optimizeMultipleInputGroups(orderedWrappers, context);

        // create the real multiple input nodes
        List<ExecNode<?>> newRootNodes =
                createMultipleInputNodes(context.getPlanner().getTableConfig(), rootWrappers);
        return new ExecNodeGraph(newRootNodes);
    }

    // --------------------------------------------------------------------------------
    // Wrapping and Sorting
    // --------------------------------------------------------------------------------

    private List<ExecNodeWrapper> wrapExecNodes(List<ExecNode<?>> rootNodes) {
        Map<ExecNode<?>, ExecNodeWrapper> wrapperMap = new HashMap<>();
        AbstractExecNodeExactlyOnceVisitor visitor =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        ExecNodeWrapper wrapper =
                                wrapperMap.computeIfAbsent(node, k -> new ExecNodeWrapper(node));
                        for (ExecEdge inputEdge : node.getInputEdges()) {
                            ExecNode<?> inputNode = inputEdge.getSource();
                            ExecNodeWrapper inputWrapper =
                                    wrapperMap.computeIfAbsent(
                                            inputNode, k -> new ExecNodeWrapper(inputNode));
                            wrapper.inputs.add(inputWrapper);
                            inputWrapper.outputs.add(wrapper);
                        }
                        visitInputs(node);
                    }
                };
        rootNodes.forEach(s -> s.accept(visitor));

        List<ExecNodeWrapper> rootWrappers = new ArrayList<>();
        for (ExecNode<?> root : rootNodes) {
            ExecNodeWrapper rootWrapper = wrapperMap.get(root);
            Preconditions.checkNotNull(rootWrapper, "Root node is not wrapped. This is a bug.");
            rootWrappers.add(rootWrapper);
        }
        return rootWrappers;
    }

    private List<ExecNodeWrapper> topologicalSort(List<ExecNodeWrapper> rootWrappers) {
        List<ExecNodeWrapper> result = new ArrayList<>();
        Queue<ExecNodeWrapper> queue = new LinkedList<>(rootWrappers);
        Map<ExecNodeWrapper, Integer> visitCountMap = new HashMap<>();

        while (!queue.isEmpty()) {
            ExecNodeWrapper wrapper = queue.poll();
            result.add(wrapper);
            for (ExecNodeWrapper inputWrapper : wrapper.inputs) {
                int visitCount =
                        visitCountMap.compute(inputWrapper, (k, v) -> v == null ? 1 : v + 1);
                if (visitCount == inputWrapper.outputs.size()) {
                    queue.offer(inputWrapper);
                }
            }
        }

        return result;
    }

    // --------------------------------------------------------------------------------
    // Multiple Input Groups Creating
    // --------------------------------------------------------------------------------

    private void createMultipleInputGroups(
            ReadableConfig tableConfig, List<ExecNodeWrapper> orderedWrappers) {
        // wrappers are checked in topological order from sinks to sources
        for (ExecNodeWrapper wrapper : orderedWrappers) {
            // we skip nodes which cannot be a member of a multiple input node
            if (!canBeMultipleInputNodeMember(wrapper)) {
                continue;
            }

            // we first try to assign this wrapper into the same group with its outputs
            MultipleInputGroup outputGroup = canBeInSameGroupWithOutputs(wrapper);
            if (outputGroup != null) {
                outputGroup.addMember(wrapper);
                continue;
            }

            // we then try to create a new multiple input group with this node as the root
            if (canBeRootOfMultipleInputGroup(tableConfig, wrapper)) {
                wrapper.group = new MultipleInputGroup(wrapper);
            }

            // all our attempts failed, this node will not be in a multiple input node
        }
    }

    private boolean canBeMultipleInputNodeMember(ExecNodeWrapper wrapper) {
        if (wrapper.inputs.isEmpty()) {
            // sources cannot be a member of multiple input node
            return false;
        }
        if (wrapper.execNode instanceof CommonExecExchange) {
            // exchange cannot be a member of multiple input node
            return false;
        }

        return true;
    }

    /**
     * A node can only be assigned into the same multiple input group of its outputs if all outputs
     * have a group and are the same.
     *
     * @return the {@link MultipleInputGroup} of the outputs if all outputs have a group and are the
     *     same, null otherwise
     */
    private MultipleInputGroup canBeInSameGroupWithOutputs(ExecNodeWrapper wrapper) {
        if (wrapper.outputs.isEmpty()) {
            return null;
        }

        MultipleInputGroup outputGroup = wrapper.outputs.get(0).group;
        if (outputGroup == null) {
            return null;
        }

        for (ExecNodeWrapper outputWrapper : wrapper.outputs) {
            if (outputWrapper.group != outputGroup) {
                return null;
            }
        }

        return outputGroup;
    }

    private boolean canBeRootOfMultipleInputGroup(
            ReadableConfig tableConfig, ExecNodeWrapper wrapper) {
        // A node with more than one input can be the root, and one-input operator can also be root
        // if the upstream node only contain Calc&HashJoin&HashAgg and operator fusion codegen
        // enabled
        return wrapper.inputs.size() >= 2
                || (tableConfig.get(TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED)
                        && oneInputNodeCanBeRoot(wrapper));
    }

    private boolean oneInputNodeCanBeRoot(ExecNodeWrapper wrapper) {
        // The Calc and HashAgg can be the root node of MultipleInputGroup if the upstream node only
        // contain Calc&HashJoin&HashAgg. This limitation can be removed if all kinds of operator
        // support OFCG.
        if (wrapper.inputs.size() == 1) {
            ExecNodeWrapper inputWrapper = wrapper.inputs.get(0);
            ExecNode<?> execNode = inputWrapper.execNode;
            // must have HashJoin which support OFCG as the upstream node
            if (execNode instanceof BatchExecHashJoin && execNode.supportFusionCodegen()) {
                return true;
            } else if (execNode instanceof BatchExecCalc
                    || execNode instanceof BatchExecHashAggregate) {
                return oneInputNodeCanBeRoot(inputWrapper);
            }
        }

        return false;
    }

    // --------------------------------------------------------------------------------
    // Multiple Input Groups Optimizing
    // --------------------------------------------------------------------------------

    private void optimizeMultipleInputGroups(
            List<ExecNodeWrapper> orderedWrappers, ProcessorContext context) {
        // wrappers are checked in topological order from sources to sinks
        for (int i = orderedWrappers.size() - 1; i >= 0; i--) {
            ExecNodeWrapper wrapper = orderedWrappers.get(i);
            MultipleInputGroup group = wrapper.group;
            if (group == null) {
                // we only consider nodes currently in a multiple input group
                continue;
            }
            if (!isEntranceOfMultipleInputGroup(wrapper)) {
                // we're not removing a node from the middle of a multiple input group
                continue;
            }

            boolean shouldRemove = false;
            if (wrapper.execNode instanceof CommonExecUnion) {
                // optimization 1. we do not allow union to be the tail of a multiple input
                // as we're paying extra function calls for this, unless one of the united
                // input is a FLIP-27 source
                shouldRemove =
                        wrapper.inputs.stream()
                                .noneMatch(
                                        inputWrapper ->
                                                isChainableSource(inputWrapper.execNode, context));
            } else if (wrapper.inputs.size() == 1) {
                // optimization 2. for one-input operators we'll remove it unless its input
                // is an exchange or a FLIP-27 source, this is mainly to avoid the following
                // pattern:
                // non-chainable source -> calc --\
                //                                 join ->
                // non-chainable source -> calc --/
                // if we move two calcs into the multiple input group rooted at the join, we're
                // directly shuffling large amount of records from the source without filtering
                // by the calc
                ExecNode<?> input = wrapper.inputs.get(0).execNode;
                shouldRemove =
                        !(input instanceof CommonExecExchange)
                                && !isChainableSource(input, context);
            }

            // optimization 3. for singleton operations (for example singleton global agg)
            // we're not including it into the multiple input node as we have to ensure that
            // the whole multiple input can only have 1 parallelism.
            // continuous singleton operations connected by forwarding shuffle will be dealt
            // together with optimization 3
            shouldRemove |=
                    wrapper.inputs.stream()
                            .anyMatch(
                                    inputWrapper ->
                                            inputWrapper.execNode instanceof CommonExecExchange
                                                    && (inputWrapper.execNode)
                                                                    .getInputProperties()
                                                                    .get(0)
                                                                    .getRequiredDistribution()
                                                                    .getType()
                                                            == InputProperty.DistributionType
                                                                    .SINGLETON);

            if (shouldRemove) {
                wrapper.group.removeMember(wrapper);
            }
        }

        // wrappers are checked in topological order from sinks to sources
        for (ExecNodeWrapper wrapper : orderedWrappers) {
            MultipleInputGroup group = wrapper.group;
            if (group == null) {
                // we only consider nodes currently in a multiple input group
                continue;
            }
            if (wrapper != wrapper.group.root) {
                // we only consider nodes at the root of the multiple input
                continue;
            }

            boolean isUnion = wrapper.execNode instanceof CommonExecUnion;

            if (group.members.size() == 1) {
                // optimization 4. we clean up multiple input groups with only 1 member,
                // unless one of its input is a FLIP-27 source (for maximizing source chaining),
                // however unions do not apply to this optimization because they're not real
                // operators
                if (isUnion
                        || wrapper.inputs.stream()
                                .noneMatch(
                                        inputWrapper ->
                                                isChainableSource(
                                                        inputWrapper.execNode, context))) {
                    wrapper.group.removeRoot();
                }
                continue;
            }

            if (isUnion) {
                // optimization 5. this optimization remove redundant union at the output of a
                // multiple input, consider the following graph:
                //
                // source -> exchange -> agg ---\
                // source -> exchange -> agg --> union ->
                // source -> exchange -> join --/
                // source -> exchange --/
                //
                // we'll initially put aggs, the join and the union into a multiple input, while
                // the union here is actually redundant.
                int numberOfUsefulInputs = 0;
                List<Integer> uselessBranches = new ArrayList<>();
                List<List<ExecNodeWrapper>> sameGroupWrappersList = new ArrayList<>();

                // an input branch is useful if it contains a node with two or more inputs other
                // than union. we shall keep the union if it has two or more useful input branches,
                // as this may benefit source chaining. consider the following example:
                //
                // chainable source -> join -\
                //                     /      \
                // chainable source --<        union
                //                     \      /
                // chainable source -> join -/
                for (int i = 0; i < wrapper.inputs.size(); i++) {
                    ExecNodeWrapper inputWrapper = wrapper.inputs.get(i);
                    List<ExecNodeWrapper> sameGroupWrappers =
                            getInputWrappersInSameGroup(inputWrapper, wrapper.group);
                    sameGroupWrappersList.add(sameGroupWrappers);
                    long numberOfValuableNodes =
                            sameGroupWrappers.stream()
                                    .filter(
                                            w ->
                                                    w.inputs.size() >= 2
                                                            && !(w.execNode
                                                                    instanceof CommonExecUnion))
                                    .count();
                    if (numberOfValuableNodes > 0) {
                        numberOfUsefulInputs++;
                    } else {
                        uselessBranches.add(i);
                    }
                }

                if (numberOfUsefulInputs < 2) {
                    // remove this union and its useless branches from multiple input
                    for (int branch : uselessBranches) {
                        List<ExecNodeWrapper> sameGroupWrappers = sameGroupWrappersList.get(branch);
                        for (ExecNodeWrapper w : sameGroupWrappers) {
                            if (w.group != null) {
                                w.group.removeMember(w);
                            }
                        }
                    }
                    wrapper.group.removeRoot();
                }
            } else if (wrapper.inputs.size() == 1) {
                // optimization 6. operators with only 1 input are not allowed to be the root,
                // as their chaining will be handled by operator chains. But Calc and HashAgg can be
                // the root node for OFCG
                // TODO If all kinds of one input operator support OFCG, we can remove this
                // limitation
                boolean fusionCodegenEnabled =
                        context.getPlanner()
                                .getTableConfig()
                                .get(TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED);
                if (fusionCodegenEnabled
                        && (wrapper.execNode instanceof BatchExecCalc
                                || wrapper.execNode instanceof BatchExecHashAggregate)) {
                    continue;
                }

                wrapper.group.removeRoot();
            }
        }
    }

    private List<ExecNodeWrapper> getInputWrappersInSameGroup(
            ExecNodeWrapper wrapper, MultipleInputGroup group) {
        List<ExecNodeWrapper> ret = new ArrayList<>();
        Queue<ExecNodeWrapper> queue = new LinkedList<>();
        Set<ExecNodeWrapper> visited = new HashSet<>();
        queue.add(wrapper);
        visited.add(wrapper);

        while (!queue.isEmpty()) {
            ExecNodeWrapper w = queue.poll();
            if (w.group != group) {
                // if a wrapper is not in the required group than its inputs will
                // also not be in the group, so we can just skip it
                continue;
            }
            ret.add(w);

            for (ExecNodeWrapper inputWrapper : w.inputs) {
                if (visited.contains(inputWrapper)) {
                    continue;
                }
                queue.add(inputWrapper);
                visited.add(inputWrapper);
            }
        }

        return ret;
    }

    private boolean isEntranceOfMultipleInputGroup(ExecNodeWrapper wrapper) {
        Preconditions.checkNotNull(
                wrapper.group,
                "Exec node wrapper does not have a multiple input group. This is a bug.");
        for (ExecNodeWrapper inputWrapper : wrapper.inputs) {
            if (inputWrapper.group == wrapper.group) {
                // one of the input is in the same group, so this node is not the entrance of the
                // group
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    static boolean isChainableSource(ExecNode<?> node, ProcessorContext context) {
        if (node instanceof BatchExecBoundedStreamScan) {
            BatchExecBoundedStreamScan scan = (BatchExecBoundedStreamScan) node;
            return scan.getDataStream().getTransformation() instanceof SourceTransformation;
        } else if (node instanceof StreamExecDataStreamScan) {
            StreamExecDataStreamScan scan = (StreamExecDataStreamScan) node;
            return scan.getDataStream().getTransformation() instanceof SourceTransformation;
        } else if (node instanceof CommonExecTableSourceScan) {
            // translateToPlan will cache the transformation,
            // this is OK because sources do not have any input so the transformation will never
            // change.
            Transformation<?> transformation =
                    node.translateToPlan(Preconditions.checkNotNull(context).getPlanner());
            return transformation instanceof SourceTransformation;
        }
        return false;
    }

    // --------------------------------------------------------------------------------
    // Multiple Input Nodes Creating
    // --------------------------------------------------------------------------------

    private List<ExecNode<?>> createMultipleInputNodes(
            ReadableConfig tableConfig, List<ExecNodeWrapper> rootWrappers) {
        List<ExecNode<?>> result = new ArrayList<>();
        Map<ExecNodeWrapper, ExecNode<?>> visitedMap = new HashMap<>();
        for (ExecNodeWrapper rootWrapper : rootWrappers) {
            result.add(getMultipleInputNode(tableConfig, rootWrapper, visitedMap));
        }
        return result;
    }

    private ExecNode<?> getMultipleInputNode(
            ReadableConfig tableConfig,
            ExecNodeWrapper wrapper,
            Map<ExecNodeWrapper, ExecNode<?>> visitedMap) {
        if (visitedMap.containsKey(wrapper)) {
            return visitedMap.get(wrapper);
        }

        for (int i = 0; i < wrapper.inputs.size(); i++) {
            ExecNode<?> multipleInputNode =
                    getMultipleInputNode(tableConfig, wrapper.inputs.get(i), visitedMap);
            ExecEdge execEdge =
                    ExecEdge.builder().source(multipleInputNode).target(wrapper.execNode).build();
            wrapper.execNode.replaceInputEdge(i, execEdge);
        }

        ExecNode<?> ret;
        if (wrapper.group != null && wrapper == wrapper.group.root) {
            ret = createMultipleInputNode(tableConfig, wrapper.group, visitedMap);
        } else {
            ret = wrapper.execNode;
        }
        visitedMap.put(wrapper, ret);
        return ret;
    }

    private ExecNode<?> createMultipleInputNode(
            ReadableConfig tableConfig,
            MultipleInputGroup group,
            Map<ExecNodeWrapper, ExecNode<?>> visitedMap) {
        // calculate the inputs of the multiple input node
        List<Tuple3<ExecNode<?>, InputProperty, ExecEdge>> inputs = new ArrayList<>();
        for (ExecNodeWrapper member : group.members) {
            for (int i = 0; i < member.inputs.size(); i++) {
                ExecNodeWrapper memberInput = member.inputs.get(i);
                if (group.members.contains(memberInput)) {
                    continue;
                }
                Preconditions.checkState(
                        visitedMap.containsKey(memberInput),
                        "Input of a multiple input member is not visited. This is a bug.");

                ExecNode<?> inputNode = visitedMap.get(memberInput);
                InputProperty inputProperty = member.execNode.getInputProperties().get(i);
                ExecEdge edge = member.execNode.getInputEdges().get(i);
                inputs.add(Tuple3.of(inputNode, inputProperty, edge));
            }
        }

        if (isStreaming) {
            return createStreamMultipleInputNode(tableConfig, group, inputs);
        } else {
            return createBatchMultipleInputNode(tableConfig, group, inputs);
        }
    }

    private StreamExecMultipleInput createStreamMultipleInputNode(
            ReadableConfig tableConfig,
            MultipleInputGroup group,
            List<Tuple3<ExecNode<?>, InputProperty, ExecEdge>> inputs) {
        ExecNode<?> rootNode = group.root.execNode;
        List<ExecNode<?>> inputNodes = new ArrayList<>();
        for (Tuple3<ExecNode<?>, InputProperty, ExecEdge> tuple3 : inputs) {
            inputNodes.add(tuple3.f0);
        }

        String description =
                ExecNodeUtil.getMultipleInputDescription(rootNode, inputNodes, new ArrayList<>());
        StreamExecMultipleInput multipleInput =
                new StreamExecMultipleInput(
                        tableConfig,
                        inputNodes.stream()
                                .map(i -> InputProperty.DEFAULT)
                                .collect(Collectors.toList()),
                        rootNode,
                        description);

        List<ExecEdge> inputEdges = new ArrayList<>(inputNodes.size());
        for (ExecNode<?> inputNode : inputNodes) {
            inputEdges.add(ExecEdge.builder().source(inputNode).target(multipleInput).build());
        }
        multipleInput.setInputEdges(inputEdges);
        return multipleInput;
    }

    private BatchExecMultipleInput createBatchMultipleInputNode(
            ReadableConfig tableConfig,
            MultipleInputGroup group,
            List<Tuple3<ExecNode<?>, InputProperty, ExecEdge>> inputs) {
        // first calculate the input orders using InputPriorityConflictResolver
        Set<ExecNode<?>> inputSet = new HashSet<>();
        for (Tuple3<ExecNode<?>, InputProperty, ExecEdge> tuple3 : inputs) {
            inputSet.add(tuple3.f0);
        }
        InputOrderCalculator calculator =
                new InputOrderCalculator(
                        group.root.execNode, inputSet, InputProperty.DamBehavior.BLOCKING);
        Map<ExecNode<?>, Integer> inputOrderMap = calculator.calculate();

        // then create input rels and edges with the input orders
        ExecNode<?> rootNode = group.root.execNode;
        List<ExecNode<?>> inputNodes = new ArrayList<>();
        List<InputProperty> inputProperties = new ArrayList<>();
        List<ExecEdge> originalEdges = new ArrayList<>();
        for (Tuple3<ExecNode<?>, InputProperty, ExecEdge> tuple3 : inputs) {
            ExecNode<?> inputNode = tuple3.f0;
            InputProperty originalInputEdge = tuple3.f1;
            ExecEdge edge = tuple3.f2;
            inputNodes.add(inputNode);
            inputProperties.add(
                    InputProperty.builder()
                            .requiredDistribution(originalInputEdge.getRequiredDistribution())
                            .damBehavior(originalInputEdge.getDamBehavior())
                            .priority(inputOrderMap.get(inputNode))
                            .build());
            originalEdges.add(edge);
        }

        List<ExecNode<?>> memberExecNodes =
                group.members.stream()
                        .map(ExecNodeWrapper::getExecNode)
                        .collect(Collectors.toList());

        String description =
                ExecNodeUtil.getMultipleInputDescription(rootNode, inputNodes, inputProperties);
        BatchExecMultipleInput multipleInput =
                new BatchExecMultipleInput(
                        tableConfig,
                        inputProperties,
                        rootNode,
                        memberExecNodes,
                        originalEdges,
                        description);

        List<ExecEdge> inputEdges = new ArrayList<>(inputNodes.size());
        for (ExecNode<?> inputNode : inputNodes) {
            inputEdges.add(ExecEdge.builder().source(inputNode).target(multipleInput).build());
        }
        multipleInput.setInputEdges(inputEdges);
        return multipleInput;
    }

    // --------------------------------------------------------------------------------
    // Helper Classes
    // --------------------------------------------------------------------------------

    private static class ExecNodeWrapper {
        private final ExecNode<?> execNode;
        private final List<ExecNodeWrapper> inputs;
        private final List<ExecNodeWrapper> outputs;
        private MultipleInputGroup group;

        private ExecNodeWrapper(ExecNode<?> execNode) {
            this.execNode = execNode;
            this.inputs = new ArrayList<>();
            this.outputs = new ArrayList<>();
            this.group = null;
        }

        public ExecNode<?> getExecNode() {
            return execNode;
        }
    }

    private static class MultipleInputGroup {
        // We use list instead of set here to ensure that the inputs of a multiple input node
        // will not change order. Although order changes do not affect the correctness of the
        // query, it does affect plan test cases.
        private final List<ExecNodeWrapper> members;

        private ExecNodeWrapper root;

        private MultipleInputGroup(ExecNodeWrapper root) {
            this.members = new ArrayList<>();
            members.add(root);
            this.root = root;
        }

        private void addMember(ExecNodeWrapper wrapper) {
            Preconditions.checkState(
                    wrapper.group == null,
                    "The given exec node wrapper is already in a multiple input group. This is a bug.");
            members.add(wrapper);
            wrapper.group = this;
        }

        private void removeMember(ExecNodeWrapper wrapper) {
            if (wrapper == root) {
                removeRoot();
            } else {
                Preconditions.checkState(
                        members.remove(wrapper),
                        "The given exec node wrapper does not exist in the multiple input group. This is a bug.");
                wrapper.group = null;
            }
        }

        private void removeRoot() {
            Preconditions.checkNotNull(
                    root, "Multiple input group does not have a root. This is a bug.");
            Set<ExecNodeWrapper> sameGroupInputWrappers = new HashSet<>();
            for (ExecNodeWrapper inputWrapper : root.inputs) {
                if (members.contains(inputWrapper)) {
                    sameGroupInputWrappers.add(inputWrapper);
                }
            }
            Preconditions.checkState(
                    sameGroupInputWrappers.size() < 2,
                    "There are two or more inputs of the root remaining in the multiple input group. This is a bug.");

            members.remove(root);
            root.group = null;
            if (sameGroupInputWrappers.isEmpty()) {
                root = null;
            } else {
                root = sameGroupInputWrappers.iterator().next();
            }
        }
    }
}
