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

package org.apache.flink.table.planner.plan.processors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.process.DAGProcessContext;
import org.apache.flink.table.planner.plan.nodes.process.DAGProcessor;
import org.apache.flink.table.planner.plan.processors.utils.InputOrderCalculator;
import org.apache.flink.table.planner.plan.processors.utils.InputPriorityConflictResolver;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Union;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * A {@link DAGProcessor} which organize {@link ExecNode}s into multiple input nodes.
 *
 * <p>For a detailed explanation of the algorithm, see appendix of the
 * <a href="https://docs.google.com/document/d/1qKVohV12qn-bM51cBZ8Hcgp31ntwClxjoiNBUOqVHsI">design doc</a>.
 */
public class MultipleInputNodeCreationProcessor implements DAGProcessor {

	private final boolean isStreaming;

	public MultipleInputNodeCreationProcessor(boolean isStreaming) {
		this.isStreaming = isStreaming;
	}

	@Override
	public List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> roots, DAGProcessContext context) {
		if (!isStreaming) {
			// As multiple input nodes use function call to deliver records between sub-operators,
			// we cannot rely on network buffers to buffer records not yet ready to be read,
			// so only BLOCKING dam behavior is safe here.
			// If conflict is detected under this stricter constraint,
			// we add a PIPELINED exchange to mark that its input and output node cannot be merged
			// into the same multiple input node
			InputPriorityConflictResolver resolver = new InputPriorityConflictResolver(
				roots,
				ExecEdge.DamBehavior.BLOCKING,
				ShuffleMode.PIPELINED);
			resolver.detectAndResolve();
		}

		List<ExecNodeWrapper> rootWrappers = wrapExecNodes(roots);
		// sort all nodes in topological order, sinks come first and sources come last
		List<ExecNodeWrapper> orderedWrappers = topologicalSort(rootWrappers);
		// group nodes into multiple input groups
		createMultipleInputGroups(orderedWrappers);
		// apply optimizations to remove unnecessary nodes out of multiple input groups
		optimizeMultipleInputGroups(orderedWrappers);

		// create the real multiple input nodes
		return createMultipleInputNodes(rootWrappers);
	}

	// --------------------------------------------------------------------------------
	// Wrapping and Sorting
	// --------------------------------------------------------------------------------

	private List<ExecNodeWrapper> wrapExecNodes(List<ExecNode<?, ?>> rootNodes) {
		Map<ExecNode<?, ?>, ExecNodeWrapper> wrapperMap = new HashMap<>();
		AbstractExecNodeExactlyOnceVisitor visitor = new AbstractExecNodeExactlyOnceVisitor() {
			@Override
			protected void visitNode(ExecNode<?, ?> node) {
				ExecNodeWrapper wrapper = wrapperMap.computeIfAbsent(node, k -> new ExecNodeWrapper(node));
				for (ExecNode<?, ?> input : node.getInputNodes()) {
					ExecNodeWrapper inputWrapper = wrapperMap.computeIfAbsent(input, k -> new ExecNodeWrapper(input));
					wrapper.inputs.add(inputWrapper);
					inputWrapper.outputs.add(wrapper);
				}
				visitInputs(node);
			}
		};
		rootNodes.forEach(s -> s.accept(visitor));

		List<ExecNodeWrapper> rootWrappers = new ArrayList<>();
		for (ExecNode<?, ?> root : rootNodes) {
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
				int visitCount = visitCountMap.compute(inputWrapper, (k, v) -> v == null ? 1 : v + 1);
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

	private void createMultipleInputGroups(List<ExecNodeWrapper> orderedWrappers) {
		// wrappers are checked in topological order from sinks to sources
		for (ExecNodeWrapper wrapper : orderedWrappers) {
			// we skip nodes which cannot be a member of a multiple input node
			if (!canBeMultipleInputNodeMember(wrapper)) {
				continue;
			}

			// we first try to assign this wrapper into the same group with its outputs
			MultipleInputGroup outputGroup = canBeInSameGroupWithOutputs(wrapper);
			if (outputGroup != null) {
				wrapper.addToGroup(outputGroup);
				continue;
			}

			// we then try to create a new multiple input group with this node as the root
			if (canBeRootOfMultipleInputGroup(wrapper)) {
				wrapper.createGroup();
			}

			// all our attempts failed, this node will not be in a multiple input node
		}
	}

	private boolean canBeMultipleInputNodeMember(ExecNodeWrapper wrapper) {
		if (wrapper.inputs.isEmpty()) {
			// sources cannot be a member of multiple input node
			return false;
		}
		if (wrapper.execNode instanceof Exchange) {
			// exchange cannot be a member of multiple input node
			return false;
		}

		return true;
	}

	/**
	 * A node can only be assigned into the same multiple input group of its outputs
	 * if all outputs have a group and are the same.
	 *
	 * @return the {@link MultipleInputGroup} of the outputs if all outputs have a
	 *         group and are the same, null otherwise
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

	private boolean canBeRootOfMultipleInputGroup(ExecNodeWrapper wrapper) {
		// only a node with more than one input can be the root,
		// as one-input operator chaining are handled by operator chains
		return wrapper.inputs.size() >= 2;
	}

	// --------------------------------------------------------------------------------
	// Multiple Input Groups Optimizing
	// --------------------------------------------------------------------------------

	private void optimizeMultipleInputGroups(List<ExecNodeWrapper> orderedWrappers) {
		// wrappers are checked in topological order from sources to sinks
		for (int i = orderedWrappers.size() - 1; i >= 0; i--) {
			ExecNodeWrapper wrapper = orderedWrappers.get(i);
			MultipleInputGroup group = wrapper.group;
			if (group == null) {
				// we only consider nodes currently in a multiple input group
				continue;
			}

			boolean isUnion = wrapper.execNode instanceof Union;

			if (group.members.size() == 1) {
				Preconditions.checkState(
					wrapper == group.root,
					"The only member of a multiple input group is not its root. This is a bug.");
				// optimization 1. we clean up multiple input groups with only 1 member,
				// unless one of its input is a FLIP-27 source (for maximizing source chaining),
				// however unions do not apply to this optimization because they're not real operators
				if (isUnion || wrapper.inputs.stream().noneMatch(
						inputWrapper -> isChainableSource(inputWrapper.execNode))) {
					wrapper.removeFromGroup();
				}
				continue;
			}

			if (!isEntranceOfMultipleInputGroup(wrapper)) {
				// we're not removing a node from the middle of a multiple input group
				continue;
			}

			boolean shouldRemove = false;
			if (isUnion) {
				// optimization 2. we do not allow union to be the tail of a multiple input
				// as we're paying extra function calls for this, unless one of the united
				// input is a FLIP-27 source
				shouldRemove = wrapper.inputs.stream().noneMatch(
					inputWrapper -> isChainableSource(inputWrapper.execNode));
			} else if (wrapper.inputs.size() == 1) {
				// optimization 3. for one-input operators we'll remove it unless its input
				// is an exchange or a FLIP-27 source, this is mainly to avoid the following
				// pattern:
				// non-chainable source -> calc --\
				//                                 join ->
				// non-chainable source -> calc --/
				// if we move two calcs into the multiple input group rooted at the join, we're
				// directly shuffling large amount of records from the source without filtering
				// by the calc
				ExecNode<?, ?> input = wrapper.inputs.get(0).execNode;
				shouldRemove = !(input instanceof Exchange) && !isChainableSource(input);
			}

			// optimization 4. for singleton operations (for example singleton global agg)
			// we're not including it into the multiple input node as we have to ensure that
			// the whole multiple input can only have 1 parallelism.
			// continuous singleton operations connected by forwarding shuffle will be dealt
			// together with optimization 3
			shouldRemove |= wrapper.inputs.stream().anyMatch(inputWrapper ->
				inputWrapper.execNode instanceof Exchange &&
					((Exchange) inputWrapper.execNode).distribution.getType() == RelDistribution.Type.SINGLETON);

			if (shouldRemove) {
				wrapper.removeFromGroup();
			}
		}
	}

	private boolean isEntranceOfMultipleInputGroup(ExecNodeWrapper wrapper) {
		Preconditions.checkNotNull(
			wrapper.group,
			"Exec node wrapper does not have a multiple input group. This is a bug.");
		for (ExecNodeWrapper inputWrapper : wrapper.inputs) {
			if (inputWrapper.group == wrapper.group) {
				// one of the input is in the same group, so this node is not the entrance of the group
				return false;
			}
		}
		return true;
	}

	private boolean isChainableSource(ExecNode<?, ?> node) {
		if (node instanceof BatchExecBoundedStreamScan) {
			BatchExecBoundedStreamScan scan = (BatchExecBoundedStreamScan) node;
			return scan.boundedStreamTable().dataStream().getTransformation() instanceof SourceTransformation;
		} else if (node instanceof StreamExecDataStreamScan) {
			StreamExecDataStreamScan scan = (StreamExecDataStreamScan) node;
			return scan.dataStreamTable().dataStream().getTransformation() instanceof SourceTransformation;
		} else if (node instanceof CommonPhysicalTableSourceScan) {
			CommonPhysicalTableSourceScan scan = (CommonPhysicalTableSourceScan) node;
			return scan.tableSource().getScanRuntimeProvider(
				ScanRuntimeProviderContext.INSTANCE) instanceof SourceProvider;
		}
		return false;
	}

	// --------------------------------------------------------------------------------
	// Multiple Input Nodes Creating
	// --------------------------------------------------------------------------------

	private List<ExecNode<?, ?>> createMultipleInputNodes(List<ExecNodeWrapper> rootWrappers) {
		List<ExecNode<?, ?>> result = new ArrayList<>();
		Map<ExecNodeWrapper, ExecNode<?, ?>> visitedMap = new HashMap<>();
		for (ExecNodeWrapper rootWrapper : rootWrappers) {
			result.add(getMultipleInputNode(rootWrapper, visitedMap));
		}
		return result;
	}

	private ExecNode<?, ?> getMultipleInputNode(
			ExecNodeWrapper wrapper,
			Map<ExecNodeWrapper, ExecNode<?, ?>> visitedMap) {
		if (visitedMap.containsKey(wrapper)) {
			return visitedMap.get(wrapper);
		}

		for (int i = 0; i < wrapper.inputs.size(); i++) {
			wrapper.execNode.replaceInputNode(i, (ExecNode) getMultipleInputNode(wrapper.inputs.get(i), visitedMap));
		}

		ExecNode<?, ?> ret;
		if (wrapper.group != null && wrapper == wrapper.group.root) {
			ret = createMultipleInputNode(wrapper.group, visitedMap);
		} else {
			ret = wrapper.execNode;
		}
		visitedMap.put(wrapper, ret);
		return ret;
	}

	private ExecNode<?, ?> createMultipleInputNode(
			MultipleInputGroup group,
			Map<ExecNodeWrapper, ExecNode<?, ?>> visitedMap) {
		// calculate the inputs of the multiple input node
		List<Tuple2<ExecNode<?, ?>, ExecEdge>> inputs = new ArrayList<>();
		for (ExecNodeWrapper member : group.members) {
			for (int i = 0; i < member.inputs.size(); i++) {
				ExecNodeWrapper memberInput = member.inputs.get(i);
				if (group.members.contains(memberInput)) {
					continue;
				}
				Preconditions.checkState(
					visitedMap.containsKey(memberInput),
					"Input of a multiple input member is not visited. This is a bug.");

				ExecNode<?, ?> inputNode = visitedMap.get(memberInput);
				ExecEdge inputEdge = member.execNode.getInputEdges().get(i);
				inputs.add(Tuple2.of(inputNode, inputEdge));
			}
		}

		if (isStreaming) {
			return createStreamMultipleInputNode(group, inputs);
		} else {
			return createBatchMultipleInputNode(group, inputs);
		}
	}

	private StreamExecMultipleInput createStreamMultipleInputNode(
			MultipleInputGroup group,
			List<Tuple2<ExecNode<?, ?>, ExecEdge>> inputs) {
		RelNode outputRel = (RelNode) group.root.execNode;
		RelNode[] inputRels = new RelNode[inputs.size()];
		for (int i = 0; i < inputs.size(); i++) {
			inputRels[i] = (RelNode) inputs.get(i).f0;
		}

		return new StreamExecMultipleInput(
			outputRel.getCluster(),
			outputRel.getTraitSet(),
			inputRels,
			outputRel);
	}

	private BatchExecMultipleInput createBatchMultipleInputNode(
			MultipleInputGroup group,
			List<Tuple2<ExecNode<?, ?>, ExecEdge>> inputs) {
		// first calculate the input orders using InputPriorityConflictResolver
		Set<ExecNode<?, ?>> inputSet = new HashSet<>();
		for (Tuple2<ExecNode<?, ?>, ExecEdge> t : inputs) {
			inputSet.add(t.f0);
		}
		InputOrderCalculator calculator = new InputOrderCalculator(
			group.root.execNode,
			inputSet,
			ExecEdge.DamBehavior.BLOCKING);
		Map<ExecNode<?, ?>, Integer> inputOrderMap = calculator.calculate();

		// then create input rels and edges with the input orders
		RelNode outputRel = (RelNode) group.root.execNode;
		RelNode[] inputRels = new RelNode[inputs.size()];
		ExecEdge[] inputEdges = new ExecEdge[inputs.size()];
		for (int i = 0; i < inputs.size(); i++) {
			ExecNode<?, ?> inputNode = inputs.get(i).f0;
			ExecEdge originalInputEdge = inputs.get(i).f1;
			inputRels[i] = (RelNode) inputNode;
			inputEdges[i] = ExecEdge.builder()
				.requiredShuffle(originalInputEdge.getRequiredShuffle())
				.damBehavior(originalInputEdge.getDamBehavior())
				.priority(inputOrderMap.get(inputNode))
				.build();
		}

		return new BatchExecMultipleInput(
			outputRel.getCluster(),
			outputRel.getTraitSet(),
			inputRels,
			outputRel,
			inputEdges);
	}

	// --------------------------------------------------------------------------------
	// Helper Classes
	// --------------------------------------------------------------------------------

	private static class ExecNodeWrapper {
		private final ExecNode<?, ?> execNode;
		private final List<ExecNodeWrapper> inputs;
		private final List<ExecNodeWrapper> outputs;
		private MultipleInputGroup group;

		private ExecNodeWrapper(ExecNode<?, ?> execNode) {
			this.execNode = execNode;
			this.inputs = new ArrayList<>();
			this.outputs = new ArrayList<>();
			this.group = null;
		}

		private void createGroup() {
			this.group = new MultipleInputGroup(this);
		}

		private void addToGroup(MultipleInputGroup group) {
			Preconditions.checkState(
				this.group == null,
				"This exec node wrapper is already in a multiple input group. This is a bug.");
			group.members.add(this);
			this.group = group;
		}

		private void removeFromGroup() {
			Preconditions.checkNotNull(
				group,
				"Trying to remove an exec node from its multiple input group while it has none. This is a bug.");
			group.members.remove(this);
			group = null;
		}
	}

	private static class MultipleInputGroup {
		private final ExecNodeWrapper root;
		// We use list instead of set here to ensure that the inputs of a multiple input node
		// will not change order. Although order changes do not affect the correctness of the
		// query, it does affect plan test cases.
		private final List<ExecNodeWrapper> members;

		private MultipleInputGroup(ExecNodeWrapper root) {
			this.root = root;
			this.members = new ArrayList<>();
			members.add(root);
		}
	}
}
