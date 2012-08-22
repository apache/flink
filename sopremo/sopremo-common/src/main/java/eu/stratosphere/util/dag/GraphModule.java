/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.util.dag;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.util.IdentitySet;

/**
 * Skeleton implementation of {@link SubGraph}.
 * 
 * @param <Node>
 *        the type of all node
 * @param <InputNode>
 *        the type of all input nodes
 * @param <OutputNode>
 *        the type of all output nodes
 */
public abstract class GraphModule<Node, InputNode extends Node, OutputNode extends Node> implements
		SubGraph<Node, InputNode, OutputNode>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8802006043539156002L;

	/**
	 * The outputs of the module.
	 */
	protected final OutputNode[] outputNodes;

	/**
	 * internal outputs
	 */
	protected final List<OutputNode> internalOutputNodes = new ArrayList<OutputNode>();

	/**
	 * The inputs of the module.
	 */
	protected final InputNode[] inputNodes;

	private final ConnectionNavigator<Node> navigator;

	private final String name;

	/**
	 * Initializes a PactModule having the given inputs, outputs, and {@link Navigator}.
	 * 
	 * @param inputNodes
	 *        the inputs
	 * @param outputNodes
	 *        the outputs
	 * @param navigator
	 *        the navigator used to traverse the graph of nodes
	 */
	protected GraphModule(final String name, final InputNode[] inputNodes, final OutputNode[] outputNodes,
			final ConnectionNavigator<Node> navigator) {
		this.name = name;
		this.inputNodes = inputNodes;
		this.outputNodes = outputNodes;
		this.navigator = navigator;
	}

	@Override
	public void addInternalOutput(final OutputNode output) {
		this.internalOutputNodes.add(output);
	}

	/**
	 * Returns the internalOutputNodes.
	 * 
	 * @return the internalOutputNodes
	 */
	@SuppressWarnings("unchecked")
	public OutputNode[] getInternalOutputNodes() {
		return this.internalOutputNodes.toArray((OutputNode[]) Array.newInstance(
			this.outputNodes.getClass().getComponentType(),
			this.internalOutputNodes.size()));
	}

	@SuppressWarnings("unchecked")
	@Override
	public OutputNode[] getAllOutputs() {
		if (this.internalOutputNodes.isEmpty())
			return this.outputNodes;
		final OutputNode[] allOutputs = (OutputNode[]) Array.newInstance(this.outputNodes.getClass()
			.getComponentType(), this.outputNodes.length + this.internalOutputNodes.size());
		System.arraycopy(this.outputNodes, 0, allOutputs, 0, this.outputNodes.length);
		for (int index = 0; index < allOutputs.length; index++)
			allOutputs[index + this.outputNodes.length] = this.internalOutputNodes.get(index);
		return allOutputs;
	}

	@Override
	public InputNode getInput(final int index) {
		return this.inputNodes[index];
	}

	@Override
	public void setInput(final int index, final InputNode input) {
		this.inputNodes[index] = input;
	}

	@Override
	public InputNode[] getInputs() {
		return this.inputNodes;
	}

	public String getName() {
		return this.name;
	}

	@Override
	public OutputNode getOutput(final int index) {
		return this.outputNodes[index];
	}

	@Override
	public OutputNode getInternalOutputNodes(final int index) {
		return this.internalOutputNodes.get(index);
	}

	@Override
	public void setOutput(final int index, final OutputNode output) {
		this.outputNodes[index] = output;
	}

	@Override
	public OutputNode[] getOutputs() {
		return this.outputNodes;
	}

	@Override
	public Iterable<? extends Node> getReachableNodes() {
		return OneTimeTraverser.INSTANCE.getReachableNodes(this.getAllOutputs(), this.navigator);
	}

	@Override
	public String toString() {
		final GraphPrinter<Node> dagPrinter = new GraphPrinter<Node>();
		dagPrinter.setWidth(80);
		return dagPrinter.toString(this.getAllOutputs(), this.navigator);
	}

	@Override
	public void validate() {
		for (final OutputNode output : this.getAllOutputs())
			for (final Node node : this.navigator.getConnectedNodes(output))
				if (node == null)
					throw new IllegalStateException(String.format("%s: output %s is not fully connected",
						this.getName(),
						output));

		final Iterable<? extends Node> reachableNodes = this.getReachableNodes();
		final List<InputNode> inputList = new LinkedList<InputNode>(Arrays.asList(this.inputNodes));
		for (final Node node : reachableNodes)
			inputList.remove(node);

		if (!inputList.isEmpty())
			throw new IllegalStateException(
				String.format("%s: inputs %s are not fully connected", this.getName(), inputList));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		for (final Node node : this.getReachableNodes())
			result = prime * result + node.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;

		@SuppressWarnings({ "rawtypes", "unchecked" })
		final GraphModule<Node, InputNode, OutputNode> other = (GraphModule) obj;
		return this.getUnmatchingNodes(other) == null;
	}

	public List<Node> getUnmatchingNodes(final GraphModule<Node, InputNode, OutputNode> other) {
		final IdentitySet<Node> seen = new IdentitySet<Node>();

		return this.getUnmatchingNode(Arrays.asList(this.getAllOutputs()), Arrays.asList(other.getAllOutputs()), seen);
	}

	/**
	 * @param allOutputs
	 * @param allOutputs2
	 * @param seen
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private List<Node> getUnmatchingNode(final Iterable<? extends Node> nodes1, final Iterable<? extends Node> nodes2,
			final IdentitySet<Node> seen) {
		final Iterator<? extends Node> iterator1 = nodes1.iterator();
		final Iterator<? extends Node> iterator2 = nodes2.iterator();

		while (iterator1.hasNext() && iterator2.hasNext()) {
			final Node node1 = iterator1.next();
			final Node node2 = iterator2.next();

			if (!node1.equals(node2))
				return Arrays.asList(node1, node2);

			final List<Node> unmatching = this.getUnmatchingNode(this.navigator.getConnectedNodes(node1),
				this.navigator.getConnectedNodes(node2), seen);
			if (unmatching != null)
				return unmatching;
		}

		return null;
	}

}
