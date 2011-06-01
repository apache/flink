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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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
public abstract class AbstractSubGraph<Node, InputNode extends Node, OutputNode extends Node> implements
		SubGraph<Node, InputNode, OutputNode> {
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

	private Navigator<Node> navigator;

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
	protected AbstractSubGraph(InputNode[] inputNodes, OutputNode[] outputNodes, Navigator<Node> navigator) {
		this.inputNodes = inputNodes;
		this.outputNodes = outputNodes;
		this.navigator = navigator;
	}

	@Override
	public void addInternalOutput(OutputNode output) {
		this.internalOutputNodes.add(output);
	}

	@SuppressWarnings("unchecked")
	@Override
	public OutputNode[] getAllOutputs() {
		if (this.internalOutputNodes.isEmpty())
			return this.outputNodes;
		OutputNode[] allOutputs = (OutputNode[]) Array.newInstance(this.outputNodes.getClass()
			.getComponentType(), this.outputNodes.length + this.internalOutputNodes.size());
		System.arraycopy(this.outputNodes, 0, allOutputs, 0, this.outputNodes.length);
		for (int index = 0; index < allOutputs.length; index++)
			allOutputs[index + this.outputNodes.length] = this.internalOutputNodes.get(index);
		return allOutputs;
	}

	@Override
	public Iterable<? extends Node> getReachableNodes() {
		return OneTimeTraverser.INSTANCE.getReachableNodes(this.getAllOutputs(), this.navigator);
	}

	@Override
	public InputNode getInput(int index) {
		return this.inputNodes[index];
	}

	@Override
	public InputNode[] getInputs() {
		return this.inputNodes;
	}

	@Override
	public OutputNode getOutput(int index) {
		return this.outputNodes[index];
	}

	@Override
	public OutputNode[] getOutputs() {
		return this.outputNodes;
	}

	@Override
	public String toString() {
		GraphPrinter<Node> dagPrinter = new GraphPrinter<Node>();
		dagPrinter.setWidth(80);
		return dagPrinter.toString(this.getAllOutputs(), this.navigator);
	}

	@Override
	public void validate() {
		for (OutputNode output : this.getAllOutputs())
			for (Node node : this.navigator.getConnectedNodes(output))
				if (node == null)
					throw new IllegalStateException(String.format("Output %s is not fully connected", output));

		final Iterable<? extends Node> reachableNodes = this.getReachableNodes();
		List<InputNode> inputList = new LinkedList<InputNode>(Arrays.asList(this.inputNodes));
		for (Node node : reachableNodes)
			inputList.remove(node);

		if (!inputList.isEmpty())
			throw new IllegalStateException(String.format("Inputs %s are not fully connected", inputList));

	}

}
