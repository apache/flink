package eu.stratosphere.util.dag;

import java.util.List;

/**
 * Base class for all kinds of subgraphs. A subgraph has an arbitrary but
 * well-defined number of inputs and outputs. It is designed to facilitate
 * modularization and thus to increase the maintainability of large graphs.
 * 
 * @author Arvid Heise
 * @param <Node>
 *            the type of all node
 * @param <InputNode>
 *            the type of all input nodes
 * @param <OutputNode>
 *            the type of all output nodes
 */
public interface SubGraph<Node, InputNode extends Node, OutputNode extends Node> {

	/**
	 * Adds an additional output node to the internal list of outputs. This new
	 * output is not part of the interface of the SubGraph. It is only used to
	 * traverse the graph and access all nodes.<br>
	 * The function is needed for SubGraphs that do not expose all graph paths
	 * publicly.
	 * 
	 * @param output
	 *            the output to add internally
	 */
	public abstract void addInternalOutput(OutputNode output);

	/**
	 * Returns all (external) and internal output nodes.
	 * 
	 * @return all output nodes
	 */
	public abstract List<OutputNode> getAllOutputs();

	/**
	 * Returns the input at the specified position.
	 * 
	 * @param index
	 *            the index of the input
	 * @return the input at the specified position
	 */
	public abstract InputNode getInput(int index);

	/**
	 * Sets the input at the specified position.
	 * 
	 * @param index
	 *            the index of the input
	 * @param input
	 *            the input at the specified position
	 */
	public void setInput(final int index, final InputNode input);

	/**
	 * Returns all inputs of this PactModule.
	 * 
	 * @return all inputs
	 */
	public abstract List<InputNode> getInputs();

	/**
	 * Returns the output at the specified position.
	 * 
	 * @param index
	 *            the index of the output
	 * @return the output at the specified position
	 */
	public abstract OutputNode getOutput(int index);

	/**
	 * Returns the internal output at the specified position.
	 * 
	 * @param index
	 *            the index of the output
	 * @return the internal output at the specified position
	 */
	public OutputNode getInternalOutputNodes(int index);

	/**
	 * Sets the output at the specified position.
	 * 
	 * @param index
	 *            the index of the output
	 * @param input
	 *            the output at the specified position
	 */
	public void setOutput(final int index, final OutputNode output);

	/**
	 * Returns all outputs of this Subgraph.
	 * 
	 * @return all outputs
	 */
	public abstract List<OutputNode> getOutputs();

	/**
	 * Returns all nodes that are either (internal) output nodes or included in
	 * the reference graph.
	 * 
	 * @return all nodes in this module
	 */
	public abstract Iterable<? extends Node> getReachableNodes();

	/**
	 * Returns the number of inputs.
	 * 
	 * @return the number of inputs
	 */
	public int getNumInputs();

	/**
	 * Returns the number of outputs.
	 * 
	 * @return the number of outputs
	 */
	public int getNumOutputs();

	/**
	 * Checks whether all declared inputs and outputs are fully connected.
	 * 
	 * @throws IllegalStateException
	 *             if the module is invalid
	 */
	public abstract void validate();

}