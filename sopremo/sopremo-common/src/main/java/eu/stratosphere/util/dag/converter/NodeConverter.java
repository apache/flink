package eu.stratosphere.util.dag.converter;

import java.util.List;

/**
 * Converts a node from one type to a node from another type using a list of recursively converted child nodes.<br>
 * The NodeConverter may be annotated with {@link AppendChildren} to aggregate graphs to flat structures or {@link Leaf}
 * for faster processing.
 * 
 * @author Arvid Heise
 * @param <InputType>
 *        the type of the input node
 * @param <OutputType>
 *        the type of the output node
 */
public interface NodeConverter<InputType, OutputType> {
	/**
	 * Converts the given input node to an output nodes using the provided child nodes.
	 * 
	 * @param inputNode
	 *        the input node
	 * @param childNodes
	 *        a list of child nodes that have been recursively converted
	 * @return the converted node or null if the inputNode should not be directly translated
	 */
	public OutputType convertNode(InputType inputNode, List<OutputType> childNodes);
}