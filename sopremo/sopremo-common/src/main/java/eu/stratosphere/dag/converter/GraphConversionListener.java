package eu.stratosphere.dag.converter;

import java.util.List;

/**
 * Provides a callback mechanism before and after a subgraph or a specific node is converted.<br>
 * The callback methods are invoked in the following order:<br>
 * <ul>
 * <li>{@link #beforeSubgraphConversion(Object)}
 * <li><i>conversion of all children</i>
 * <li>{@link #beforeNodeConversion(Object, List)}
 * <li><i>conversion of node</i>
 * <li>{@link #afterNodeConversion(Object, List, Object)}
 * <li>{@link #afterSubgraphConversion(Object, Object)}
 * </ul>
 * Since the conversion of child nodes is recursive, multiple {@link #beforeSubgraphConversion(Object)} with different
 * nodes as parameter will occur before a {@link #beforeNodeConversion(Object, List)}.
 * 
 * @author Arvid Heise
 * @param <InputType>
 *        the type of the input node
 * @param <OutputType>
 *        the type of the output node
 */
public interface GraphConversionListener<InputType, OutputType> {
	/**
	 * The callback is invoked after the children have been converted but before the actual node is converted.
	 * 
	 * @param in
	 *        the input node
	 * @param children
	 *        the child nodes
	 */
	public void beforeNodeConversion(InputType in, List<OutputType> children);

	/**
	 * This method is called after all nodes and child nodes have been converted.
	 * 
	 * @param in
	 *        the input node
	 * @param children
	 *        the child nodes
	 * @param out
	 *        the converted node
	 */
	public void afterNodeConversion(InputType in, List<OutputType> children, OutputType out);

	/**
	 * The callback is invoked before any child node or the actual node have been converted.<br>
	 * This method is not called for {@link GraphConverter#convertNode(Object, List)}.
	 * 
	 * @param in
	 *        the input node
	 */
	public void beforeSubgraphConversion(InputType in);

	/**
	 * This method is called after all nodes and child nodes have been converted. It is called
	 * {@link #afterNodeConversion(Object, List, Object)} but is not invoked for
	 * {@link GraphConverter#convertNode(Object, List)}.
	 * 
	 * @param in
	 *        the input node
	 * @param out
	 *        the converted node
	 */
	public void afterSubgraphConversion(InputType in, OutputType out);
}
