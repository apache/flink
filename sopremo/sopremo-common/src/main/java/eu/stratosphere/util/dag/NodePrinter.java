package eu.stratosphere.util.dag;

/**
 * May provide an alternative implementation of the {@link Object#toString()}.
 * 
 * @author Arvid Heise
 * @param <Node>
 *        the class of the node
 */
public interface NodePrinter<Node> {
	/**
	 * Returns a string representation of the given node.
	 * 
	 * @param node
	 *        the node
	 * @return a string representation.
	 * @see Object#toString()
	 */
	public String toString(Node node);
}