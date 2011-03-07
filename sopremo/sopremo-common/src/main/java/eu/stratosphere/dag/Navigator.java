package eu.stratosphere.dag;

/**
 * Navigates through the directed acyclic graph by returning the targets of the directed edges.
 * 
 * @author Arvid Heise
 * @param <Node>
 *        the class of the node
 */
public interface Navigator<Node> {
	/**
	 * Return all nodes which are referenced by the given node.
	 * 
	 * @param node
	 *        the node
	 * @return all referenced nodes
	 */
	public Iterable<Node> getConnectedNodes(Node node);
}