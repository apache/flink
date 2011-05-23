package eu.stratosphere.dag;

/**
 * Callback for nodes found with an {@link DAGTraverser}.
 * 
 * @author Arvid Heise
 * @param <Node>
 *        the class of the nodes
 */
public interface DAGTraverseListener<Node> {
	/**
	 * Called for each node found by a {@link DAGTraverser}.
	 * 
	 * @param node
	 *        the current node
	 */
	public void nodeTraversed(Node node);
}
