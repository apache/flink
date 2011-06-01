package eu.stratosphere.util.dag;

/**
 * Callback for nodes found with an {@link GraphTraverser}.
 * 
 * @author Arvid Heise
 * @param <Node>
 *        the class of the nodes
 */
public interface GraphTraverseListener<Node> {
	/**
	 * Called for each node found by a {@link GraphTraverser}.
	 * 
	 * @param node
	 *        the current node
	 */
	public void nodeTraversed(Node node);
}
