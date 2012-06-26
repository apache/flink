package eu.stratosphere.util.dag;

import java.util.List;

/**
 * Navigates through the directed acyclic graph by returning the targets of the directed edges.
 * 
 * @author Arvid Heise
 * @param <Node>
 *        the class of the node
 */
public interface ConnectionNavigator<Node> {
	/**
	 * Return all nodes which are referenced by the given node.
	 * 
	 * @param node
	 *        the node
	 * @return all referenced nodes
	 */
	public List<? extends Node> getConnectedNodes(Node node);
}