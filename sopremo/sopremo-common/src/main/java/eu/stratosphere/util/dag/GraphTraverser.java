package eu.stratosphere.util.dag;

import java.util.Iterator;

/**
 * Implementing classes traverse a directed acyclic graph (DAG) with a specific strategy. Unless explicitly stated, no
 * implementing class should visit a node more than once.
 * 
 * @author Arvid Heise
 */
public interface GraphTraverser {
	/**
	 * Traverses the DAG consisting of the given start nodes and all notes reachable with the navigator and calls the
	 * specified {@link GraphTraverseListener} for each found node.
	 * 
	 * @param startNodes
	 *        the initial nodes of the graph
	 * @param navigator
	 *        successively returns all connected nodes from the initial nodes
	 * @param listener
	 *        the callback called for all nodes in the DAG
	 * @param <Node>
	 *        the class of the nodes
	 */
	public abstract <Node> void traverse(Iterator<? extends Node> startNodes, Navigator<Node> navigator,
			GraphTraverseListener<Node> listener);

}