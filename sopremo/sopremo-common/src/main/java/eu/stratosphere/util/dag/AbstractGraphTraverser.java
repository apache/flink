package eu.stratosphere.util.dag;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementing classes traverse a directed acyclic graph (DAG) with a specific strategy. Unless explicitly stated, no
 * implementing class should visit a node more than once.
 * 
 * @author Arvid Heise
 */
public abstract class AbstractGraphTraverser implements GraphTraverser {

	/**
	 * Returns all reachable nodes from the start nodes including the start nodes themselves.
	 * 
	 * @param startNodes
	 *        the initial nodes of the graph
	 * @param navigator
	 *        successively returns all connected nodes from the initial nodes
	 * @param <Node>
	 *        the class of the nodes
	 * @return all reachable nodes
	 */
	public <Node> Iterable<Node> getReachableNodes(final Iterable<? extends Node> startNodes,
			final Navigator<Node> navigator) {
		return this.getReachableNodes(startNodes.iterator(), navigator);
	}

	/**
	 * Returns all reachable nodes from the start nodes including the start nodes themselves.
	 * 
	 * @param startNodes
	 *        the initial nodes of the graph
	 * @param navigator
	 *        successively returns all connected nodes from the initial nodes
	 * @param <Node>
	 *        the class of the nodes
	 * @return all reachable nodes
	 */
	public <Node> Iterable<Node> getReachableNodes(final Iterator<? extends Node> startNodes,
			final Navigator<Node> navigator) {
		final Map<Node, Object> visitedNodes = new IdentityHashMap<Node, Object>();

		this.traverse(startNodes, navigator, new GraphTraverseListener<Node>() {
			@Override
			public void nodeTraversed(final Node node) {
				visitedNodes.put(node, null);
			}
		});

		return visitedNodes.keySet();
	}

	/**
	 * Returns all reachable nodes from the start nodes including the start nodes themselves.
	 * 
	 * @param startNodes
	 *        the initial nodes of the graph
	 * @param navigator
	 *        successively returns all connected nodes from the initial nodes
	 * @param <Node>
	 *        the class of the nodes
	 * @return all reachable nodes
	 */
	public <Node> Iterable<Node> getReachableNodes(final Node[] startNodes, final Navigator<Node> navigator) {
		return this.getReachableNodes(Arrays.asList(startNodes).iterator(), navigator);
	}

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
	public <Node> void traverse(final Iterable<? extends Node> startNodes, final Navigator<Node> navigator,
			final GraphTraverseListener<Node> listener) {
		this.traverse(startNodes.iterator(), navigator, listener);
	}

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
	public <Node> void traverse(final Node[] startNodes, final Navigator<Node> navigator,
			final GraphTraverseListener<Node> listener) {
		this.traverse(Arrays.asList(startNodes).iterator(), navigator, listener);
	}
}