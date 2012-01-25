package eu.stratosphere.util.dag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.util.IdentityList;

/**
 * Finds successive partitions of independent graph nodes and returns them in ascending order.<br>
 * The partitions are organized level-wise in a way that all nodes of level X are only referenced from nodes of level
 * X-1 or less.<br>
 * Applying the partitioner to a DAG will result in all start nodes forming the first partition or level. All nodes that
 * are referenced from the start nodes are partitioned next if there is no other input to these nodes.<br>
 * <br>
 * Example: given a graph with start nodes A, B and references A-&gt;C, B-&gt;D, and C-&gt;D will result in the three
 * levels [A, B], [C], [D]. A and B are both independent from other nodes from the beginning. C only depends from A and
 * thus from nodes of the first level. However, D depends additionally on C and thus can not be included in the second
 * level.
 * 
 * @author Arvid Heise
 */
public class GraphLevelPartitioner {

	private static <Node> void gatherNodes(final List<Node> nodes, final ConnectionNavigator<Node> navigator, final Node node) {
		if (!nodes.contains(node))
			if (node != null) {
				nodes.add(node);
				for (final Node child : navigator.getConnectedNodes(node))
					gatherNodes(nodes, navigator, child);
			}
	}

	/**
	 * Partitions the DAG given by the start nodes and all nodes reachable by the navigator.
	 * 
	 * @param startNodes
	 *        the start nodes
	 * @param navigator
	 *        the navigator
	 * @param <Node>
	 *        the type of the node
	 * @return the levels of the DAG
	 * @throws IllegalStateException
	 *         if the graph contains cycles
	 */
	public static <Node> List<Level<Node>> getLevels(final Iterable<? extends Node> startNodes,
			final ConnectionNavigator<Node> navigator) {
		return getLevels(startNodes.iterator(), navigator);
	}

	/**
	 * Partitions the DAG given by the start nodes and all nodes reachable by the navigator.
	 * 
	 * @param startNodes
	 *        the start nodes
	 * @param navigator
	 *        the navigator
	 * @param <Node>
	 *        the type of the node
	 * @return the levels of the DAG
	 * @throws IllegalStateException
	 *         if the graph contains cycles
	 */
	public static <Node> List<Level<Node>> getLevels(final Iterator<? extends Node> startNodes,
			final ConnectionNavigator<Node> navigator) {

		final List<Node> remainingNodes = new IdentityList<Node>();
		while (startNodes.hasNext())
			gatherNodes(remainingNodes, navigator, startNodes.next());

		final List<Node> usedNodes = new IdentityList<Node>();
		final List<Level<Node>> levels = new ArrayList<Level<Node>>();

		while (!remainingNodes.isEmpty()) {
			final List<Node> independentNodes = new ArrayList<Node>();

			for (final Node node : remainingNodes)
				if (isIndependent(node, usedNodes, navigator))
					independentNodes.add(node);

			if (independentNodes.isEmpty())
				throw new IllegalStateException("graph does not have nodes without input");
			levels.add(new Level<Node>(independentNodes, navigator));

			remainingNodes.removeAll(independentNodes);
			usedNodes.addAll(independentNodes);
		}

		return levels;
	}

	/**
	 * Partitions the DAG given by the start nodes and all nodes reachable by the navigator.
	 * 
	 * @param startNodes
	 *        the start nodes
	 * @param navigator
	 *        the navigator
	 * @param <Node>
	 *        the type of the node
	 * @return the levels of the DAG
	 * @throws IllegalStateException
	 *         if the graph contains cycles
	 */
	public static <Node> List<Level<Node>> getLevels(final Node[] startNodes, final ConnectionNavigator<Node> navigator) {
		return getLevels(Arrays.asList(startNodes).iterator(), navigator);
	}

	private static <Node> boolean isIndependent(final Node node, final Collection<Node> usedNodes,
			final ConnectionNavigator<Node> navigator) {
		for (final Object input : navigator.getConnectedNodes(node))
			if (!usedNodes.contains(input))
				return false;
		return true;
	}

	/**
	 * A level contains only nodes that depend (are referenced from) on nodes of the previous levels.
	 * 
	 * @author Arvid Heise
	 * @see GraphLevelPartitioner
	 * @param <Node>
	 *        the type of the node
	 */
	public static class Level<Node> {
		private final IdentityHashMap<Object, List<Object>> outgoings = new IdentityHashMap<Object, List<Object>>();

		private final List<Node> levelNodes;

		private Level(final List<Node> nodes, final ConnectionNavigator<Node> navigator) {
			this.levelNodes = nodes;

			// initializes all outgoing links
			for (final Node node : nodes) {
				final ArrayList<Object> links = new ArrayList<Object>();
				for (final Object connectedNode : navigator.getConnectedNodes(node))
					links.add(connectedNode);
				this.outgoings.put(node, links);
			}
		}

		/**
		 * Adds a node to this level at a specific index.
		 * 
		 * @param index
		 *        the index of the node after successful insertion
		 * @param node
		 *        the node to add
		 * @throws IndexOutOfBoundsException
		 *         if the index is out of bounds
		 */
		public void add(final int index, final Node node) {
			this.levelNodes.add(index, node);
			this.outgoings.put(node, new ArrayList<Object>());
		}

		/**
		 * Adds a node to this level
		 * 
		 * @param node
		 *        the node to add
		 */
		public void add(final Node node) {
			this.levelNodes.add(node);
			this.outgoings.put(node, new ArrayList<Object>());
		}

		/**
		 * Returns all nodes of this level.
		 * 
		 * @return all nodes in this level
		 */
		public List<Node> getLevelNodes() {
			return this.levelNodes;
		}

		/**
		 * Returns all outgoing links from a node of this level.
		 * 
		 * @param node
		 *        the node
		 * @return all outgoing links
		 */
		public List<Object> getLinks(final Node node) {
			return this.outgoings.get(node);
		}

		@Override
		public String toString() {
			return this.levelNodes.toString();
		}

		/**
		 * Updates a link from the given node to a given load. The update is only reflected in this virtual level and
		 * the actual graph remains untouched.
		 * 
		 * @param fromNode
		 *        the node from which the link originates
		 * @param oldTarget
		 *        the old node to which the link goes or null if the newTarget should only be added
		 * @param newTarget
		 *        the new target of the node
		 */
		public void updateLink(final Object fromNode, final Object oldTarget, final Object newTarget) {
			if (oldTarget == null)
				this.outgoings.get(fromNode).add(newTarget);
			else
				this.outgoings.get(fromNode).set(this.outgoings.get(fromNode).indexOf(oldTarget), newTarget);
		}
	}
}
