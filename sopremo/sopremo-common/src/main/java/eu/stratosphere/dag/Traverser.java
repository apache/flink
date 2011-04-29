package eu.stratosphere.dag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;

public class Traverser<Node> {
	private Collection<Node> nodes;

	private Navigator<Node> navigator;

	/**
	 * Initializes Traverser with the given {@link Navigator} and the start nodes.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 */
	public Traverser(Navigator<Node> navigator, Collection<Node> startNodes) {
		this.navigator = navigator;
		this.nodes = new ArrayList<Node>();
		for (Node node : startNodes)
			gatherNodes(node);
	}

	/**
	 * Initializes Traverser with the given {@link Navigator} and the start nodes.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 */
	public Traverser(Navigator<Node> navigator, Iterable<Node> startNodes) {
		this(navigator, startNodes.iterator());
	}

	/**
	 * Initializes Traverser with the given {@link Navigator} and the start nodes.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 */
	public Traverser(Navigator<Node> navigator, Iterator<Node> startNodes) {
		this.navigator = navigator;
		this.nodes = new ArrayList<Node>();
		while (startNodes.hasNext())
			gatherNodes(startNodes.next());
	}

	/**
	 * Initializes Traverser with the given {@link Navigator} and the start nodes.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param startNodes
	 *        the start nodes
	 */
	public Traverser(Navigator<Node> navigator, Node... startNodes) {
		this.navigator = navigator;
		this.nodes = new ArrayList<Node>();
		for (Node node : startNodes)
			gatherNodes(node);
	}

	/**
	 * Initializes Traverser with the given {@link Navigator} and all nodes reachable from the root node.
	 * 
	 * @param navigator
	 *        the navigator
	 * @param rootNode
	 *        the root node
	 */
	public Traverser(Navigator<Node> navigator, Node rootNode) {
		this.navigator = navigator;
		this.nodes = new ArrayList<Node>();
		gatherNodes(rootNode);
	}

	private void gatherNodes(Node node) {
		if (!nodes.contains(node))
			if (node != null) {
				this.nodes.add(node);
				for (Node child : navigator.getConnectedNodes(node))
					gatherNodes(child);
			}
	}

	private class Level {
		private IdentityHashMap<Object, List<Object>> outgoings = new IdentityHashMap<Object, List<Object>>();

		private List<Node> levelNodes = new ArrayList<Node>();

		public Level(List<Node> nodes) {
			this.levelNodes.addAll(nodes);

			// initializes all outgoing links
			for (Node node : nodes) {
				ArrayList<Object> links = new ArrayList<Object>();
				for (Object connectedNode : Traverser.this.navigator.getConnectedNodes(node))
					links.add(connectedNode);
				this.outgoings.put(node, links);
			}
		}

		public List<Object> getLinks(Object node) {
			return this.outgoings.get(node);
		}

		public List<Node> getLevelNodes() {
			return levelNodes;
		}

		@Override
		public String toString() {
			return this.levelNodes.toString();
		}

		public int indexOf(Object input) {
			int index = 0;
			for (Object node : getLevelNodes()) {
				if (node == input)
					return index;
				index++;
			}
			return -1;
		}

		public void updateLink(Object from, Object to, Object placeholder) {
			this.outgoings.get(from).set(this.outgoings.get(from).indexOf(to), placeholder);
		}
	}

	private List<Level> getLevels() {
		Collection<Node> remainingNodes = new ArrayList<Node>(this.nodes);
		List<Node> usedNodes = new ArrayList<Node>();
		List<Level> levels = new ArrayList<Level>();

		while (!remainingNodes.isEmpty()) {
			List<Node> independentNodes = new ArrayList<Node>();

			for (Node node : remainingNodes)
				if (this.isIndependent(node, usedNodes))
					independentNodes.add(node);

			if (independentNodes.isEmpty())
				throw new IllegalStateException("graph does not have nodes without input");
			levels.add(new Level(new ArrayList<Node>(independentNodes)));

			remainingNodes.removeAll(independentNodes);
			usedNodes.addAll(independentNodes);
		}

		return levels;
	}

	private boolean isIndependent(Node node, Collection<Node> usedNodes) {
		for (Object input : this.navigator.getConnectedNodes(node))
			if (input != null && !usedNodes.contains(input))
				return false;
		return true;
	}

	public void traverse(TraverseListener<Node> listener) {
		for (Level level : this.getLevels()) {
			for (Node node : level.getLevelNodes())
				listener.nodeTraversed(node);
		}
	}
}
