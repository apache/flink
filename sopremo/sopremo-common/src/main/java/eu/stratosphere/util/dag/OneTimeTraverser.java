package eu.stratosphere.util.dag;

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Basic traverse strategy that visits every reachable node exactly once.
 * 
 * @author Arvid Heise
 */
public class OneTimeTraverser extends AbstractGraphTraverser {
	/**
	 * The default, stateless instance of the {@link OneTimeTraverser}.
	 */
	public static final OneTimeTraverser INSTANCE = new OneTimeTraverser();

	@Override
	public <Node> Iterable<Node> getReachableNodes(final Iterator<? extends Node> startNodes,
			final Navigator<Node> navigator) {
		final Map<Node, Object> visitedNodes = new IdentityHashMap<Node, Object>();

		this.visitNodes(startNodes, navigator, new GraphTraverseListener<Node>() {
			@Override
			public void nodeTraversed(final Node node) {
			}
		}, visitedNodes);

		return visitedNodes.keySet();
	}

	@Override
	public <Node> void traverse(final Iterator<? extends Node> startNodes, final Navigator<Node> navigator,
			final GraphTraverseListener<Node> listener) {
		final Map<Node, Object> visitedNodes = new IdentityHashMap<Node, Object>();

		this.visitNodes(startNodes, navigator, listener, visitedNodes);
	}

	private <Node> void visitNodes(final Iterator<? extends Node> startNodes, final Navigator<Node> navigator,
			final GraphTraverseListener<Node> listener, final Map<Node, Object> visitedNodes) {
		while (startNodes.hasNext()) {
			final Node node = startNodes.next();
			if (node != null && !visitedNodes.containsKey(node)) {
				visitedNodes.put(node, null);
				listener.nodeTraversed(node);
				this.visitNodes(navigator.getConnectedNodes(node).iterator(), navigator, listener, visitedNodes);
			}
		}
	}
}
