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
	public <Node> void traverse(Iterator<? extends Node> startNodes, Navigator<Node> navigator,
			GraphTraverseListener<Node> listener) {
		final Map<Node, Object> visitedNodes = new IdentityHashMap<Node, Object>();

		visitNodes(startNodes, navigator, listener, visitedNodes);
	}

	private <Node> void visitNodes(Iterator<? extends Node> startNodes, Navigator<Node> navigator,
			GraphTraverseListener<Node> listener, final Map<Node, Object> visitedNodes) {
		while (startNodes.hasNext()) {
			Node node = startNodes.next();
			if (!visitedNodes.containsKey(node)) {
				visitedNodes.put(node, null);
				listener.nodeTraversed(node);
				visitNodes(navigator.getConnectedNodes(node).iterator(), navigator, listener, visitedNodes);
			}
		}
	}

	@Override
	public <Node> Iterable<Node> getReachableNodes(Iterator<? extends Node> startNodes, Navigator<Node> navigator) {
		final Map<Node, Object> visitedNodes = new IdentityHashMap<Node, Object>();

		visitNodes(startNodes, navigator, new GraphTraverseListener<Node>() {
			@Override
			public void nodeTraversed(Node node) {
			}
		}, visitedNodes);

		return visitedNodes.keySet();
	}
}
