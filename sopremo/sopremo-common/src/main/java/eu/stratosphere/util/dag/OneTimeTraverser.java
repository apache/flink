package eu.stratosphere.util.dag;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.util.IdentityList;
import eu.stratosphere.util.IdentitySet;

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
			final ConnectionNavigator<Node> navigator) {
		final List<Node> visitedNodes = new IdentityList<Node>();

		this.visitNodes(startNodes, navigator, new GraphTraverseListener<Node>() {
			@Override
			public void nodeTraversed(final Node node) {
			}
		}, visitedNodes);

		return visitedNodes;
	}

	@Override
	public <Node> void traverse(final Iterator<? extends Node> startNodes, final ConnectionNavigator<Node> navigator,
			final GraphTraverseListener<Node> listener) {
		final Set<Node> visitedNodes = new IdentitySet<Node>();

		this.visitNodes(startNodes, navigator, listener, visitedNodes);
	}

	private <Node> void visitNodes(final Iterator<? extends Node> startNodes,
			final ConnectionNavigator<Node> navigator,
			final GraphTraverseListener<Node> listener, final Collection<Node> visitedNodes) {
		while (startNodes.hasNext()) {
			final Node node = startNodes.next();
			if (node != null && !visitedNodes.contains(node)) {
				visitedNodes.add(node);
				listener.nodeTraversed(node);
				this.visitNodes(navigator.getConnectedNodes(node).iterator(), navigator, listener, visitedNodes);
			}
		}
	}
}
