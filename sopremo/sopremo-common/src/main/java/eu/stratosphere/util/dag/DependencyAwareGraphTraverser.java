package eu.stratosphere.util.dag;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.util.dag.GraphLevelPartitioner.Level;

/**
 * Traverses a directed acyclic graph guaranteeing that all nodes with outgoing edges to a nodes are visited before that
 * node. Additionally, every node is visited exactly once.
 * 
 * @author Arvid Heise
 */
public class DependencyAwareGraphTraverser extends AbstractGraphTraverser implements GraphTraverser {
	/**
	 * The default instance of the {@link DependencyAwareGraphTraverser}.
	 */
	public final static DependencyAwareGraphTraverser INSTANCE = new DependencyAwareGraphTraverser();

	@Override
	public <Node> void traverse(final Iterator<? extends Node> startNodes, final Navigator<Node> navigator,
			final GraphTraverseListener<Node> listener) {
		final List<Level<Node>> levels = GraphLevelPartitioner.getLevels(startNodes, navigator);
		for (final Level<Node> level : levels)
			for (final Node node : level.getLevelNodes())
				listener.nodeTraversed(node);
	}
}
