package eu.stratosphere.dag;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.dag.GraphLevelPartitioner.Level;

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
	public <Node> void traverse(Iterator<Node> startNodes, Navigator<Node> navigator, GraphTraverseListener<Node> listener) {
		List<Level<Node>> levels = GraphLevelPartitioner.getLevels(startNodes, navigator);
		for (Level<Node> level : levels)
			for (Node node : level.getLevelNodes())
				listener.nodeTraversed(node);
	}
}
