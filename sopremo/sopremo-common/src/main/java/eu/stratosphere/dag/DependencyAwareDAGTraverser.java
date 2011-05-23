package eu.stratosphere.dag;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.dag.DAGLevelPartitioner.Level;

/**
 * Traverses a directed acyclic graph guaranteeing that all nodes with outgoing edges to a nodes are visited before that
 * node. Additionally, every node is visited exactly once.
 * 
 * @author Arvid Heise
 */
public class DependencyAwareDAGTraverser extends AbstractDAGTraverser implements DAGTraverser {
	/**
	 * The default instance of the {@link DependencyAwareDAGTraverser}.
	 */
	public final static DependencyAwareDAGTraverser INSTANCE = new DependencyAwareDAGTraverser();
	
	@Override
	public <Node> void traverse(Iterator<Node> startNodes, Navigator<Node> navigator, DAGTraverseListener<Node> listener) {
		List<Level<Node>> levels = DAGLevelPartitioner.getLevels(startNodes, navigator);
		for (Level<Node> level : levels)
			for (Node node : level.getLevelNodes())
				listener.nodeTraversed(node);
	}
}
