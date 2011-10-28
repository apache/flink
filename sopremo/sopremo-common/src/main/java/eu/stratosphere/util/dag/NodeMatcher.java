package eu.stratosphere.util.dag;

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Arvid Heise
 */
public class NodeMatcher {
	/**
	 * The default, stateless instance of the {@link NodeMatcher}.
	 */
	public static final NodeMatcher INSTANCE = new NodeMatcher();

//	private 
	
	public <T> void match(Iterator<? extends T> nodes, Navigator<T> navigator, Iterator<? extends T> patterns) {
		OneTimeTraverser.INSTANCE.traverse(nodes, navigator, new GraphTraverseListener<T>() {
			public void nodeTraversed(T node) {
				
			};
		});
	}
}
