package eu.stratosphere.util.dag;

import java.util.Iterator;

/**
 * @author Arvid Heise
 */
public class NodeMatcher {
	/**
	 * The default, stateless instance of the {@link NodeMatcher}.
	 */
	public static final NodeMatcher INSTANCE = new NodeMatcher();

	// private

	public <T> void match(final Iterator<? extends T> nodes, final ConnectionNavigator<T> navigator,
			final Iterator<? extends T> patterns) {
		OneTimeTraverser.INSTANCE.traverse(nodes, navigator, new GraphTraverseListener<T>() {
			@Override
			public void nodeTraversed(final T node) {

			};
		});
	}
}
