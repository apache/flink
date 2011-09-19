package eu.stratosphere.sopremo;

import eu.stratosphere.util.dag.Navigator;

/**
 * Provides a mean to traverse the directed acyclic graph of interconnected {@link Operator<?>}s.
 * 
 * @author Arvid Heise
 */
public final class OperatorNavigator implements Navigator<Operator<?>> {
	/**
	 * The default, stateless instance.
	 */
	public final static OperatorNavigator INSTANCE = new OperatorNavigator();

	@Override
	public Iterable<Operator<?>> getConnectedNodes(final Operator<?> node) {
		return node.getInputOperators();
	}
}