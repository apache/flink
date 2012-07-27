package eu.stratosphere.sopremo.operator;

import java.util.List;

import eu.stratosphere.util.dag.ConnectionNavigator;

/**
 * Provides a mean to traverse the directed acyclic graph of interconnected {@link Operator<?>}s.
 * 
 * @author Arvid Heise
 */
public final class OperatorNavigator implements ConnectionNavigator<Operator<?>> {
	/**
	 * The default, stateless instance.
	 */
	public final static OperatorNavigator INSTANCE = new OperatorNavigator();

	@Override
	public List<Operator<?>> getConnectedNodes(final Operator<?> node) {
		return node.getInputOperators();
	}
}