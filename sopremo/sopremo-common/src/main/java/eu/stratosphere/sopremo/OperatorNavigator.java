package eu.stratosphere.sopremo;

import eu.stratosphere.dag.Navigator;

public final class OperatorNavigator implements Navigator<Operator> {
	public final static OperatorNavigator INSTANCE = new OperatorNavigator();
	
	@Override
	public Iterable<Operator> getConnectedNodes(Operator node) {
		return node.getInputOperators();
	}
}