package eu.stratosphere.sopremo;

import eu.stratosphere.dag.Navigator;

public final class OperatorNavigator implements Navigator<Operator> {
	@Override
	public Iterable<Operator> getConnectedNodes(Operator node) {
		return node.getInputs();
	}
}