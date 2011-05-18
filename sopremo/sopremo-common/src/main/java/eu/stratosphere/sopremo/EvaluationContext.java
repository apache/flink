package eu.stratosphere.sopremo;

import eu.stratosphere.sopremo.function.FunctionRegistry;

public class EvaluationContext implements SopremoType {
	private FunctionRegistry functionRegistry = new FunctionRegistry();

	public FunctionRegistry getFunctionRegistry() {
		return functionRegistry;
	}
}
