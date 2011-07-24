package eu.stratosphere.sopremo;

import eu.stratosphere.sopremo.function.FunctionRegistry;

/**
 * Provides additional context to the evaluation of {@link Evaluable}s, such as access to all registered functions.
 * 
 * @author Arvid Heise
 */
public class EvaluationContext implements SerializableSopremoType {
	private static final long serialVersionUID = 7701485388451926506L;

	private FunctionRegistry functionRegistry = new FunctionRegistry();

	private int inputCounter = 0;

	public void increaseInputCounter() {
		this.inputCounter++;
	}

	public int getInputCounter() {
		return this.inputCounter;
	}

	/**
	 * Returns the {@link FunctionRegistry} containing all registered function in the current evaluation context.
	 * 
	 * @return the FunctionRegistry
	 */
	public FunctionRegistry getFunctionRegistry() {
		return this.functionRegistry;
	}
}
