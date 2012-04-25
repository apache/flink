package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class SetOperation<Op extends SetOperation<Op>> extends CompositeOperator<Op> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5431211249370548419L;

	public SetOperation() {
	}

	@Override
	public SopremoModule asElementaryOperators() {
		final int numInputs = this.getInputOperators().size();
		final SopremoModule module = new SopremoModule(this.getName(), numInputs, 1);

		// successively connect binary operators
		// connect the result of one binary operator with each new input
		Operator<?> leftInput = module.getInput(0);
		for (int index = 1; index < numInputs; index++)
			leftInput = createBinaryOperations(leftInput, module.getInput(index));

		module.getOutput(0).setInput(0, leftInput);

		return module;
	}

	/**
	 * Creates a binary operator for two streams.
	 */
	protected abstract Operator<?> createBinaryOperations(JsonStream leftInput, JsonStream rightInput);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.CompositeOperator#getKeyExpressions()
	 */
	@Override
	public Iterable<? extends EvaluationExpression> getKeyExpressions() {
		return ALL_KEYS;
	}
}
