package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.ElementarySopremoModule;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.OutputCardinality;

@InputCardinality(min = 1)
@OutputCardinality(1)
public abstract class SetOperation<Op extends SetOperation<Op>> extends CompositeOperator<Op> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5431211249370548419L;

	public SetOperation() {
	}

	@Override
	public ElementarySopremoModule asElementaryOperators(EvaluationContext context) {
		final int numInputs = this.getInputOperators().size();
		final ElementarySopremoModule module = new ElementarySopremoModule(this.getName(), numInputs, 1);

		// successively connect binary operators
		// connect the result of one binary operator with each new input
		ElementaryOperator<?> leftInput = module.getInput(0);
		for (int index = 1; index < numInputs; index++) {
			leftInput = createBinaryOperations(leftInput, module.getInput(index));
			leftInput.setKeyExpressions(0, ALL_KEYS);
			leftInput.setKeyExpressions(1, ALL_KEYS);
		}

		module.getOutput(0).setInput(0, leftInput);

		return module;
	}

	/**
	 * Creates a binary operator for two streams.
	 */
	protected abstract ElementaryOperator<?> createBinaryOperations(JsonStream leftInput, JsonStream rightInput);
}
