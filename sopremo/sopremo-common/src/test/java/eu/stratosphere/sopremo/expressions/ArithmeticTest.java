package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.expressions.Arithmetic.ArithmeticOperator;

public class ArithmeticTest extends EvaluableExpressionTest<Arithmetic> {
	@Override
	protected Arithmetic createDefaultInstance(int index) {
		return new Arithmetic(new Constant(1), ArithmeticOperator.ADDITION, new Constant(index));
	}
}
