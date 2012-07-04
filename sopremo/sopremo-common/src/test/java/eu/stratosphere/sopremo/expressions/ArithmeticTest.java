package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;

public class ArithmeticTest extends EvaluableExpressionTest<ArithmeticExpression> {
	@Override
	protected ArithmeticExpression createDefaultInstance(final int index) {
		return new ArithmeticExpression(new ConstantExpression(1), ArithmeticOperator.ADDITION, new ConstantExpression(
			index));
	}
}
