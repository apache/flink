package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.Operator;

public class NestedOperatorExpression extends UnevaluableExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2006595670580919325L;

	private final Operator<?> operator;

	public NestedOperatorExpression(final Operator<?> operator) {
		super("Nested operator: " + operator);
		this.operator = operator;
	}

	public Operator<?> getOperator() {
		return this.operator;
	}
}
