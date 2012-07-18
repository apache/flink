package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.Operator;

/**
 * This expression represents all {@link Operator}s.
 */
public class NestedOperatorExpression extends UnevaluableExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2006595670580919325L;

	private final Operator<?> operator;

	/**
	 * Initializes a NestedOperatorExpression with the given {@link Operator}.
	 * 
	 * @param operator
	 *        the operator that should be represented by this expression
	 */
	public NestedOperatorExpression(final Operator<?> operator) {
		super("Nested operator: " + operator);
		this.operator = operator;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.operator.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final NestedOperatorExpression other = (NestedOperatorExpression) obj;
		return this.operator.equals(other.operator);
	}

	@Override
	public void toString(final StringBuilder builder) {
		this.appendTags(builder);
		builder.append("<").append(this.operator).append(">");
	}

	/**
	 * Returns the operator
	 * 
	 * @return the operator
	 */
	public Operator<?> getOperator() {
		return this.operator;
	}
}
