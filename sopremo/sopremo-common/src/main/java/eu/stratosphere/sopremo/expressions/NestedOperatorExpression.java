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

	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + operator.hashCode();
		return result;
	}



	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		NestedOperatorExpression other = (NestedOperatorExpression) obj;
		return operator.equals(other.operator);
	}

@Override
public void toString(StringBuilder builder) {
	this.appendTags(builder);
	builder.append("<").append(this.operator).append(">");
}

	public Operator<?> getOperator() {
		return this.operator;
	}
}
