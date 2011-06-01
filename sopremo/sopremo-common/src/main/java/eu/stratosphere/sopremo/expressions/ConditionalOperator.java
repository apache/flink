package eu.stratosphere.sopremo.expressions;

import java.util.List;

import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;

public abstract class ConditionalOperator extends Operator {

	private Condition condition;

	public ConditionalOperator(EvaluableExpression transformation, Condition condition, JsonStream... inputs) {
		super(transformation, inputs);
		this.condition = condition;
	}

	public ConditionalOperator(EvaluableExpression transformation, Condition condition,
			List<? extends JsonStream> inputs) {
		super(transformation, inputs);
		this.condition = condition;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		return super.equals(obj) && this.condition.equals(((ConditionalOperator) obj).condition);
	}

	public Condition getCondition() {
		return this.condition;
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result + this.condition.hashCode();
		return result;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" on ").append(this.condition);
		if (this.getTransformation() != EvaluableExpression.IDENTITY)
			builder.append(" to ").append(this.getTransformation());
		return builder.toString();
	}

}
