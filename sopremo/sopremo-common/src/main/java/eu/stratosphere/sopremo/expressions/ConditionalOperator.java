package eu.stratosphere.sopremo.expressions;

import java.util.List;

import eu.stratosphere.sopremo.DataStream;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.Operator;

public abstract class ConditionalOperator extends Operator {

	private Condition condition;

	public ConditionalOperator(Evaluable transformation, Condition condition, List<? extends DataStream> inputs) {
		super(transformation, inputs);
		this.condition = condition;
	}

	public ConditionalOperator(Evaluable transformation, Condition condition, DataStream... inputs) {
		super(transformation, inputs);
		this.condition = condition;
	}

	public Condition getCondition() {
		return this.condition;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" on ").append(this.condition);
		if (this.getEvaluableExpression() != EvaluableExpression.IDENTITY)
			builder.append(" to ").append(this.getEvaluableExpression());
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result + this.condition.hashCode();
		return result;
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

}
