package eu.stratosphere.sopremo.operator;

import java.util.Collection;
import java.util.List;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.Transformation;

public abstract class ConditionalOperator extends Operator {

	private Condition condition;

	public ConditionalOperator(Transformation transformation, Condition condition, List<Operator> inputs) {
		super(transformation, inputs);
		this.condition = condition;
	}

	public ConditionalOperator(Transformation transformation, Condition condition, Operator... inputs) {
		super(transformation, inputs);
		this.condition = condition;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(getName());
		builder.append(" on ").append(condition);
		if (getTransformation() != Transformation.IDENTITY)
			builder.append(" to ").append(getTransformation());
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result + condition.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		return super.equals(obj) && condition.equals(((ConditionalOperator) obj).condition);
	}

}
