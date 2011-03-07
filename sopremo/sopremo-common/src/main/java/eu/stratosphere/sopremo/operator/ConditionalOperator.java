package eu.stratosphere.sopremo.operator;

import java.util.Collection;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Transformation;

public class ConditionalOperator extends Operator {

	private Condition condition;

	public ConditionalOperator(Transformation transformation, Condition condition, Collection<Operator> inputs) {
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
		builder.append(", ").append(condition);
		if (getTransformation() != Transformation.IDENTITY)
			builder.append(", ").append(getTransformation());
		return builder.toString();
	}
}
