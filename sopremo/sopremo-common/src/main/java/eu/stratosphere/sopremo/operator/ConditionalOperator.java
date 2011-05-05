package eu.stratosphere.sopremo.operator;

import java.util.List;

import eu.stratosphere.nephele.configuration.Configuration;
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

	public Condition getCondition() {
		return this.condition;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" on ").append(this.condition);
		if (this.getTransformation() != Transformation.IDENTITY)
			builder.append(" to ").append(this.getTransformation());
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

	protected static void setCondition(Configuration config, String key, Condition condition) {
		config.setString(key, objectToString(condition));
	}

	protected static Condition getCondition(Configuration config, String key) {
		String string = config.getString(key, null);
		if (string == null)
			return null;
		return (Condition) stringToObject(string);
	}

}
