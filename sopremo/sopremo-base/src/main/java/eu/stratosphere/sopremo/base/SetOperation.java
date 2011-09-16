package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class SetOperation extends MultiSourceOperator<Intersection> {
	public SetOperation() {
		this.setDefaultKeyProjection(EvaluationExpression.VALUE);
	}

	@Property(input = true)
	@Name(preposition = "on")
	public void setIdentityKey(int inputIndex, EvaluationExpression keyProjection) {
		super.setKeyProjection(inputIndex, keyProjection);
	}
	
	public EvaluationExpression getIdentityKey(int inputIndex) {
		return getKeyProjection(inputIndex);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(super.toString());
		builder.append(" on ").append(getKeyProjection(0));
		for (int index = 1; index < getInputs().size(); index++) 
			builder.append(", ").append(getKeyProjection(index));
		return builder.toString();
	}
}
