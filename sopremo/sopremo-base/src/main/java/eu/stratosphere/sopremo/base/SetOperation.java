package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class SetOperation<Op extends SetOperation<Op>>  extends MultiSourceOperator<Op> {
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

	public Op withIdentityKey(int inputIndex, EvaluationExpression identityKey) {
		setIdentityKey(inputIndex, identityKey);
		return (Op) this;
	}
	
	@Override
	public void setValueProjection(int inputIndex, EvaluationExpression valueProjection) {
		super.setValueProjection(inputIndex, valueProjection);
	}
	
	@Override
	public EvaluationExpression getValueProjection(int index) {
		return super.getValueProjection(index);
	}
	
	@Override
	public Op withValueProjection(int inputIndex, EvaluationExpression valueProjection) {
		return super.withValueProjection(inputIndex, valueProjection);
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
