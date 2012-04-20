package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class SetOperation<Op extends SetOperation<Op>> extends MultiSourceOperator<Op> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5431211249370548419L;

	public SetOperation() {
		this.setDefaultKeyProjection(EvaluationExpression.VALUE);
	}

	@Property(input = true)
	@Name(preposition = "on")
	public void setIdentityKey(int inputIndex, EvaluationExpression keyProjection) {
		super.setKeyProjection(inputIndex, keyProjection);
	}

	public EvaluationExpression getIdentityKey(int inputIndex) {
		return this.getKeyProjection(inputIndex);
	}

	public Op withIdentityKey(int inputIndex, EvaluationExpression identityKey) {
		this.setIdentityKey(inputIndex, identityKey);
		return this.self();
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
		builder.append(" on ").append(this.getKeyProjection(0));
		for (int index = 1; index < this.getInputs().size(); index++)
			builder.append(", ").append(this.getKeyProjection(index));
		return builder.toString();
	}
}
