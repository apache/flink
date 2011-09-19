package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class MultiSourceOperator<Self extends MultiSourceOperator<Self>> extends CompositeOperator<Self> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4054964985762240025L;

	private final Map<Operator<?>.Output, EvaluationExpression>
			keyProjections = new IdentityHashMap<Operator<?>.Output, EvaluationExpression>(),
			valueProjections = new IdentityHashMap<Operator<?>.Output, EvaluationExpression>();

	private EvaluationExpression defaultKeyProjection = EvaluationExpression.NULL;

	private EvaluationExpression defaultValueProjection = EvaluationExpression.VALUE;

	private boolean resetKey = true;

	@Override
	public SopremoModule asElementaryOperators() {
		final int numInputs = this.getInputOperators().size();
		final SopremoModule module = new SopremoModule(this.getName(), numInputs, 1);

		final List<Operator<?>> inputs = new ArrayList<Operator<?>>();
		for (int index = 0; index < numInputs; index++)
			inputs.add(new Projection().
				withKeyTransformation(this.getKeyProjection(index)).
				withValueTransformation(this.getValueProjection(index)).
				withInputs(module.getInput(index)));

		Operator<?> lastOperator = this.createElementaryOperations(inputs);

		if (resetKey)
			lastOperator = new Projection().
				withKeyTransformation(EvaluationExpression.NULL).
				withInputs(lastOperator);
		module.getOutput(0).setInput(0, lastOperator);

		return module;
	}

	public boolean isResetKey() {
		return this.resetKey;
	}

	public void setResetKey(boolean resetKey) {
		this.resetKey = resetKey;
	}

	@SuppressWarnings("unchecked")
	public Self withResetKey(boolean resetKey) {
		setResetKey(resetKey);
		return (Self) this;
	}

	protected abstract Operator<?> createElementaryOperations(List<Operator<?>> inputs);

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final MultiSourceOperator<?> other = (MultiSourceOperator<?>) obj;
		return this.keyProjections.equals(other.keyProjections) && this.valueProjections.equals(other.valueProjections);
	}

	protected EvaluationExpression getDefaultKeyProjection(final Operator<?>.Output source) {
		return this.defaultKeyProjection;
	}

	protected EvaluationExpression getDefaultValueProjection(final Operator<?>.Output source) {
		return this.defaultValueProjection;
	}

	protected EvaluationExpression getKeyProjection(final int index) {
		return this.getKeyProjection(this.getInput(index));
	}

	protected EvaluationExpression getKeyProjection(final JsonStream input) {
		final Operator<?>.Output source = input == null ? null : input.getSource();
		EvaluationExpression keyProjection = this.keyProjections.get(source);
		if (keyProjection == null)
			keyProjection = this.getDefaultKeyProjection(source);
		return keyProjection;
	}

	protected EvaluationExpression getValueProjection(final int index) {
		return this.getValueProjection(this.getInput(index));
	}

	protected EvaluationExpression getValueProjection(final JsonStream input) {
		final Operator<?>.Output source = input == null ? null : input.getSource();
		EvaluationExpression valueProjection = this.valueProjections.get(source);
		if (valueProjection == null)
			valueProjection = this.getDefaultValueProjection(source);
		return valueProjection;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.keyProjections.hashCode();
		result = prime * result + this.valueProjections.hashCode();
		return result;
	}

	protected void setDefaultKeyProjection(final EvaluationExpression defaultKeyProjection) {
		this.defaultKeyProjection = defaultKeyProjection;
	}

	protected void setDefaultValueProjection(final EvaluationExpression defaultValueProjection) {
		this.defaultValueProjection = defaultValueProjection;
	}

	protected void setKeyProjection(final int inputIndex, final EvaluationExpression keyProjection) {
		this.setKeyProjection(this.getInput(inputIndex), keyProjection);
	}

	protected void setKeyProjection(final JsonStream input, final EvaluationExpression keyProjection) {
		if (keyProjection == null)
			throw new NullPointerException("keyProjection must not be null");

		this.keyProjections.put(input.getSource(), keyProjection);
	}

	protected void setValueProjection(final int inputIndex, final EvaluationExpression valueProjection) {
		this.setValueProjection(this.getInput(inputIndex), valueProjection);
	}

	protected void setValueProjection(final JsonStream input, final EvaluationExpression valueProjection) {
		if (valueProjection == null)
			throw new NullPointerException("valueProjection must not be null");

		this.valueProjections.put(input.getSource(), valueProjection);
	}

	@SuppressWarnings("unchecked")
	protected Self withValueProjection(final int inputIndex, final EvaluationExpression valueProjection) {
		setValueProjection(inputIndex, valueProjection);
		return (Self) this;
	}

	protected Self withKeyProjection(final int inputIndex, final EvaluationExpression valueProjection) {
		setKeyProjection(inputIndex, valueProjection);
		return self();
	}

	protected Self withValueProjection(final EvaluationExpression valueProjection) {
		setDefaultValueProjection(valueProjection);
		return self();
	}

	protected Self withKeyProjection(final EvaluationExpression valueProjection) {
		setDefaultKeyProjection(valueProjection);
		return self();
	}
	// @Override
	// public String toString() {
	// StringBuilder builder = new StringBuilder(this.getName()).append(" on ");
	// List<Output> inputs = this.getInputs();
	// builder.append(this.getKeyProjection(0)).append("/").append(this.getValueProjection(0));
	// for (int index = 1; index < inputs.size(); index++)
	// builder.append(", ").append(this.getKeyProjection(index)).append("/")
	// .append(this.getValueProjection(index));
	// return builder.toString();
	// }
}
