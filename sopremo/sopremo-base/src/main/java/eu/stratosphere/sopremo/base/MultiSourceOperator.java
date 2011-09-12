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

public abstract class MultiSourceOperator<Op extends MultiSourceOperator<Op>> extends CompositeOperator {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4054964985762240025L;

	private final Map<Operator.Output, EvaluationExpression>
			keyProjections = new IdentityHashMap<Operator.Output, EvaluationExpression>(),
			valueProjections = new IdentityHashMap<Operator.Output, EvaluationExpression>();

	private EvaluationExpression defaultKeyProjection = EvaluationExpression.NULL;

	private EvaluationExpression defaultValueProjection = EvaluationExpression.VALUE;

	private boolean resetKey = true;

	public MultiSourceOperator(final JsonStream... inputs) {
		super(inputs);
	}

	public MultiSourceOperator(final List<? extends JsonStream> inputs) {
		super(inputs);
	}

	@Override
	public SopremoModule asElementaryOperators() {
		final int numInputs = this.getInputOperators().size();
		final SopremoModule module = new SopremoModule(this.getName(), numInputs, 1);

		final List<Operator> inputs = new ArrayList<Operator>();
		for (int index = 0; index < numInputs; index++)
			inputs.add(new Projection(module.getInput(index)).
				withKeyTransformation(this.getKeyProjection(index)).
				withValueTransformation(this.getValueProjection(index)));

		Operator lastOperator = this.createElementaryOperations(inputs);

		if (resetKey)
			lastOperator = new Projection(lastOperator).withKeyTransformation(EvaluationExpression.NULL);
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
	public Op withResetKey(boolean resetKey) {
		setResetKey(resetKey);
		return (Op) this;
	}

	protected abstract Operator createElementaryOperations(List<Operator> inputs);

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

	protected EvaluationExpression getDefaultKeyProjection(final Output source) {
		return this.defaultKeyProjection;
	}

	protected EvaluationExpression getDefaultValueProjection(final Output source) {
		return this.defaultValueProjection;
	}

	public EvaluationExpression getKeyProjection(final int index) {
		return this.getKeyProjection(this.getInput(index));
	}

	public EvaluationExpression getKeyProjection(final JsonStream input) {
		final Output source = input == null ? null : input.getSource();
		EvaluationExpression keyProjection = this.keyProjections.get(source);
		if (keyProjection == null)
			keyProjection = this.getDefaultKeyProjection(source);
		return keyProjection;
	}

	public EvaluationExpression getValueProjection(final int index) {
		return this.getValueProjection(this.getInput(index));
	}

	public EvaluationExpression getValueProjection(final JsonStream input) {
		final Output source = input == null ? null : input.getSource();
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

	public void setKeyProjection(final int inputIndex, final EvaluationExpression keyProjection) {
		this.setKeyProjection(this.getInput(inputIndex), keyProjection);
	}

	public void setKeyProjection(final JsonStream input, final EvaluationExpression keyProjection) {
		if (keyProjection == null)
			throw new NullPointerException("keyProjection must not be null");

		this.keyProjections.put(input.getSource(), keyProjection);
	}

	public void setValueProjection(final int inputIndex, final EvaluationExpression valueProjection) {
		this.setValueProjection(this.getInput(inputIndex), valueProjection);
	}

	public void setValueProjection(final JsonStream input, final EvaluationExpression valueProjection) {
		if (valueProjection == null)
			throw new NullPointerException("valueProjection must not be null");

		this.valueProjections.put(input.getSource(), valueProjection);
	}

	@SuppressWarnings("unchecked")
	public Op withValueProjection(final int inputIndex, final EvaluationExpression valueProjection) {
		setValueProjection(inputIndex, valueProjection);
		return (Op) this;
	}

	@SuppressWarnings("unchecked")
	public Op withKeyProjection(final int inputIndex, final EvaluationExpression valueProjection) {
		setKeyProjection(inputIndex, valueProjection);
		return (Op) this;
	}

	@SuppressWarnings("unchecked")
	public Op withValueProjection(final EvaluationExpression valueProjection) {
		setDefaultValueProjection(valueProjection);
		return (Op) this;
	}

	@SuppressWarnings("unchecked")
	public Op withKeyProjection(final EvaluationExpression valueProjection) {
		setDefaultKeyProjection(valueProjection);
		return (Op) this;
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
