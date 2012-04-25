package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.util.CollectionUtil;
import eu.stratosphere.util.ConversionIterable;

public abstract class MultiSourceOperator<Self extends MultiSourceOperator<Self>> extends CompositeOperator<Self> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4054964985762240025L;

	private final Map<Operator<?>.Output, EvaluationExpression> valueProjections =
		new IdentityHashMap<Operator<?>.Output, EvaluationExpression>();

	private final Map<Operator<?>.Output, List<? extends EvaluationExpression>> keyExpressions =
		new IdentityHashMap<Operator<?>.Output, List<? extends EvaluationExpression>>();

	private EvaluationExpression defaultValueProjection = EvaluationExpression.VALUE;

	private List<? extends EvaluationExpression> defaultKeyExpressions = Arrays.asList(new ConstantExpression(
		NullNode.getInstance()));

	@Override
	public SopremoModule asElementaryOperators() {
		final int numInputs = this.getInputOperators().size();
		final SopremoModule module = new SopremoModule(this.getName(), numInputs, 1);

		final List<Operator<?>> inputs = new ArrayList<Operator<?>>();
		for (int index = 0; index < numInputs; index++)
			inputs.add(new Projection().
				withTransformation(this.getValueProjection(index)).
				withInputs(module.getInput(index)));

		module.getOutput(0).setInput(0, this.createElementaryOperations(inputs));

		return module;
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

		int size = getInputs().size();
		if (other.getInputs().size() != size)
			return false;

		for (int index = 0; index < size; index++)
			if (!getValueProjection(index).equals(other.getValueProjection(index)))
				return false;

		return true;
	}

	@SuppressWarnings("unused")
	protected EvaluationExpression getDefaultValueProjection(final JsonStream input) {
		return this.defaultValueProjection;
	}

	protected List<? extends EvaluationExpression> getKeyExpressions(final int index) {
		return this.getKeyExpressions(this.getInput(index));
	}

	protected List<? extends EvaluationExpression> getKeyExpressions(final JsonStream input) {
		final Operator<?>.Output source = input == null ? null : input.getSource();
		List<? extends EvaluationExpression> keyExpressions = this.keyExpressions.get(source);
		if (keyExpressions == null)
			keyExpressions = this.getDefaultKeyExpressions();
		return keyExpressions;
	}

	public List<? extends EvaluationExpression> getDefaultKeyExpressions() {
		return this.defaultKeyExpressions;
	}

	public void setDefaultKeyExpressions(List<? extends EvaluationExpression> defaultKeyExpressions) {
		if (defaultKeyExpressions == null)
			throw new NullPointerException("defaultKeyExpressions must not be null");

		this.defaultKeyExpressions = defaultKeyExpressions;
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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.CompositeOperator#getKeyExpressions()
	 */
	@Override
	public Iterable<? extends EvaluationExpression> getKeyExpressions() {
		return CollectionUtil.mergeUnique(new ConversionIterable<JsonStream, List<? extends EvaluationExpression>>(
			getInputs()) {
			@Override
			protected List<? extends EvaluationExpression> convert(JsonStream stream) {
				return getKeyExpressions(stream);
			};
		});
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.keyExpressions.hashCode();
		result = prime * result + this.valueProjections.hashCode();
		return result;
	}

	protected void setDefaultValueProjection(final EvaluationExpression defaultValueProjection) {
		this.defaultValueProjection = defaultValueProjection;
	}

	protected void setKeyExpressions(final int inputIndex, final List<? extends EvaluationExpression> keyExpressions) {
		this.setKeyExpressions(getSafeInput(inputIndex), keyExpressions);
	}

	protected void setKeyExpressions(final JsonStream input, final List<? extends EvaluationExpression> keyExpressions) {
		if (keyExpressions == null)
			throw new NullPointerException("keyExpression must not be null");

		this.keyExpressions.put(input.getSource(), keyExpressions);
	}

	protected void setValueProjection(final int inputIndex, final EvaluationExpression valueProjection) {
		this.setValueProjection(this.getSafeInput(inputIndex), valueProjection);
	}

	protected void setValueProjection(final JsonStream input, final EvaluationExpression valueProjection) {
		if (valueProjection == null)
			throw new NullPointerException("valueProjection must not be null");

		this.valueProjections.put(input.getSource(), valueProjection);
	}

	@SuppressWarnings("unchecked")
	protected Self withValueProjection(final int inputIndex, final EvaluationExpression valueProjection) {
		this.setValueProjection(inputIndex, valueProjection);
		return (Self) this;
	}

	protected Self withKeyExpressions(final int inputIndex, final List<? extends EvaluationExpression> keyExpressions) {
		this.setKeyExpressions(inputIndex, keyExpressions);
		return this.self();
	}

	protected Self withValueProjection(final EvaluationExpression valueProjection) {
		this.setDefaultValueProjection(valueProjection);
		return this.self();
	}

	// @Override
	// public String toString() {
	// StringBuilder builder = new StringBuilder(this.getName()).append(" on ");
	// List<Output> inputs = this.getInputs();
	// builder.append(this.getKeyExpression(0)).append("/").append(this.getValueProjection(0));
	// for (int index = 1; index < inputs.size(); index++)
	// builder.append(", ").append(this.getKeyExpression(index)).append("/")
	// .append(this.getValueProjection(index));
	// return builder.toString();
	// }
}
