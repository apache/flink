package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;

public abstract class MultiSourceOperator extends CompositeOperator {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4054964985762240025L;

	private Map<Operator.Output, EvaluableExpression>
			keyProjections = new IdentityHashMap<Operator.Output, EvaluableExpression>(),
			valueProjections = new IdentityHashMap<Operator.Output, EvaluableExpression>();

	private EvaluableExpression defaultKeyProjection = EvaluableExpression.NULL;

	private EvaluableExpression defaultValueProjection = EvaluableExpression.SAME_VALUE;

	public MultiSourceOperator(JsonStream... inputs) {
		super(inputs);
	}

	public MultiSourceOperator(List<? extends JsonStream> inputs) {
		super(inputs);
	}

	@Override
	public SopremoModule asElementaryOperators() {
		int numInputs = this.getInputOperators().size();
		SopremoModule module = new SopremoModule(this.getName(), numInputs, 1);

		List<Operator> inputs = new ArrayList<Operator>();
		for (int index = 0; index < numInputs; index++)
			inputs.add(new Projection(this.getKeyProjection(index), this.getValueProjection(index), module
				.getInput(index)));

		Operator lastOperator = this.createElementaryOperations(inputs);

		module.getOutput(0).setInput(0,
			new Projection(EvaluableExpression.NULL, EvaluableExpression.SAME_VALUE, lastOperator));

		return module;
	}

	protected abstract Operator createElementaryOperations(List<Operator> inputs);

	public EvaluableExpression getKeyProjection(int index) {
		return this.getKeyProjection(this.getInput(index));
	}

	public EvaluableExpression getKeyProjection(JsonStream input) {
		Output source = input == null ? null : input.getSource();
		EvaluableExpression keyProjection = this.keyProjections.get(source);
		if (keyProjection == null)
			keyProjection = this.getDefaultKeyProjection(source);
		return keyProjection;
	}

	protected EvaluableExpression getDefaultKeyProjection(Output source) {
		return this.defaultKeyProjection;
	}

	protected EvaluableExpression getDefaultValueProjection(Output source) {
		return this.defaultValueProjection;
	}

	public void setKeyProjection(int inputIndex, EvaluableExpression keyProjection) {
		this.setKeyProjection(this.getInput(inputIndex), keyProjection);
	}

	public void setKeyProjection(JsonStream input, EvaluableExpression keyProjection) {
		if (keyProjection == null)
			throw new NullPointerException("keyProjection must not be null");

		this.keyProjections.put(input.getSource(), keyProjection);
	}

	public EvaluableExpression getValueProjection(int index) {
		return this.getValueProjection(this.getInput(index));
	}

	public EvaluableExpression getValueProjection(JsonStream input) {
		Output source = input == null ? null : input.getSource();
		EvaluableExpression valueProjection = this.valueProjections.get(source);
		if (valueProjection == null)
			valueProjection = this.getDefaultValueProjection(source);
		return valueProjection;
	}

	public void setValueProjection(int inputIndex, EvaluableExpression valueProjection) {
		this.setValueProjection(this.getInput(inputIndex), valueProjection);
	}

	public void setValueProjection(JsonStream input, EvaluableExpression valueProjection) {
		if (valueProjection == null)
			throw new NullPointerException("valueProjection must not be null");

		this.valueProjections.put(input.getSource(), valueProjection);
	}

	protected void setDefaultValueProjection(EvaluableExpression defaultValueProjection) {
		this.defaultValueProjection = defaultValueProjection;
	}

	protected void setDefaultKeyProjection(EvaluableExpression defaultKeyProjection) {
		this.defaultKeyProjection = defaultKeyProjection;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.keyProjections.hashCode();
		result = prime * result + this.valueProjections.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		MultiSourceOperator other = (MultiSourceOperator) obj;
		return this.keyProjections.equals(other.keyProjections) && this.valueProjections.equals(other.valueProjections);
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
