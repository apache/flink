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
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class MultiSourceOperator extends CompositeOperator {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4054964985762240025L;

	private Map<Operator.Output, EvaluationExpression>
			keyProjections = new IdentityHashMap<Operator.Output, EvaluationExpression>(),
			valueProjections = new IdentityHashMap<Operator.Output, EvaluationExpression>();

	private EvaluationExpression defaultKeyProjection = EvaluationExpression.NULL;

	private EvaluationExpression defaultValueProjection = EvaluationExpression.SAME_VALUE;

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
			new Projection(EvaluationExpression.NULL, EvaluationExpression.SAME_VALUE, lastOperator));

		return module;
	}

	protected abstract Operator createElementaryOperations(List<Operator> inputs);

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

	protected EvaluationExpression getDefaultKeyProjection(Output source) {
		return this.defaultKeyProjection;
	}

	protected EvaluationExpression getDefaultValueProjection(Output source) {
		return this.defaultValueProjection;
	}

	public EvaluationExpression getKeyProjection(int index) {
		return this.getKeyProjection(this.getInput(index));
	}

	public EvaluationExpression getKeyProjection(JsonStream input) {
		Output source = input == null ? null : input.getSource();
		EvaluationExpression keyProjection = this.keyProjections.get(source);
		if (keyProjection == null)
			keyProjection = this.getDefaultKeyProjection(source);
		return keyProjection;
	}

	public EvaluationExpression getValueProjection(int index) {
		return this.getValueProjection(this.getInput(index));
	}

	public EvaluationExpression getValueProjection(JsonStream input) {
		Output source = input == null ? null : input.getSource();
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

	protected void setDefaultKeyProjection(EvaluationExpression defaultKeyProjection) {
		this.defaultKeyProjection = defaultKeyProjection;
	}

	protected void setDefaultValueProjection(EvaluationExpression defaultValueProjection) {
		this.defaultValueProjection = defaultValueProjection;
	}

	public void setKeyProjection(int inputIndex, EvaluationExpression keyProjection) {
		this.setKeyProjection(this.getInput(inputIndex), keyProjection);
	}

	public void setKeyProjection(JsonStream input, EvaluationExpression keyProjection) {
		if (keyProjection == null)
			throw new NullPointerException("keyProjection must not be null");

		this.keyProjections.put(input.getSource(), keyProjection);
	}

	public void setValueProjection(int inputIndex, EvaluationExpression valueProjection) {
		this.setValueProjection(this.getInput(inputIndex), valueProjection);
	}

	public void setValueProjection(JsonStream input, EvaluationExpression valueProjection) {
		if (valueProjection == null)
			throw new NullPointerException("valueProjection must not be null");

		this.valueProjections.put(input.getSource(), valueProjection);
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
