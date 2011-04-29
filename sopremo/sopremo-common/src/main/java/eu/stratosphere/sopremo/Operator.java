package eu.stratosphere.sopremo;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.expressions.Transformation;

public abstract class Operator implements SopremoType {
	public class Output {
		private int index;

		public Output(int index) {
			this.index = index;
		}

		public Operator getOperator() {
			return Operator.this;
		}

		public int getIndex() {
			return index;
		}

		@Override
		public String toString() {
			return String.format("%s@%d", getOperator(), index + 1);
		}
	}

	private List<Operator.Output> inputs;

	private Transformation transformation;

	private String name;

	private Output[] outputs;

	public Operator(String name, Transformation transformation,
			Operator... inputs) {
		this(name, transformation, Arrays.asList(inputs));
	}

	protected Operator(Transformation transformation,
			Operator... inputs) {
		this(null, transformation, inputs);
	}

	protected Operator(Transformation transformation,
			List<Operator> inputs) {
		this(null, transformation, inputs);
	}

	public Operator(String name, Transformation transformation,
			List<Operator> inputs) {
		if (transformation == null || inputs == null)
			throw new NullPointerException();
		this.inputs = new ArrayList<Operator.Output>();
		for (Operator operator : inputs)
			this.inputs.add(operator == null ? null : operator.getOutput(0));
		this.name = name == null ? this.getClass().getSimpleName() : name;
		this.transformation = transformation;
		this.outputs = new Output[] { new Output(0) };
	}

	public Output getOutput(int index) {
		return outputs[index];
	}

	public Operator(String name, int numberOfOutputs, Transformation transformation,
			Operator.Output... inputs) {
		this(name, numberOfOutputs, transformation, Arrays.asList(inputs));
	}

	protected Operator(Transformation transformation, int numberOfOutputs,
			Operator.Output... inputs) {
		this(null, 1, transformation, inputs);
	}

	protected Operator(Transformation transformation, int numberOfOutputs,
			List<Operator.Output> inputs) {
		this(null, numberOfOutputs, transformation, inputs);
	}

	public Operator(String name, int numberOfOutputs, Transformation transformation,
			List<Operator.Output> inputs) {
		if (transformation == null || inputs == null)
			throw new NullPointerException();
		this.inputs = inputs;
		this.name = name == null ? this.getClass().getSimpleName() : name;
		this.transformation = transformation;
		this.outputs = new Output[numberOfOutputs];
		for (int index = 0; index < numberOfOutputs; index++)
			this.outputs[index] = new Output(index);
	}

	public List<Operator> getInputOperators() {
		return new AbstractList<Operator>() {

			@Override
			public Operator get(int index) {
				return inputs.get(index) == null ? null : inputs.get(index).getOperator();
			}

			@Override
			public int size() {
				return inputs.size();
			}
		};
	}

	public void setInputs(List<Operator.Output> inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");

		this.inputs = inputs;
	}

	public void setInputOperators(List<Operator> inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");

		this.inputs.clear();
		for (Operator operator : inputs)
			this.inputs.add(operator.getOutput(0));
	}

	public List<Operator.Output> getInputs() {
		return inputs;
	}

	public Transformation getTransformation() {
		return this.transformation;
	}

	public void setTransformation(Transformation transformation) {
		if (transformation == null)
			throw new NullPointerException("transformation must not be null");

		this.transformation = transformation;
	}

	public abstract PactModule asPactModule();

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		if (name == null)
			throw new NullPointerException("name must not be null");

		this.name = name;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		if (this.getTransformation() != Transformation.IDENTITY)
			builder.append(" to ").append(this.getTransformation());
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.name.hashCode();
		result = prime * result + this.transformation.hashCode();
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
		Operator other = (Operator) obj;
		return this.name.equals(other.name) && this.transformation.equals(other.transformation);
	}

}