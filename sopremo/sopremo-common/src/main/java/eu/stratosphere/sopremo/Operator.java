package eu.stratosphere.sopremo;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;

public abstract class Operator implements SerializableSopremoType, JsonStream {
	private List<Operator.Output> inputs = new ArrayList<Operator.Output>();

	private Evaluable transformation;

	private String name;

	private Output[] outputs;

	protected Operator(Evaluable transformation, int numberOfOutputs, JsonStream... inputs) {
		this(transformation, 1, Arrays.asList(inputs));
	}

	protected Operator(Evaluable transformation, int numberOfOutputs, List<? extends JsonStream> inputs) {
		if (transformation == null || inputs == null)
			throw new NullPointerException();
		for (JsonStream input : inputs)
			this.inputs.add(input == null ? null : input.getSource());
		this.name = this.getClass().getSimpleName();
		this.transformation = transformation;
		this.outputs = new Output[numberOfOutputs];
		for (int index = 0; index < numberOfOutputs; index++)
			this.outputs[index] = new Output(index);
	}

	protected Operator(Evaluable transformation, JsonStream... inputs) {
		this(transformation, 1, inputs);
	}

	protected Operator(Evaluable transformation, List<? extends JsonStream> inputs) {
		this(transformation, 1, inputs);
	}

	public abstract PactModule asPactModule(EvaluationContext context);

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

	public Evaluable getEvaluableExpression() {
		return this.transformation;
	}

	public List<Operator.Output> getInputIndex() {
		return this.inputs;
	}

	public List<Operator> getInputOperators() {
		return new AbstractList<Operator>() {

			@Override
			public Operator get(int index) {
				return Operator.this.inputs.get(index) == null ? null : Operator.this.inputs.get(index).getOperator();
			}

			@Override
			public int indexOf(Object o) {
				ListIterator<Output> e = Operator.this.inputs.listIterator();
				while (e.hasNext())
					if (o == e.next())
						return e.previousIndex();
				return -1;
			}

			@Override
			public int size() {
				return Operator.this.inputs.size();
			}
		};
	}

	public List<Operator.Output> getInputs() {
		return this.inputs;
	}

	public String getName() {
		return this.name;
	}

	public Output getOutput(int index) {
		return this.outputs[index];
	}

	@Override
	public Output getSource() {
		return getOutput(0);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.name.hashCode();
		result = prime * result + this.transformation.hashCode();
		return result;
	}

	public void setEvaluableExpression(Evaluable transformation) {
		if (transformation == null)
			throw new NullPointerException("transformation must not be null");

		this.transformation = transformation;
	}

	public void setInputOperators(List<Operator> inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");

		this.inputs.clear();
		for (Operator operator : inputs)
			this.inputs.add(operator.getOutput(0));
	}

	public void setInputOperators(Operator... inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");

		this.inputs.clear();
		for (Operator operator : inputs)
			this.inputs.add(operator.getOutput(0));
	}

	public void setInputs(List<Operator.Output> inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");

		this.inputs = inputs;
	}

	public void setName(String name) {
		if (name == null)
			throw new NullPointerException("name must not be null");

		this.name = name;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		if (this.getEvaluableExpression() != EvaluableExpression.IDENTITY)
			builder.append(" to ").append(this.getEvaluableExpression());
		return builder.toString();
	}

	public class Output implements JsonStream {
		private int index;

		public Output(int index) {
			this.index = index;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			Output other = (Output) obj;
			return this.index == other.index && this.getOperator() == other.getOperator();
		}

		public int getIndex() {
			return this.index;
		}

		public Operator getOperator() {
			return Operator.this;
		}

		@Override
		public Output getSource() {
			return this;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + this.index;
			result = prime * result + this.getOperator().hashCode();
			return result;
		}

		@Override
		public String toString() {
			return String.format("%s@%d", this.getOperator(), this.index + 1);
		}
	}

}