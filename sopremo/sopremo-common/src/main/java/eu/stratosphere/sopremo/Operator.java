package eu.stratosphere.sopremo;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;

/**
 * Base class for all Sopremo operators. Every operator consumes and produces a specific number of {@link JsonStream}s.
 * The operator groups input json objects accordingly to its semantics and transforms the partitioned objects to one or
 * more outputs with an {@link Evaluable} transformation.<br>
 * Each Sopremo operator may be converted to a {@link PactModule} with the {@link #asPactModule(EvaluationContext)}
 * method.
 * 
 * @author Arvid Heise
 */
public abstract class Operator implements SerializableSopremoType, JsonStream, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7808932536291658512L;

	private List<Operator.Output> inputs = new ArrayList<Operator.Output>();

	private String name;

	private Output[] outputs;

	/**
	 * Initializes the Operator with the given transformation, the number of outputs, and the given input
	 * {@link JsonStream}. A JsonStream is either the output of another operator or the operator itself.
	 * 
	 * @param transformation
	 *        the transformation that is applied to a partition of input tuples or
	 *        {@link EvaluableExpression#SAME_VALUE} if no transformation is desired
	 * @param numberOfOutputs
	 *        the number of outputs
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	protected Operator(int numberOfOutputs, JsonStream... inputs) {
		this(1, Arrays.asList(inputs));
	}

	/**
	 * Initializes the Operator with the given transformation, the number of outputs, and the given input
	 * {@link JsonStream}. A JsonStream is either the output of another operator or the operator itself.
	 * 
	 * @param transformation
	 *        the transformation that is applied to a partition of input tuples or
	 *        {@link EvaluableExpression#SAME_VALUE} if no transformation is desired
	 * @param numberOfOutputs
	 *        the number of outputs
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	protected Operator(int numberOfOutputs, List<? extends JsonStream> inputs) {
		if (inputs == null)
			throw new NullPointerException();
		if (numberOfOutputs < 0)
			throw new IllegalArgumentException("numberOfOutputs < 0");

		for (JsonStream input : inputs)
			this.inputs.add(input == null ? null : input.getSource());
		this.name = this.getClass().getSimpleName();
		this.outputs = new Output[numberOfOutputs];
		for (int index = 0; index < numberOfOutputs; index++)
			this.outputs[index] = new Output(index);
	}

	/**
	 * Initializes the Operator with the given transformation, and the given input {@link JsonStream}. A JsonStream is
	 * either the output of another operator or the operator itself. The number of outputs is set to 1.
	 * 
	 * @param transformation
	 *        the transformation that is applied to a partition of input tuples or
	 *        {@link EvaluableExpression#SAME_VALUE} if no transformation is desired
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	protected Operator(JsonStream... inputs) {
		this(1, inputs);
	}

	/**
	 * Initializes the Operator with the given transformation, and the given input {@link JsonStream}. A JsonStream is
	 * either the output of another operator or the operator itself. The number of outputs is set to 1.
	 * 
	 * @param transformation
	 *        the transformation that is applied to a partition of input tuples or
	 *        {@link EvaluableExpression#SAME_VALUE} if no transformation is desired
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	protected Operator(List<? extends JsonStream> inputs) {
		this(1, inputs);
	}

	/**
	 * Converts this operator to a {@link PactModule} using the provided {@link EvaluationContext}.
	 * 
	 * @param context
	 *        the context in which the evaluation should be conducted
	 * @return the {@link PactModule} representing this operator
	 */
	public abstract PactModule asPactModule(EvaluationContext context);

	//
	// public SopremoModule asElementaryOperators() {
	// SopremoModule module = new SopremoModule(getName(), inputs.size(), outputs.length);
	// Operator clone = this.clone();
	// clone.setInputs(module.getInputs());
	// Sink[] outputs = module.getOutputs();
	// for (int index = 0; index < outputs.length; index++)
	// outputs[index].setInput(index, clone.getOutput(index));
	// return module;
	// }

	// protected static class PactImplementation extends Operator {
	// /**
	// *
	// */
	// private static final long serialVersionUID = 6207171377820045505L;
	//
	// private PactModule implementation;
	//
	// public PactImplementation(String name, PactModule implementation, JsonStream... inputs) {
	// super(implementation.getOutputs().length, inputs);
	// setName(name);
	// this.implementation = implementation;
	// }
	//
	// public PactImplementation(String name, PactModule implementation,
	// List<? extends JsonStream> inputs) {
	// super(implementation.getOutputs().length, inputs);
	// setName(name);
	// this.implementation = implementation;
	// }
	//
	// @Override
	// public PactModule asPactModule(EvaluationContext context) {
	// return implementation;
	// }
	//
	// }

	@Override
	public Operator clone() {
		try {
			return (Operator) super.clone();
		} catch (CloneNotSupportedException e) {
			// cannot happen
			return null;
		}
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
		return this.name.equals(other.name);
	}

	/**
	 * Returns a list of operators producing the {@link JsonStream}s that are the inputs to this operator.<br>
	 * If multiple outputs of an operator are used as inputs for this operator, the operator appears several times.
	 * 
	 * @return a list of operators that produce the input of this operator
	 */
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

	/**
	 * Returns a list of outputs of operators producing the {@link JsonStream}s that are the inputs to this operator.<br>
	 * If an output is used multiple times as inputs for this operator, the output appears several times (for example in
	 * a self-join).
	 * 
	 * @return a list of outputs that produce the input of this operator
	 */
	public List<Operator.Output> getInputs() {
		return this.inputs;
	}

	/**
	 * Returns the output of an operator producing the {@link JsonStream} that is the input to this operator at the
	 * given position.
	 * 
	 * @param index
	 *        the index of the output
	 * @return the output that produces the input of this operator at the given position
	 */
	public Operator.Output getInput(int index) {
		return this.inputs.get(index);
	}

	/**
	 * The name of this operator, which is the class name by default.
	 * 
	 * @return the name of this operator.
	 * @see #setName(String)
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Returns the output at the specified index.
	 * 
	 * @param index
	 *        the index to lookup
	 * @return the output at the given position
	 */
	public Output getOutput(int index) {
		return this.outputs[index];
	}

	/**
	 * Returns the first output of this operator.
	 */
	@Override
	public Output getSource() {
		return this.getOutput(0);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.name.hashCode();
		return result;
	}

	/**
	 * Replaces the current list of inputs with the given list of {@link JsonStream}s.
	 * 
	 * @param inputs
	 *        the new inputs
	 */
	public void setInputs(JsonStream... inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");

		this.inputs.clear();
		for (JsonStream input : inputs)
			this.inputs.add(input.getSource());
	}

	/**
	 * Replaces the input at the given location with the given {@link JsonStream}s.
	 * 
	 * @param index
	 *        the index of the input
	 * @param input
	 *        the new input
	 */
	public void setInput(int index, JsonStream input) {
		if (input == null)
			throw new NullPointerException("input must not be null");

		this.inputs.set(index, input.getSource());
	}

	/**
	 * Replaces the current list of inputs with the given list of {@link JsonStream}s.
	 * 
	 * @param inputs
	 *        the new inputs
	 */
	public void setInputs(List<? extends JsonStream> inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");

		this.inputs.clear();
		for (JsonStream input : inputs)
			this.inputs.add(input.getSource());
	}

	/**
	 * Sets the name of this operator.
	 * 
	 * @param name
	 *        the new name of this operator
	 */
	public void setName(String name) {
		if (name == null)
			throw new NullPointerException("name must not be null");

		this.name = name;
	}

	@Override
	public String toString() {
		return this.getName();
	}

	/**
	 * Represents one output of this {@link Operator}. The output should be connected to another Operator to create a
	 * directed acyclic graph of Operators.
	 * 
	 * @author Arvid Heise
	 */
	public class Output implements JsonStream {
		private int index;

		private Output(int index) {
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

		/**
		 * Returns the index of this output in the list of outputs of the associated operator.
		 * 
		 * @return the index of this output
		 */
		public int getIndex() {
			return this.index;
		}

		/**
		 * Returns the associated operator.
		 * 
		 * @return the associated operator
		 */
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
			return String.format("%s@%d", this.getOperator(), this.index);
		}
	}

}