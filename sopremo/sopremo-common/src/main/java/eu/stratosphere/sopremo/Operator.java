package eu.stratosphere.sopremo;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import eu.stratosphere.pact.common.plan.PactModule;

/**
 * Base class for all Sopremo operators. Every operator consumes and produces a specific number of {@link JsonStream}s.
 * The operator groups input json objects accordingly to its semantics and transforms the partitioned objects to one or
 * more outputs.<br>
 * Each Sopremo operator may be converted to a {@link PactModule} with the {@link #asPactModule(EvaluationContext)}
 * method.<br>
 * Implementations of an operator should either extend {@link ElementaryOperator} or {@link CompositeOperator}.
 * 
 * @author Arvid Heise
 */
public abstract class Operator implements SerializableSopremoType, JsonStream, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7808932536291658512L;

	private transient List<Operator.Output> inputs = new ArrayList<Operator.Output>();

	private String name;

	private transient Output[] outputs = new Output[0];

	/**
	 * Initializes the Operator with the given number of outputs and the given input {@link JsonStream}s. A JsonStream
	 * is either the output of another operator or the operator itself.
	 * 
	 * @param numberOfOutputs
	 *        the number of outputs
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	Operator(final int numberOfOutputs, final JsonStream... inputs) {
		this(1, Arrays.asList(inputs));
	}

	/**
	 * Initializes the Operator with the given number of outputs, and the given input {@link JsonStream}s. A JsonStream
	 * is either the output of another operator or the operator itself.
	 * 
	 * @param numberOfOutputs
	 *        the number of outputs
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	Operator(final int numberOfOutputs, final List<? extends JsonStream> inputs) {
		if (inputs == null)
			throw new NullPointerException();
		if (numberOfOutputs < 0)
			throw new IllegalArgumentException("numberOfOutputs < 0");

		for (final JsonStream input : inputs)
			this.inputs.add(input == null ? null : input.getSource());
		this.name = this.getClass().getSimpleName();
		this.setNumberOfOutputs(numberOfOutputs);
	}

	/**
	 * Initializes the Operator with the given input {@link JsonStream}s. A JsonStream is
	 * either the output of another operator or the operator itself. The number of outputs is set to 1.
	 * 
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	Operator(final JsonStream... inputs) {
		this(1, inputs);
	}

	/**
	 * Initializes the Operator with the given input {@link JsonStream}s. A JsonStream is
	 * either the output of another operator or the operator itself. The number of outputs is set to 1.
	 * 
	 * @param inputs
	 *        the input JsonStreams produces by other operators
	 */
	Operator(final List<? extends JsonStream> inputs) {
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
	
	public SopremoModule toElementaryOperators() {
		SopremoModule module = new SopremoModule(getName(), getInputs().size(), getOutputs().size());
		Operator clone = clone();
		for (int index = 0; index < getInputs().size(); index++) 
			clone.setInput(index, module.getInput(index));
		for (int index = 0; index < getOutputs().size(); index++)
			module.getOutput(index).setInput(index, clone. getOutput(index));
		return module;
	}

	@Override
	public Operator clone() {
		try {
			Operator clone = (Operator) super.clone();
			clone.inputs = new ArrayList<Operator.Output>(this.inputs);
			clone.setNumberOfOutputs(0);
			clone.setNumberOfOutputs(this.outputs.length);
			return clone;
		} catch (final CloneNotSupportedException e) {
			// cannot happen
			return null;
		}
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final Operator other = (Operator) obj;
		return this.name.equals(other.name);
	}

	/**
	 * Returns the output of an operator producing the {@link JsonStream} that is the input to this operator at the
	 * given position.
	 * 
	 * @param index
	 *        the index of the output
	 * @return the output that produces the input of this operator at the given position
	 */
	public Operator.Output getInput(final int index) {
		return this.inputs.get(index);
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
			public Operator get(final int index) {
				return Operator.this.inputs.get(index) == null ? null : Operator.this.inputs.get(index).getOperator();
			}

			@Override
			public int indexOf(final Object o) {
				final ListIterator<Output> e = Operator.this.inputs.listIterator();
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
		return new ArrayList<Operator.Output>(this.inputs);
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
	public Output getOutput(final int index) {
		return this.outputs[index];
	}

	/**
	 * Returns all outputs of this operator.
	 * 
	 * @return all outputs of this operator
	 */
	public List<Output> getOutputs() {
		return Arrays.asList(this.outputs);
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
	 * Replaces the input at the given location with the given {@link JsonStream}s.
	 * 
	 * @param index
	 *        the index of the input
	 * @param input
	 *        the new input
	 */
	public void setInput(final int index, final JsonStream input) {
		this.inputs.set(index, input == null ? null : input.getSource());
	}

	/**
	 * Replaces the current list of inputs with the given list of {@link JsonStream}s.
	 * 
	 * @param inputs
	 *        the new inputs
	 */
	public void setInputs(final JsonStream... inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");

		this.inputs.clear();
		for (final JsonStream input : inputs)
			this.inputs.add(input == null ? null : input.getSource());
	}

	/**
	 * Replaces the current list of inputs with the given list of {@link JsonStream}s.
	 * 
	 * @param inputs
	 *        the new inputs
	 */
	public void setInputs(final List<? extends JsonStream> inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");

		this.inputs.clear();
		for (final JsonStream input : inputs)
			this.inputs.add(input == null ? null : input.getSource());
	}

	/**
	 * Sets the name of this operator.
	 * 
	 * @param name
	 *        the new name of this operator
	 */
	public void setName(final String name) {
		if (name == null)
			throw new NullPointerException("name must not be null");

		this.name = name;
	}

	/**
	 * Sets the number of outputs of this operator retaining all old outputs if possible (increased number of outputs).
	 * 
	 * @param numberOfOutputs
	 *        the number of outputs
	 */
	protected final void setNumberOfOutputs(final int numberOfOutputs) {
		final Output[] outputs = new Output[numberOfOutputs];
		System.arraycopy(this.outputs, 0, outputs, 0, Math.min(numberOfOutputs, this.outputs.length));

		for (int index = this.outputs.length; index < numberOfOutputs; index++)
			outputs[index] = new Output(index);
		this.outputs = outputs;
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
	public class Output implements JsonStream, Cloneable {
		private final int index;

		private Output(final int index) {
			this.index = index;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			final Output other = (Output) obj;
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