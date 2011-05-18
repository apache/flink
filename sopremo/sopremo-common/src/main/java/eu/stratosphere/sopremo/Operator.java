package eu.stratosphere.sopremo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.operator.SopremoMap;

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
			return this.index;
		}

		@Override
		public String toString() {
			return String.format("%s@%d", this.getOperator(), this.index + 1);
		}
	}
	private List<Operator.Output> inputs;

	private Evaluable transformation;

	private String name;

	private Output[] outputs;

	public Operator(String name, Evaluable transformation,
			Operator... inputs) {
		this(name, transformation, Arrays.asList(inputs));
	}

	protected Operator(Evaluable transformation,
			Operator... inputs) {
		this(null, transformation, inputs);
	}

	protected Operator(Evaluable transformation,
			List<Operator> inputs) {
		this(null, transformation, inputs);
	}

	public Operator(String name, Evaluable transformation,
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
		return this.outputs[index];
	}

	public Operator(String name, int numberOfOutputs, Evaluable transformation,
			Operator.Output... inputs) {
		this(name, numberOfOutputs, transformation, Arrays.asList(inputs));
	}

	protected Operator(Evaluable transformation, int numberOfOutputs,
			Operator.Output... inputs) {
		this(null, 1, transformation, inputs);
	}

	protected Operator(Evaluable transformation, int numberOfOutputs,
			List<Operator.Output> inputs) {
		this(null, numberOfOutputs, transformation, inputs);
	}

	public Operator(String name, int numberOfOutputs, Evaluable transformation,
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
				return Operator.this.inputs.get(index) == null ? null : Operator.this.inputs.get(index).getOperator();
			}

			@Override
			public int size() {
				return Operator.this.inputs.size();
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

	public void setInputOperators(Operator... inputs) {
		if (inputs == null)
			throw new NullPointerException("inputs must not be null");

		this.inputs.clear();
		for (Operator operator : inputs)
			this.inputs.add(operator.getOutput(0));
	}

	public List<Operator.Output> getInputs() {
		return this.inputs;
	}

	public Evaluable getEvaluableExpression() {
		return this.transformation;
	}

	public void setEvaluableExpression(Evaluable transformation) {
		if (transformation == null)
			throw new NullPointerException("transformation must not be null");

		this.transformation = transformation;
	}

	public abstract PactModule asPactModule(EvaluationContext context);

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
		if (this.getEvaluableExpression() != EvaluableExpression.IDENTITY)
			builder.append(" to ").append(this.getEvaluableExpression());
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