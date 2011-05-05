package eu.stratosphere.sopremo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.expressions.AbstractIterator;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.Path;
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
			return this.index;
		}

		@Override
		public String toString() {
			return String.format("%s@%d", this.getOperator(), this.index + 1);
		}
	}

	public static class KeyExtractionStub extends
			MapStub<PactNull, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
		private EvaluableExpression evaluableExpression;

		@Override
		public void configure(Configuration parameters) {
			this.evaluableExpression = getObject(parameters, "extraction", EvaluableExpression.class);
		}

		@Override
		public void map(PactNull key, PactJsonObject value, Collector<PactJsonObject.Key, PactJsonObject> out) {
			out.collect(PactJsonObject.keyOf(this.evaluableExpression.evaluate(value.getValue())), value);
		}
	}

	protected static class UnwrappingIterator extends AbstractIterator<JsonNode> {
		private final Iterator<PactJsonObject> values;

		public UnwrappingIterator(Iterator<PactJsonObject> values) {
			this.values = values;
		}

		@Override
		protected JsonNode loadNext() {
			if (!values.hasNext())
				return noMoreElements();
			return values.next().getValue();
		}
	}

	protected Contract addKeyExtraction(PactModule module, Path expr) {
		MapContract<PactNull, PactJsonObject, PactJsonObject.Key, PactJsonObject> selectionMap =
			new MapContract<PactNull, PactJsonObject, PactJsonObject.Key, PactJsonObject>(KeyExtractionStub.class);
		selectionMap.setInput(module.getInput(this.getInputIndex(expr)));
		setObject(selectionMap.getStubParameters(), "extraction",
			Path.replace(expr, new Path(expr.getFragment(0)), new Path(new Input(0))));

		return selectionMap;
	}

	protected int getInputIndex(Path expr) {
		return ((Input) expr.getFragment(0)).getIndex();
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
		return this.outputs[index];
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

	protected static void setTransformation(Configuration config, String key, Transformation transformation) {
		config.setString(key, objectToString(transformation));
	}

	protected static Transformation getTransformation(Configuration config, String key) {
		String string = config.getString(key, null);
		if (string == null)
			return null;
		return (Transformation) stringToObject(string);
	}

	protected static Object stringToObject(String string) {
		Object object = null;
		try {
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(string
				.getBytes())));
			object = in.readObject();
			in.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return object;
	}

	protected static String objectToString(Object transformation) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream out = new ObjectOutputStream(bos);
			out.writeObject(transformation);
			out.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		String string = new String(Base64.encodeBase64(bos.toByteArray()));
		return string;
	}

	protected static void setObject(Configuration config, String key, Object object) {
		config.setString(key, objectToString(object));
	}

	@SuppressWarnings("unchecked")
	protected static <T> T getObject(Configuration config, String key, Class<T> objectClass) {
		String string = config.getString(key, null);
		if (string == null)
			return null;
		return (T) stringToObject(string);
	}

}