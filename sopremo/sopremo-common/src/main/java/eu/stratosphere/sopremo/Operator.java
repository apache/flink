package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class Operator {
	private List<Operator> inputs;

	private Transformation transformation;

	private String name;

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
		this.inputs = inputs;
		this.name = name == null ? this.getClass().getSimpleName() : name;
		this.transformation = transformation;
	}

	public List<Operator> getInputs() {
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

}