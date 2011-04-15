package eu.stratosphere.sopremo;

import java.util.Arrays;
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
		if (transformation == null || inputs == null)
			throw new NullPointerException();
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