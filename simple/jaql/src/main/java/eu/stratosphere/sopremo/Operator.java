package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.Collection;

public class Operator {
	private Collection<Operator> inputs;

	private Transformation transformation;

	private Condition condition;

	private Partition partition;

	private String name;

	public Operator(String name, Partition partition, Transformation transformation, Condition condition,
			Operator... inputs) {
		this(name, partition, transformation, condition, Arrays.asList(inputs));
	}

	protected Operator(Partition partition, Transformation transformation, Condition condition,
			Operator... inputs) {
		this(null, partition, transformation, condition, inputs);
	}

	protected Operator(Partition partition, Transformation transformation, Condition condition,
			Collection<Operator> inputs) {
		this(null, partition, transformation, condition, inputs);
	}

	public Operator(String name, Partition partition, Transformation transformation, Condition condition,
			Collection<Operator> inputs) {
		this.inputs = inputs;
		this.name = name == null ? getClass().getSimpleName() : name;
		this.transformation = transformation;
		this.condition = condition;
		this.partition = partition;
	}

	public Collection<Operator> getInputs() {
		return inputs;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(name);
		if (condition != null)
			builder.append(", ").append(condition);
		if (transformation != null)
			builder.append(", ").append(transformation);
		return builder.toString();
	}

}