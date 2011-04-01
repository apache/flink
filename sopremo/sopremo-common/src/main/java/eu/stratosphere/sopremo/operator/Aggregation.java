package eu.stratosphere.sopremo.operator;

import java.util.Collection;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.JsonPath;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Partition;
import eu.stratosphere.sopremo.Transformation;

public class Aggregation extends Operator {
	private JsonPath grouping;

	public Aggregation(Transformation transformation, JsonPath grouping, Operator input) {
		super(transformation, input);
		this.grouping = grouping;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		if (this.grouping != null)
			builder.append(" on ").append(this.grouping);
		if (this.getTransformation() != Transformation.IDENTITY)
			builder.append(" to ").append(this.getTransformation());
		return builder.toString();
	}
}
