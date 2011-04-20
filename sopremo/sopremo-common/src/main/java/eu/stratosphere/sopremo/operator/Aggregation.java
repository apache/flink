package eu.stratosphere.sopremo.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.JsonPath;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Partition;
import eu.stratosphere.sopremo.Transformation;

public class Aggregation extends Operator {
	public final static List<JsonPath> NO_GROUPING = new ArrayList<JsonPath>();

	private List<JsonPath> groupings;

	public Aggregation(Transformation transformation, List<JsonPath> grouping, Operator... inputs) {
		super(transformation, inputs);
		if (grouping == null)
			throw new NullPointerException();
		this.groupings = grouping;
	}

	public Aggregation(Transformation transformation, List<JsonPath> grouping, List<Operator> inputs) {
		super(transformation, inputs);
		if (grouping == null)
			throw new NullPointerException();
		this.groupings = grouping;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		if (this.groupings != null)
			builder.append(" on ").append(this.groupings);
		if (this.getTransformation() != Transformation.IDENTITY)
			builder.append(" to ").append(this.getTransformation());
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 67;
		int result = super.hashCode();
		result = prime * result + groupings.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Aggregation other = (Aggregation) obj;
		if (!groupings.equals(other.groupings))
			return false;

		for (int index = 0; index < groupings.size(); index++)
			if (!groupings.get(index).deepEquals(other.groupings.get(index)))
				return false;
		return true;
	}

}
