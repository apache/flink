package eu.stratosphere.sopremo.aggregation;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class AggregationFunction implements SerializableSopremoType, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5701471344038419637L;

	private final String name;

	public AggregationFunction(final String name) {
		this.name = name;
	}

	public abstract void aggregate(IJsonNode node, EvaluationContext context);

	public AggregationExpression asExpression() {
		return new AggregationExpression(this);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.name == null ? 0 : this.name.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final AggregationFunction other = (AggregationFunction) obj;
		if (this.name == null) {
			if (other.name != null)
				return false;
		} else if (!this.name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public AggregationFunction clone() {
		try {
			return (AggregationFunction) super.clone();
		} catch (final CloneNotSupportedException e) {
			throw new IllegalStateException("should not happen", e);
		}
	}

	public abstract IJsonNode getFinalAggregate();

	public void initialize() {
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(this.name);
	}
}
