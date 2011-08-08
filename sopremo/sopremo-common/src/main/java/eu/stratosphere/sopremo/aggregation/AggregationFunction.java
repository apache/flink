package eu.stratosphere.sopremo.aggregation;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.expressions.AggregationExpression;

public abstract class AggregationFunction implements SerializableSopremoType, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5701471344038419637L;

	private final String name;

	public AggregationFunction(final String name) {
		this.name = name;
	}

	public abstract void aggregate(JsonNode node, EvaluationContext context);

	public AggregationExpression asExpression() {
		return new AggregationExpression(this);
	}

	@Override
	public AggregationFunction clone() {
		try {
			return (AggregationFunction) super.clone();
		} catch (final CloneNotSupportedException e) {
			throw new IllegalStateException("should not happen", e);
		}
	}

	public abstract JsonNode getFinalAggregate();

	public void initialize() {
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}

	public void toString(final StringBuilder builder) {
		builder.append(this.name);
	}
}
