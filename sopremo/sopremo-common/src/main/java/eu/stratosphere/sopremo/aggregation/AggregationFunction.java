package eu.stratosphere.sopremo.aggregation;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class AggregationFunction implements SerializableSopremoType, Cloneable {
	private String name;

	public AggregationFunction(String name) {
		this.name = name;
	}

	@Override
	public AggregationFunction clone() {
		try {
			return (AggregationFunction) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new IllegalStateException("should not happen", e);
		}
	}

	public void initialize() {
	}

	public abstract void aggregate(JsonNode node, EvaluationContext context);

	public abstract JsonNode getFinalAggregate();

	public void toString(StringBuilder builder) {
		builder.append(name);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		toString(builder);
		return builder.toString();
	}

	public AggregationExpression asExpression() {
		return new AggregationExpression(this);
	}
}
