package eu.stratosphere.sopremo.cleansing.conflict_resolution;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.aggregation.TransitiveAggregationFunction;

public class DefaultValueResolution extends TransitiveAggregationFunction {
	@Override
	protected JsonNode aggregate(JsonNode aggregate, JsonNode node, EvaluationContext context) {
		return aggregate;
	}

	public DefaultValueResolution(JsonNode defaultValue) {
		super("default value", defaultValue);
	}
}
