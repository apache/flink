package eu.stratosphere.sopremo.cleansing.conflict_resolution;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;

public class FilterRecordResolution extends AggregationFunction {
	public FilterRecordResolution(String name) {
		super("filter");
	}

	@Override
	public void aggregate(JsonNode node, EvaluationContext context) {
	}

	@Override
	public void initialize() {
		throw new UnresolvableEvalatuationException();
	}

	@Override
	public JsonNode getFinalAggregate() {
		return null;
	}
}
