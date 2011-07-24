package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;

public class AggregationExpression extends EvaluationExpression {
	private AggregationFunction function;

	private EvaluationExpression preprocessing;

	public AggregationExpression(AggregationFunction function, EvaluationExpression preprocessing) {
		this.function = function.clone();
		this.preprocessing = preprocessing;
	}
	
	public AggregationExpression(AggregationFunction function) {
		this(function, EvaluationExpression.SAME_VALUE);
	}

	public AggregationFunction getFunction() {
		return function;
	}

	public EvaluationExpression getPreprocessing() {
		return preprocessing;
	}

	@Override
	public JsonNode evaluate(JsonNode nodes, EvaluationContext context) {
		function.initialize();
		for (JsonNode node : nodes)
			function.aggregate(preprocessing.evaluate(node, context), context);
		return function.getFinalAggregate();
	}

	@Override
	protected void toString(StringBuilder builder) {
		super.toString(builder);
		builder.append('.');
		function.toString(builder);
		builder.append('(');
		if (preprocessing != EvaluationExpression.SAME_VALUE)
			builder.append(preprocessing);
		builder.append(')');
	}
}
