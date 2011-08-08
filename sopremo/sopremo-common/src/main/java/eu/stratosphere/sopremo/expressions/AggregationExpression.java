package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;

public class AggregationExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1420818869290609780L;

	private final AggregationFunction function;

	private final EvaluationExpression preprocessing;

	public AggregationExpression(final AggregationFunction function) {
		this(function, EvaluationExpression.SAME_VALUE);
	}

	public AggregationExpression(final AggregationFunction function, final EvaluationExpression preprocessing) {
		this.function = function.clone();
		this.preprocessing = preprocessing;
	}

	@Override
	public JsonNode evaluate(final JsonNode nodes, final EvaluationContext context) {
		this.function.initialize();
		for (final JsonNode node : nodes)
			this.function.aggregate(this.preprocessing.evaluate(node, context), context);
		return this.function.getFinalAggregate();
	}

	public AggregationFunction getFunction() {
		return this.function;
	}

	public EvaluationExpression getPreprocessing() {
		return this.preprocessing;
	}

	@Override
	protected void toString(final StringBuilder builder) {
		super.toString(builder);
		builder.append('.');
		this.function.toString(builder);
		builder.append('(');
		if (this.preprocessing != EvaluationExpression.SAME_VALUE)
			builder.append(this.preprocessing);
		builder.append(')');
	}
}
