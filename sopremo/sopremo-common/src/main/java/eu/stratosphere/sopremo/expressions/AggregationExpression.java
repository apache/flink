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
		this(function, EvaluationExpression.VALUE);
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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((function == null) ? 0 : function.hashCode());
		result = prime * result + ((preprocessing == null) ? 0 : preprocessing.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AggregationExpression other = (AggregationExpression) obj;
		if (function == null) {
			if (other.function != null)
				return false;
		} else if (!function.equals(other.function))
			return false;
		if (preprocessing == null) {
			if (other.preprocessing != null)
				return false;
		} else if (!preprocessing.equals(other.preprocessing))
			return false;
		return true;
	}

	@Override
	protected void toString(final StringBuilder builder) {
		super.toString(builder);
		builder.append('.');
		this.function.toString(builder);
		builder.append('(');
		if (this.preprocessing != EvaluationExpression.VALUE)
			builder.append(this.preprocessing);
		builder.append(')');
	}
}
