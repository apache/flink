package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonNode;

/**
 * Returns an aggregate of the elements of the given {@link IArrayNode}.
 * The result is calculated with help of the specified {@link AggregationExpression}.
 */
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
	public IJsonNode evaluate(final IJsonNode nodes, IJsonNode target, final EvaluationContext context) {
		this.function.initialize();
		for (final IJsonNode node : (IArrayNode) nodes)
			this.function.aggregate(this.preprocessing.evaluate(node, null, context), context);
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
		int result = super.hashCode();
		result = prime * result + (this.function == null ? 0 : this.function.hashCode());
		result = prime * result + (this.preprocessing == null ? 0 : this.preprocessing.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final AggregationExpression other = (AggregationExpression) obj;
		return this.function.equals(other.function) && this.preprocessing.equals(other.preprocessing);
	}

	@Override
	public void toString(final StringBuilder builder) {
		super.toString(builder);
		builder.append('.');
		this.function.toString(builder);
		builder.append('(');
		if (this.preprocessing != EvaluationExpression.VALUE)
			builder.append(this.preprocessing);
		builder.append(')');
	}
}
