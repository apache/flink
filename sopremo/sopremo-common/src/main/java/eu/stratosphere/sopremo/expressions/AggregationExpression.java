package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Returns an aggregate of the elements of a {@link IArrayNode}.
 * The result is calculated with help of the specified {@link AggregationExpression}.
 */
public class AggregationExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1420818869290609780L;

	private final AggregationFunction function;

	private CachingExpression<IJsonNode> preprocessing;

	private transient IJsonNode aggregator;

	/**
	 * Initializes an AggregationExpression with the given {@link AggregationFunction}.
	 * 
	 * @param function
	 *        the function which will should be used for aggregation
	 */
	public AggregationExpression(final AggregationFunction function) {
		this(function, EvaluationExpression.VALUE);
	}

	/**
	 * Initializes an AggregationExpression with the given {@link AggregationFunction} and an additional preprocessing.
	 * 
	 * @param function
	 *        the function which will should be used for aggregation
	 * @param preprocessing
	 *        an {@link EvaluationExpression} which evaluates each element of the input before they are used for
	 *        aggregation.
	 */
	public AggregationExpression(final AggregationFunction function, final EvaluationExpression preprocessing) {
		this.function = function.clone();
		this.preprocessing = CachingExpression.ofSubclass(preprocessing, IJsonNode.class);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode nodes, IJsonNode target, final EvaluationContext context) {
		this.aggregator = this.function.initialize(this.aggregator);
		for (final IJsonNode node : (IArrayNode) nodes)
			this.aggregator =
				this.function.aggregate(this.preprocessing.evaluate(node, context), this.aggregator, context);
		return this.function.getFinalAggregate(this.aggregator, target);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions
	 * .TransformFunction)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public EvaluationExpression transformRecursively(TransformFunction function) {
		this.preprocessing = (CachingExpression<IJsonNode>) this.preprocessing.transformRecursively(function);
		return function.call(this);
	}

	/**
	 * Returns the function.
	 * 
	 * @return the function
	 */
	public AggregationFunction getFunction() {
		return this.function;
	}

	/**
	 * Returns the preprocessing.
	 * 
	 * @return the preprocessing
	 */
	public EvaluationExpression getPreprocessing() {
		return this.preprocessing.getInnerExpression();
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
			this.preprocessing.toString(builder);
		// builder.append(this.preprocessing);
		builder.append(')');
	}
}
