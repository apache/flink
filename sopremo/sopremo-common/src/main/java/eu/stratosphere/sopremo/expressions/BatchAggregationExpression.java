package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Batch aggregates one stream of {@link IJsonNode} with several {@link AggregationFunction}s.
 * 
 * @author Arvid Heise
 */
public class BatchAggregationExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 39927024374509247L;

	private final List<Partial> partials;

	private transient IJsonNode lastResult;

	private transient int lastInputCounter = Integer.MIN_VALUE;

	/**
	 * Initializes a BatchAggregationExpression with the given {@link AggregationFunction}s.
	 * 
	 * @param functions
	 *        all functions that should be used
	 */
	public BatchAggregationExpression(final AggregationFunction... functions) {
		this(Arrays.asList(functions));
	}

	/**
	 * Initializes a BatchAggregationExpression with the given {@link AggregationFunction}s.
	 * 
	 * @param functions
	 *        a set of all functions that should be used
	 */
	public BatchAggregationExpression(final List<AggregationFunction> functions) {
		this.partials = new ArrayList<Partial>(functions.size());
		for (final AggregationFunction function : functions)
			this.partials.add(new Partial(function, EvaluationExpression.VALUE, this.partials.size()));
		this.expectedTarget = ArrayNode.class;
	}

	/**
	 * Adds a new {@link AggregationFunction}.
	 * 
	 * @param function
	 *        the function that should be added
	 * @return the function which has been added as a {@link Partial}
	 */
	public EvaluationExpression add(final AggregationFunction function) {
		return this.add(function, EvaluationExpression.VALUE);
	}

	/**
	 * Adds a new {@link AggregationFunction} with the given preprocessing.
	 * 
	 * @param function
	 *        the function that should be added
	 * @param preprocessing
	 *        the preprocessing that should be used for this function
	 * @return the function which has been added as a {@link Partial}
	 */
	public EvaluationExpression add(final AggregationFunction function, final EvaluationExpression preprocessing) {
		final Partial partial = new Partial(function, preprocessing, this.partials.size());
		this.partials.add(partial);
		return partial;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions.TransformFunction)
	 */
	@Override
	public EvaluationExpression transformRecursively(TransformFunction function) {
		// partials are transformed separately, where appropriate
		return function.call(this);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		if (this.lastInputCounter == context.getInputCounter())
			return this.lastResult;
		target = SopremoUtil.reuseTarget(target, this.expectedTarget);

		this.lastInputCounter = context.getInputCounter();

		for (final Partial partial : this.partials)
			partial.getFunction().initialize();
		for (final IJsonNode input : (ArrayNode) node)
			for (final Partial partial : this.partials)
				partial.getFunction().aggregate(partial.getPreprocessing().evaluate(input, null, context), context);

		final IJsonNode[] results = new IJsonNode[this.partials.size()];
		for (int index = 0; index < results.length; index++)
			results[index] = this.partials.get(index).getFunction().getFinalAggregate();

		return this.lastResult = ((IArrayNode) target).addAll(results);
	}

	private class Partial extends AggregationExpression {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7597385785040647923L;

		private final int index;

		/**
		 * Initializes a Partial with the given function, preprocessing and index.
		 * 
		 * @param function
		 *        an {@link AggregationFunction} that should be used by this Partial
		 * @param preprocessing
		 *        the preprocessing that should be used by this Partial
		 * @param index
		 *        the index of this Partial
		 */
		public Partial(final AggregationFunction function, final EvaluationExpression preprocessing, final int index) {
			super(function, preprocessing);
			this.index = index;
		}

		@Override
		public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
			return ((IArrayNode) BatchAggregationExpression.this.evaluate(node, null, context)).get(this.index);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (this.partials == null ? 0 : this.partials.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final BatchAggregationExpression other = (BatchAggregationExpression) obj;
		return this.partials.equals(other.partials);
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append("batch");
	}
}