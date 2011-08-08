package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;

/**
 * Batch aggregates one stream of {@link JsonNode} with several {@link AggregationFunction}s.
 * 
 * @author Arvid Heise
 */
public class BatchAggregationExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 39927024374509247L;

	private final List<Partial> partials;

	private transient JsonNode lastResult;

	private transient int lastInputCounter = Integer.MIN_VALUE;

	public BatchAggregationExpression(final AggregationFunction... functions) {
		this(Arrays.asList(functions));
	}

	public BatchAggregationExpression(final List<AggregationFunction> functions) {
		this.partials = new ArrayList<Partial>(functions.size());
		for (final AggregationFunction function : functions)
			this.partials.add(new Partial(function, EvaluationExpression.SAME_VALUE, this.partials.size()));
	}

	public EvaluationExpression add(final AggregationFunction function) {
		return this.add(function, EvaluationExpression.SAME_VALUE);
	}

	public EvaluationExpression add(final AggregationFunction function, final EvaluationExpression preprocessing) {
		final Partial partial = new Partial(function, preprocessing, this.partials.size());
		this.partials.add(partial);
		return partial;
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		if (this.lastInputCounter == context.getInputCounter())
			return this.lastResult;

		this.lastInputCounter = context.getInputCounter();

		for (final Partial partial : this.partials)
			partial.getFunction().initialize();
		for (final JsonNode input : node)
			for (final Partial partial : this.partials)
				partial.getFunction().aggregate(partial.getPreprocessing().evaluate(input, context), context);

		final JsonNode[] results = new JsonNode[this.partials.size()];
		for (int index = 0; index < results.length; index++)
			results[index] = this.partials.get(index).getFunction().getFinalAggregate();

		return this.lastResult = JsonUtil.asArray(results);
	}

	private class Partial extends AggregationExpression {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7597385785040647923L;

		private final int index;

		public Partial(final AggregationFunction function, final EvaluationExpression preprocessing, final int index) {
			super(function, preprocessing);
			this.index = index;
		}

		@Override
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			return BatchAggregationExpression.this.evaluate(node, context).get(this.index);
		}
	}
}