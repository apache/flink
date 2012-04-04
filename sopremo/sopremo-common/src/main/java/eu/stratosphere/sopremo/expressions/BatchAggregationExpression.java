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

	public BatchAggregationExpression(final AggregationFunction... functions) {
		this(Arrays.asList(functions));
	}

	public BatchAggregationExpression(final List<AggregationFunction> functions) {
		this.partials = new ArrayList<Partial>(functions.size());
		for (final AggregationFunction function : functions)
			this.partials.add(new Partial(function, EvaluationExpression.VALUE, this.partials.size()));
		this.expectedTarget = ArrayNode.class;
	}

	public EvaluationExpression add(final AggregationFunction function) {
		return this.add(function, EvaluationExpression.VALUE);
	}

	public EvaluationExpression add(final AggregationFunction function, final EvaluationExpression preprocessing) {
		final Partial partial = new Partial(function, preprocessing, this.partials.size());
		this.partials.add(partial);
		return partial;
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		if (this.lastInputCounter == context.getInputCounter())
			return this.lastResult;

		this.lastInputCounter = context.getInputCounter();

		for (final Partial partial : this.partials)
			partial.getFunction().initialize();
		for (final IJsonNode input : (ArrayNode) node)
			for (final Partial partial : this.partials)
				partial.getFunction().aggregate(partial.getPreprocessing().evaluate(input, null, context), context);

		final IJsonNode[] results = new IJsonNode[this.partials.size()];
		for (int index = 0; index < results.length; index++)
			results[index] = this.partials.get(index).getFunction().getFinalAggregate();

		try {
			target = SopremoUtil.reuseTarget(target, this.expectedTarget);
		} catch (InstantiationException e) {
			return this.lastResult = new ArrayNode(results);
		} catch (IllegalAccessException e) {
			return this.lastResult = new ArrayNode(results);
		}

		return this.lastResult = ((IArrayNode) target).addAll(results);
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