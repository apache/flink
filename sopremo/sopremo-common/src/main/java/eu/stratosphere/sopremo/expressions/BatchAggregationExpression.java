package eu.stratosphere.sopremo.expressions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.aggregation.Aggregation;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IStreamArrayNode;
import eu.stratosphere.util.CollectionUtil;

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

	private transient List<IJsonNode> lastPreprocessingResults = new ArrayList<IJsonNode>(),
			lastAggregators = new ArrayList<IJsonNode>();

	private transient int lastInputCounter = Integer.MIN_VALUE;

	private transient IArrayNode results;

	/**
	 * Initializes a BatchAggregationExpression with the given {@link AggregationFunction}s.
	 * 
	 * @param functions
	 *        all functions that should be used
	 */
	public BatchAggregationExpression(final Aggregation<?, ?>... functions) {
		this(Arrays.asList(functions));
	}

	/**
	 * Initializes a BatchAggregationExpression with the given {@link AggregationFunction}s.
	 * 
	 * @param functions
	 *        a set of all functions that should be used
	 */
	public BatchAggregationExpression(final List<Aggregation<?, ?>> functions) {
		this.partials = new ArrayList<Partial>(functions.size());
		for (final Aggregation<?, ?> function : functions)
			this.partials.add(new Partial(function, EvaluationExpression.VALUE, this.partials.size()));
		CollectionUtil.ensureSize(this.lastPreprocessingResults, this.partials.size());
		CollectionUtil.ensureSize(this.lastAggregators, this.partials.size());
	}

	private void readObject(final ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		this.lastInputCounter = Integer.MIN_VALUE;
		this.lastPreprocessingResults = new ArrayList<IJsonNode>();
		this.lastAggregators = new ArrayList<IJsonNode>();
		CollectionUtil.ensureSize(this.lastPreprocessingResults, this.partials.size());
		CollectionUtil.ensureSize(this.lastAggregators, this.partials.size());
	}

	/**
	 * Adds a new {@link AggregationFunction}.
	 * 
	 * @param function
	 *        the function that should be added
	 * @return the function which has been added as a {@link Partial}
	 */
	public EvaluationExpression add(final Aggregation<?, ?> function) {
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
	public EvaluationExpression add(final Aggregation<?, ?> function, final EvaluationExpression preprocessing) {
		final Partial partial = new Partial(function, preprocessing, this.partials.size());
		this.partials.add(partial);
		this.lastPreprocessingResults.add(null);
		this.lastAggregators.add(null);
		return partial;
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
		if (this.lastInputCounter == context.getInputCounter())
			return this.results;
		this.results = SopremoUtil.reinitializeTarget(target, ArrayNode.class);
		this.lastInputCounter = context.getInputCounter();

		for (int index = 0; index < this.lastAggregators.size(); index++)
			this.lastAggregators.set(index,
				this.partials.get(index).getFunction().initialize(this.lastAggregators.get(index)));
		for (final IJsonNode input : (IStreamArrayNode) node)
			for (int index = 0; index < this.partials.size(); index++) {
				final AggregationExpression partial = this.partials.get(index);
				final IJsonNode preprocessedValue =
					partial.getPreprocessing().evaluate(input, this.lastPreprocessingResults.get(index), context);
				if (preprocessedValue.isMissing())
					throw new EvaluationException(String.format("Cannot access %s for aggregation %s",
						partial.getPreprocessing(), partial));
				this.lastAggregators.set(index,
					partial.getFunction().aggregate(preprocessedValue, this.lastAggregators.get(index), context));
			}

		for (int index = 0; index < this.partials.size(); index++) {
			final IJsonNode partialResult =
				this.partials.get(index).getFunction().getFinalAggregate(this.lastAggregators.get(index),
					this.results.get(index));
			this.results.set(index, partialResult);
		}

		return this.results;
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
		public Partial(final Aggregation<?, ?> function, final EvaluationExpression preprocessing, final int index) {
			super(function, preprocessing);
			this.index = index;
		}

		@Override
		public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
			return ((IArrayNode) BatchAggregationExpression.this.evaluate(node, null, context)).get(this.index);
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.expressions.AggregationExpression#toString(java.lang.StringBuilder)
		 */
		@Override
		public void toString(StringBuilder builder) {
			this.getFunction().toString(builder);
			builder.append('(');
			BatchAggregationExpression.this.toString(builder);
			if (this.getPreprocessing() != EvaluationExpression.VALUE)
				this.getPreprocessing().toString(builder);
			builder.append(')');
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
		builder.append("<batch>");
	}
}