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

	private List<Partial> partials;

	public BatchAggregationExpression(List<AggregationFunction> functions) {
		this.partials = new ArrayList<Partial>(functions.size());
		for (AggregationFunction function : functions)
			partials.add(new Partial(function, EvaluationExpression.SAME_VALUE, partials.size()));
	}

	public BatchAggregationExpression(AggregationFunction... functions) {
		this(Arrays.asList(functions));
	}

	private transient JsonNode lastResult;

	private transient int lastInputCounter = Integer.MIN_VALUE;

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		if (lastInputCounter == context.getInputCounter())
			return lastResult;

		lastInputCounter = context.getInputCounter();

		for (Partial partial : this.partials)
			partial.getFunction().initialize();
		for (JsonNode input : node)
			for (Partial partial : this.partials)
				partial.getFunction().aggregate(partial.getPreprocessing().evaluate(input, context), context);

		JsonNode[] results = new JsonNode[partials.size()];
		for (int index = 0; index < results.length; index++)
			results[index] = partials.get(index).getFunction().getFinalAggregate();

		return lastResult = JsonUtil.asArray(results);
	}

	public EvaluationExpression add(AggregationFunction function) {
		return add(function, EvaluationExpression.SAME_VALUE);
	}

	public EvaluationExpression add(AggregationFunction function, EvaluationExpression preprocessing) {
		Partial partial = new Partial(function, preprocessing, partials.size());
		partials.add(partial);
		return partial;
	}

	private class Partial extends AggregationExpression {
		private int index;

		public Partial(AggregationFunction function, EvaluationExpression preprocessing, int index) {
			super(function, preprocessing);
			this.index = index;
		}

		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			return BatchAggregationExpression.this.evaluate(node, context).get(index);
		}
	}
}