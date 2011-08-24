package eu.stratosphere.sopremo.cleansing.fusion;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class FusionRule extends CleansingRule<FusionContext> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5841402171573265477L;

	public FusionRule(final EvaluationExpression... targetPath) {
		super(targetPath);
	}

	@Override
	public final JsonNode evaluate(final JsonNode values, final FusionContext context) {
		return this.fuse(StreamArrayNode.valueOf(values.iterator(), true).toArray(), context.getWeights(), context);
	}

	public abstract JsonNode fuse(JsonNode[] values, double[] weights, FusionContext context);
}
