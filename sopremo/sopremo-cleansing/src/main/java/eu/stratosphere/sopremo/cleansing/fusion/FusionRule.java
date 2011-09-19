package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

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
		return this.fuse(((ArrayNode)ArrayNode.valueOf(((ArrayNode)values).iterator())).toArray(), context.getWeights(), context);
	}

	public abstract JsonNode fuse(JsonNode[] values, double[] weights, FusionContext context);
}
