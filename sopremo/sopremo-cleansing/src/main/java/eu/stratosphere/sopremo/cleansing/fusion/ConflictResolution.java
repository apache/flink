package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

public abstract class ConflictResolution extends CleansingRule<FusionContext> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5841402171573265477L;

	@Override
	public final JsonNode evaluateRule(final JsonNode values, final FusionContext context) {
		return this.fuse(ArrayNode.valueOf(((ArrayNode) values).iterator()).toArray(), context.getWeights(), context);
	}

	public abstract JsonNode fuse(JsonNode[] values, double[] weights, FusionContext context);
}
