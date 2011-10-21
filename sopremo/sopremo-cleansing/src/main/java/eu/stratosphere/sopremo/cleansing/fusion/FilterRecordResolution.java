package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.type.JsonNode;

public class FilterRecordResolution extends FusionRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1764171809609427171L;

	@Override
	public JsonNode fuse(final JsonNode[] values, final double[] weights, final FusionContext context) {
		throw new UnresolvableEvaluationException();
	}
}
