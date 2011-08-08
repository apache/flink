package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;

public abstract class ValueCorrection extends CleansingRule<ValidationContext> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5426600708331727317L;

	@Override
	public JsonNode evaluate(final JsonNode node, final ValidationContext context) {
		return this.fix(node, context);
	}

	public abstract JsonNode fix(JsonNode value, ValidationContext context);
}
