package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.cleansing.fusion.UnresolvableEvaluationException;

public class UnresolvableCorrection extends ValueCorrection {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4543623927861750109L;

	/**
	 * The default, stateless instance.
	 */
	public final static UnresolvableCorrection INSTANCE = new UnresolvableCorrection();

	@Override
	public JsonNode fix(final JsonNode value, final ValidationContext context) {
		throw new UnresolvableEvaluationException(String.format("Cannot fix %s voilating %s", value,
			context.getViolatedRule()));
	}

}
