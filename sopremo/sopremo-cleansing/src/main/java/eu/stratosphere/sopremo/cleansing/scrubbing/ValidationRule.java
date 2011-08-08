package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;

public abstract class ValidationRule extends CleansingRule<ValidationContext> {
	public static final UnresolvableCorrection DEFAULT_CORRECTION = UnresolvableCorrection.INSTANCE;

	/**
	 * 
	 */
	private static final long serialVersionUID = -7139939245760263511L;

	private ValueCorrection valueCorrection = DEFAULT_CORRECTION;

	public ValidationRule(final String... targetPath) {
		super(targetPath);
	}

	@Override
	public final JsonNode evaluate(final JsonNode value, final ValidationContext context) {
		if (!this.validate(value, context))
			return this.fix(value, context);
		return value;
	}

	protected JsonNode fix(final JsonNode value, final ValidationContext context) {
		return this.valueCorrection.fix(value, context);
	}

	public ValueCorrection getValueCorrection() {
		return this.valueCorrection;
	}

	public void setValueCorrection(final ValueCorrection valueCorrection) {
		if (valueCorrection == null)
			throw new NullPointerException("valueCorrection must not be null");

		this.valueCorrection = valueCorrection;
	}

	protected boolean validate(final JsonNode value, final ValidationContext context) {
		return false;
	}
}
