package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.TypeCoercer;

public class TypeValidationExpression extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5475336828366519516L;

	private final Class<? extends JsonNode> type;

	public TypeValidationExpression(final Class<? extends JsonNode> type,
			final String... targetPath) {
		super(targetPath);
		this.type = type;
	}

	@Override
	protected JsonNode fix(final JsonNode value, final ValidationContext context) {
		try {
			if (value.isTextual())
				return LenientParser.INSTANCE.parse(value.getTextValue(), this.type,
					LenientParser.ELIMINATE_NOISE);
			return TypeCoercer.INSTANCE.coerce(value, this.type);
		} catch (final Exception e) {
			return super.fix(value, context);
		}
	}

	@Override
	protected boolean validate(final JsonNode value, final ValidationContext context) {
		return this.type.isInstance(value);
	}
}
