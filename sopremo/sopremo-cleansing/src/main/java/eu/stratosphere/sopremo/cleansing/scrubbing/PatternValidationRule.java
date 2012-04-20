package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.regex.Pattern;

import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.TextNode;

public class PatternValidationRule extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = -414237332065402567L;

	private final Pattern pattern;

	public PatternValidationRule(final Pattern pattern) {
		this.pattern = pattern;
	}

	@Override
	protected boolean validate(final JsonNode node, final ValidationContext context) {
		return this.pattern.matcher(TypeCoercer.INSTANCE.coerce(node, TextNode.class).getTextValue())
			.matches();
	}
}
