package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class PatternValidationExpression extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = -414237332065402567L;

	private final Pattern pattern;

	public PatternValidationExpression(final Pattern pattern, final EvaluationExpression... targetPath) {
		super(targetPath);
		this.pattern = pattern;
	}

	@Override
	protected boolean validate(final JsonNode node, final ValidationContext context) {
		return this.pattern.matcher(TypeCoercer.INSTANCE.coerce(node, TextNode.class).getTextValue())
			.matches();
	}
}
