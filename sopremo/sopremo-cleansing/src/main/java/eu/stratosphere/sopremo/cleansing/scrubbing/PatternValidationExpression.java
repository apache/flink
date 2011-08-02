package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;

public class PatternValidationExpression extends ValidationRule {
	private Pattern pattern;

	public PatternValidationExpression(Pattern pattern, ObjectAccess... targetPath) {
		super(targetPath);
		this.pattern = pattern;
	}

	@Override
	protected boolean validate(JsonNode node, JsonNode contextNode, EvaluationContext context) {
		return this.pattern.matcher(TypeCoercer.INSTANCE.coerce(node, TextNode.class).getTextValue())
			.matches();
	}
}
