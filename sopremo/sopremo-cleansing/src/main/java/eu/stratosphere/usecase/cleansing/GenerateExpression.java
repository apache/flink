package eu.stratosphere.usecase.cleansing;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class GenerateExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3577122499530356668L;
	private String pattern;

	private long id;

	public GenerateExpression(String patternString) {
		int patternPos = patternString.indexOf("%");
		if (patternPos == -1)
			patternString += "%s %s";
		else if (patternString.indexOf("%", patternPos + 1) == -1)
			patternString = patternString.replaceAll("%", "%s %");
		this.pattern = patternString;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return TextNode.valueOf(String.format(this.pattern, context.getTaskId(), this.id++));
	}
}
