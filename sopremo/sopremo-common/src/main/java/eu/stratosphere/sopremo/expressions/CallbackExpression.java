package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.JsonNode;

public class CallbackExpression extends EvaluationExpression {
	private String functionName;
	
	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return context.getFunctionRegistry().evaluate(functionName, node, null, context);
	}
	
	@Override
	protected void toString(StringBuilder builder) {
		builder.append("&").append(functionName);
	}
}
