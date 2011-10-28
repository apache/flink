package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

public class MethodPointerExpression extends EvaluationExpression {
	private String functionName;
	
	public MethodPointerExpression(String functionName) {
		this.functionName = functionName;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return context.getFunctionRegistry().evaluate(this.functionName, (ArrayNode) node, context);
	}
	
	@Override
	protected void toString(StringBuilder builder) {
		builder.append("&").append(this.functionName);
	}
}
