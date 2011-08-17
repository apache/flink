package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;

public interface WritableEvaluable {
	public JsonNode set(JsonNode node, JsonNode value, EvaluationContext context);
	
	public EvaluationExpression asExpression();
}
