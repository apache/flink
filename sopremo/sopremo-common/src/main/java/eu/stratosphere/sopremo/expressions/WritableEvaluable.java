package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public interface WritableEvaluable {
	public JsonNode set(JsonNode node, JsonNode value, EvaluationContext context);
	
	public EvaluationExpression asExpression();
}
