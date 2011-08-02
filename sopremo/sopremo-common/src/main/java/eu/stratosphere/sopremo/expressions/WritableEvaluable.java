package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;

public interface WritableEvaluable {
	public void set(JsonNode node, JsonNode value, EvaluationContext context);
}
