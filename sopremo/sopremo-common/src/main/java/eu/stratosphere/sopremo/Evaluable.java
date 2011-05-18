package eu.stratosphere.sopremo;

import org.codehaus.jackson.JsonNode;

public interface Evaluable {
	public abstract JsonNode evaluate(JsonNode node, EvaluationContext context);
}