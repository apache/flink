package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SerializableSopremoType;

public interface ValueCorrection extends SerializableSopremoType {
	public JsonNode fix(JsonNode contextNode, JsonNode value, ValidationRule voilatedExpression,
			EvaluationContext context);
}
