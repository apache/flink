package eu.stratosphere.sopremo.cleansing.conflict_resolution;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class ConflictResolution extends EvaluationExpression {

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return null;
	}

}
