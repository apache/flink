package eu.stratosphere.sopremo.cleansing.conflict_resolution;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class ConflictResolution extends EvaluationExpression {
	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return resolve(node.iterator(), context);
	}

	protected abstract JsonNode resolve(Iterator<JsonNode> values, EvaluationContext context);
}
